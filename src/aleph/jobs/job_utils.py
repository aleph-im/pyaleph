import asyncio
import logging
from itertools import groupby
from typing import Callable
from typing import Dict
from typing import Iterable, Tuple
from typing import cast

import pymongo.errors
from configmanager import Config

import aleph.config
from aleph.model import init_db_globals
from aleph.model.db_bulk_operation import DbBulkOperation
from aleph.services.ipfs.common import init_ipfs_globals
from aleph.services.p2p import init_p2p_client
from aleph.toolkit.exceptions import ignore_exceptions
from aleph.toolkit.split import split_iterable
from aleph.toolkit.timer import Timer

LOGGER = logging.getLogger(__name__)


def prepare_loop(config_values: Dict) -> Tuple[asyncio.AbstractEventLoop, Config]:
    """
    Prepares all the global variables (sigh) needed to run an Aleph subprocess.

    :param config_values: Dictionary of config values, as provided by the main process.
    :returns: A preconfigured event loop, and the application config for convenience.
              Use the event loop as event loop of the process as it is used by Motor. Using another
              event loop will cause DB calls to fail.
    """

    loop = asyncio.get_event_loop()

    config = aleph.config.app_config
    config.load_values(config_values)

    init_db_globals(config)
    init_ipfs_globals(config)
    # _ = await init_p2p_client(config, service_name=service_name)
    return loop, config


async def perform_db_operations(db_operations: Iterable[DbBulkOperation]) -> None:
    # Sort the operations by collection name before grouping and executing them.
    sorted_operations = sorted(
        db_operations,
        key=lambda op: op.collection.__name__,
    )

    # Process updates collection by collection. Note that we use an ugly hack for capped
    # collections where we ignore bulk write errors. These errors are usually caused
    # by the node processing the same message twice in parallel, from different sources.
    # This results in updates to a message with different confirmation fields, and capped
    # collections do not like updates that increase the size of a document.
    # We can safely ignore these errors as long as our only capped collection is
    # CappedMessage. Using unordered bulk writes guarantees that all operations will be
    # attempted, meaning that all the messages that can be inserted will be.
    for collection, operations in groupby(sorted_operations, lambda op: op.collection):
        exceptions_to_ignore = []
        if collection.is_capped():
            exceptions_to_ignore.append(pymongo.errors.BulkWriteError)

        with Timer() as timer:
            mongo_ops = [op.operation for op in operations]

            with ignore_exceptions(
                *exceptions_to_ignore,
                on_error=lambda e: LOGGER.warning(
                    "Bulk write error while writing to %s", collection.__name__
                )
            ):
                await collection.collection.bulk_write(mongo_ops, ordered=False)

        LOGGER.info(
            "Performed %d operations on %s in %.4f seconds",
            len(mongo_ops),
            collection.__name__,
            timer.elapsed(),
        )


async def process_job_results(
    tasks: Iterable[asyncio.Task],  # TODO: switch to a generic type when moving to 3.9+
    on_error: Callable[[BaseException], None],
):
    """
    Processes the result of the pending TX/message tasks.

    Splits successful and failed jobs, handles exceptions and performs
    DB operations.

    :param tasks: Finished job tasks. Each of these tasks must return a list of
                  DbBulkOperation objects. It is up to the caller to determine
                  when tasks are done, for example by using asyncio.wait.
    :param on_error: Error callback function. This function will be called
                     on each error from one of the tasks.
    """
    successes, errors = split_iterable(tasks, lambda t: t.exception() is None)

    for error in errors:
        # mypy sees Optional[BaseException] otherwise
        exception = cast(BaseException, error.exception())
        on_error(exception)

    db_operations = (op for success in successes for op in success.result())

    await perform_db_operations(db_operations)
