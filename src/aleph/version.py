import logging
from subprocess import STDOUT, CalledProcessError, check_output

logger = logging.getLogger(__name__)


def get_version_from_git() -> str | None:
    try:
        return (
            check_output(("git", "describe", "--tags"), stderr=STDOUT).strip().decode()
        )
    except FileNotFoundError:
        logger.warning("version: git not found")
        return None
    except CalledProcessError as err:
        logger.info(
            "version: git description not available: %s", err.output.decode().strip()
        )
        return None


def get_version_from_resources() -> str | None:
    from pkg_resources import DistributionNotFound, get_distribution

    try:
        # Change here if project is renamed and does not equal the package name
        dist_name = __name__
        return get_distribution(dist_name).version
    except DistributionNotFound:
        return get_version_from_git()
    finally:
        del get_distribution, DistributionNotFound


def get_version() -> str | None:
    return get_version_from_resources() or get_version_from_git()


# The version number is hardcoded in the following line when packaging the software
# Change here if project is renamed and does not equal the package name
__version__ = get_version() or "version-unavailable"
