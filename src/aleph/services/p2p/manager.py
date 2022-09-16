import logging
from typing import Coroutine, List

from aleph_p2p_client import AlephP2PServiceClient
from configmanager import Config

from aleph.services.ipfs.common import get_public_address
from aleph.services.peers.monitor import monitor_hosts_ipfs, monitor_hosts_p2p
from aleph.services.peers.publish import publish_host
from aleph.services.utils import get_IP

LOGGER = logging.getLogger("P2P.host")


# Save published adress to present them in the web process later
public_adresses = []


async def initialize_host(
    config: Config,
    p2p_client: AlephP2PServiceClient,
    api_servers: List[str],
    host: str = "0.0.0.0",
    port: int = 4025,
    listen: bool = True,
) -> List[Coroutine]:

    from .jobs import reconnect_p2p_job, tidy_http_peers_job

    tasks: List[Coroutine]

    transport_opt = f"/ip4/{host}/tcp/{port}"

    tasks = [
        reconnect_p2p_job(config=config, p2p_client=p2p_client),
        tidy_http_peers_job(config=config, api_servers=api_servers),
    ]
    if listen:
        peer_id = (await p2p_client.identify()).peer_id
        LOGGER.info("Listening on " + f"{transport_opt}/p2p/{peer_id}")
        ip = await get_IP()
        public_address = f"/ip4/{ip}/tcp/{port}/p2p/{peer_id}"
        http_port = config.p2p.http_port.value
        public_adresses.append(public_address)

        public_http_address = f"http://{ip}:{http_port}"

        LOGGER.info("Probable public on " + public_address)
        # TODO: set correct interests and args here
        tasks += [
            publish_host(
                public_address,
                p2p_client,
                p2p_alive_topic=config.p2p.alive_topic.value,
                ipfs_alive_topic=config.ipfs.alive_topic.value,
                peer_type="P2P",
                use_ipfs=config.ipfs.enabled.value,
            ),
            publish_host(
                public_http_address,
                p2p_client,
                p2p_alive_topic=config.p2p.alive_topic.value,
                ipfs_alive_topic=config.ipfs.alive_topic.value,
                peer_type="HTTP",
                use_ipfs=config.ipfs.enabled.value,
            ),
            monitor_hosts_p2p(p2p_client, alive_topic=config.p2p.alive_topic.value),
        ]

        if config.ipfs.enabled.value:
            tasks.append(
                monitor_hosts_ipfs(alive_topic=config.ipfs.alive_topic.value)
            )
            try:
                public_ipfs_address = await get_public_address()
                tasks.append(
                    publish_host(
                        public_ipfs_address,
                        p2p_client,
                        p2p_alive_topic=config.p2p.alive_topic.value,
                        ipfs_alive_topic=config.ipfs.alive_topic.value,
                        peer_type="IPFS",
                        use_ipfs=True,
                    )
                )
            except Exception:
                LOGGER.exception("Can't publish public IPFS address")

    return tasks
