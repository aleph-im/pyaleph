import aio_pika


async def make_mq_conn(config) -> aio_pika.abc.AbstractConnection:
    mq_conn = await aio_pika.connect_robust(
        host=config.p2p.mq_host.value,
        port=config.rabbitmq.port.value,
        login=config.rabbitmq.username.value,
        password=config.rabbitmq.password.value,
    )
    return mq_conn
