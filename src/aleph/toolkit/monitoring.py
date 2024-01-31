import sentry_sdk
from configmanager import Config
from sentry_sdk.integrations.aiohttp import AioHttpIntegration
from sentry_sdk.integrations.asyncio import AsyncioIntegration


def setup_sentry(config: Config, traces_sample_rate=None):
    if dsn := config.sentry.dsn.value:
        if traces_sample_rate:
            if (
                config_sample_rate := config.sentry.traces_sample_rate.value
            ) is not None:
                traces_sample_rate = float(config_sample_rate)
            else:
                traces_sample_rate = None

        sentry_sdk.init(
            dsn=dsn,
            traces_sample_rate=traces_sample_rate,
            ignore_errors=[KeyboardInterrupt],
            integrations=[
                AioHttpIntegration(),
                AsyncioIntegration(),
            ],
        )
