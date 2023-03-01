from configmanager import Config
import sentry_sdk
from sentry_sdk.integrations.aiohttp import AioHttpIntegration


def setup_sentry(config: Config):
    if dsn := config.sentry.dsn.value:
        if (config_sample_rate := config.sentry.traces_sample_rate.value) is not None:
            traces_sample_rate = float(config_sample_rate)
        else:
            traces_sample_rate = None

        sentry_sdk.init(
            dsn=dsn,
            traces_sample_rate=traces_sample_rate,
            ignore_errors=[KeyboardInterrupt],
            integrations=[AioHttpIntegration()],
        )
