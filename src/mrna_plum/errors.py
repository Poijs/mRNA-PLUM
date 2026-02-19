class mRnaPlumError(Exception):
    """Base app error."""


class ConfigError(mRnaPlumError):
    pass


class InputDataError(mRnaPlumError):
    pass


class MixedPeriodsError(mRnaPlumError):
    pass


class ProcessingError(mRnaPlumError):
    pass
