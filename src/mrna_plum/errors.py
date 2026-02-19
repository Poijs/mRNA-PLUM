class mRnaPlumError(Exception):
    """Base app error."""


class ConfigError(mRnaPlumError):
    pass


class InputDataError(RnaPlumError):
    pass


class MixedPeriodsError(mRnaPlumError):
    pass


class ProcessingError(mRnaPlumError):
    pass
