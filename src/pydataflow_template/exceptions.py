class MyException(Exception):
    pretext = ""

    def __init__(self, msg, *args):
        if self.pretext:
            msg = f"{self.pretext}: {msg}"
        super().__init__(msg, *args)


class ParameterValidationError(MyException):
    pretext = "Invalid parameter"


class ParameterCombinationError(MyException):
    pretext = "Invalid parameter combination"


class ParameterRuntimeError(MyException):
    pretext = "Invalid parameter at runtime"


class InvalidPathError(MyException):
    pretext = "Invalid path"


class IoError(MyException):
    pretext = "Io error"


class ConfigNotRegisteredError(MyException):
    pretext = "Config not registered error"


class ModuleNotRegisteredError(MyException):
    pretext = "Module not registered error"
