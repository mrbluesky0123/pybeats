class PyBeatsException(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class PyBeatsFileWorkException(PyBeatsException):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class PyBeatsSocketWorkException(PyBeatsException):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class PyBeatsDBWorkException(PyBeatsException):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)