import abc

class AbstractFileFormatHandler(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def load(self, from_stream):
        pass

    @abc.abstractmethod
    def loads(self, from_string):
        pass

    @abc.abstractmethod
    def get(self, xpath):
        pass

    @abc.abstractmethod
    def update(self, xpath, new_data):
        pass

    @abc.abstractmethod
    def dump(self, to_stream):
        pass

    @abc.abstractmethod
    def dumps(self):
        pass

