from abstract_fileformat_handler import AbstractFileFormatHandler
import csv

import io

class CsvHandler(AbstractFileFormatHandler):
    def __init__(self, **kwargs):
        self.data = []
        self.kwargs = kwargs
        self.header = []

    def load(self, from_stream):
        csv_reader = csv.reader(from_stream, **self.kwargs)
        # set self.header if header setting
        for row in csv_reader:
            self.data.append(row)
        return self.data

    def loads(self, from_string):
        stream = io.StringIO(from_string)
        return self.load(stream)

    def get_tree_path(self, content):
        for row in self.data:
            if content in row:
                return row
        return None

    def set_tree_path(self, content, new_data):
        row = self.get_tree_path(content)
        if row is not None:
            index = self.data.index(row)
            self.data[index] = new_data
        else:
            self.data.append(new_data)

    def dump(self, to_stream):
        csv_writer = csv.writer(to_stream, **self.kwargs)
        for row in self.data:
            csv_writer.writerow(row)

    def dumps(self):
        stream = io.StringIO()
        csv_writer = csv.writer(stream)
        for row in self.data:
            csv_writer.writerow(row)
        return stream.getvalue()

