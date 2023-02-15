from abstract_fileformat_handler import AbstractFileFormatHandler
# pd 로 대체
import pandas as pd
# import csv
import io


class CsvHandler(AbstractFileFormatHandler):
    def __init__(self, encoding='utf-8', delimiter=',', quotechar='"', escapechar='\\'):
        self.data_df = None
        self.encoding = encoding
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.escapechar = escapechar

    def load(self, from_stream, header=0, skiprows=0):
        self.data_df = pd.read_csv(
            from_stream,
            encoding=self.encoding,
            delimiter=self.delimiter,
            quotechar=self.quotechar,
            escapechar=self.escapechar,
            header=header,
            skiprows=skiprows
        )
        return self.data_df

    def loads(self, from_string, header=0, skiprows=0):
        stream = io.StringIO(from_string)
        return self.load(stream, header=header, skiprows=skiprows)

    def get_tree_path(self, tree_path: str):
        if self.data_df:
            path_keys = [p for p in tree_path.split('/') if p]
            if len(path_keys) == 0:
                return self.data_df
            elif len(path_keys) == 1:
                colname = path_keys[0]
                if colname in self.data_df.colnums:
                    return self.data_df[colname]
            else:
                colname = path_keys[0]
                row_index = int(path_keys[1])
                if colname in self.data_df.colnums:
                    return self.data_df.loc[row_index, colname]
        return None

    def set_tree_path(self, tree_path: str, new_data):
        if self.data_df:
            path_keys = [p for p in tree_path.split('/') if p]
            if len(path_keys) == 0 and isinstance(new_data, pd.DataFrame):
                self.data_df = new_data
            elif len(path_keys) == 1 and isinstance(new_data, pd.Series):
                colname = path_keys[0]
                if colname in self.data_df.colnums:
                    self.data_df[colname] = new_data
            else:
                colname = str(path_keys[0])
                row_index = int(path_keys[1])
                if colname in self.data_df.colnums:
                    self.data_df.loc[row_index, colname] = new_data

    def dump(self, to_stream, quoting=0):
        if self.data_df:
            self.data_df.to_csv(
                to_stream,
                encoding=self.encoding,
                delimiter=self.delimiter,
                quotechar=self.quotechar,
                escapechar=self.escapechar,
                quoting=quoting
            )

    def dumps(self, quoting=0):
        if self.data_df:
            csv_str = self.data_df.to_csv(
                encoding=self.encoding,
                delimiter=self.delimiter,
                quotechar=self.quotechar,
                escapechar=self.escapechar,
                quoting=quoting
            )
            return csv_str
        else:
            return None
