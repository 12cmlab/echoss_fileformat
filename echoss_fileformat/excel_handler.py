from abstract_fileformat_handler import AbstractFileFormatHandler

import io
import pandas as pd
import openpyxl

class ExcelHandler(AbstractFileFormatHandler):
        def __init__(self):
            self.data = pd.DataFrame()

        def load(self, stream):
            self.data = pd.read_excel(stream)

        def loads(self, string):
            file = io.BytesIO(string)
            self.data = pd.read_excel(file)

        def get_tree_path(self, content):
            for row in self.data.iterrows():
                if content in row:
                    return row
            return None

        def set_tree_path(self, content, new_data):
            index = None
            for i, row in self.data.iterrows():
                if content in row:
                    index = i
                    break
            if index is not None:
                self.data.loc[index] = new_data
            else:
                self.data = self.data.append(new_data, ignore_index=True)

        def dump(self, stream):
            self.data.to_excel(stream, index=False)

        def dumps(self):
            file = io.BytesIO()
            self.data.to_excel(file, index=False)
            return file.getvalue()


class ExcelFileFormatHandler2(AbstractFileFormatHandler):
    def __init__(self):
        self.workbook = None

    def load(self, stream):
        self.workbook = openpyxl.load_workbook(stream)

    def loads(self, string):
        # Not supported in openpyxl
        raise NotImplementedError("ExcelFileFormatHandler does not support loads")

    def get_tree_path(self, path):
        sheet_name, row_index, column_index = path
        sheet = self.workbook[sheet_name]
        return sheet.cell(row=row_index, column=column_index).value

    def set_tree_path(self, path, value):
        sheet_name, row_index, column_index = path
        sheet = self.workbook[sheet_name]
        sheet.cell(row=row_index, column=column_index).value = value

    def dump(self, stream):
        self.workbook.save(stream)

    def dumps(self):
        # Not supported in openpyxl
        raise NotImplementedError("ExcelFileFormatHandler does not support dumps")