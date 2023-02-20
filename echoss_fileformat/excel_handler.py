from .fileformat_base import FileformatBase
import io
import pandas as pd
import openpyxl


class ExcelHandler(FileformatBase):
    # 지원하는 추가 키워드
    KW_DICT = {
        'load': {

        },
        'dump': {

        }
    }

    def __get_kw_dict(self) -> dict:
        return ExcelHandler.KW_DICT

    def __init__(self):
        self.data_df = pd.DataFrame()

    def load(self, file_or_filename, sheet_name=0, header=0, skiprows=0, nrows=None, usecols=None):
        self.data_df = pd.read_excel(
            file_or_filename,
            sheet_name=sheet_name,
            header=header,
            skiprows=skiprows,
            nrows=nrows,
            usecols=usecols
        )
        return self.data_df

    def loads(self, str_or_bytes, sheet_name=0, header=0, skiprows=0, nrows=None, usecols=None):
        file = io.BytesIO(str_or_bytes)
        return self.load(file, sheet_name, header, skiprows, nrows, usecols)

    def get_tree_path(self, tree_path):
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

    def set_tree_path(self, tree_path, new_data):
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

    def dump(self, stream):
        self.data_df.to_excel(stream, index=False)

    def dumps(self):
        file = io.BytesIO()
        self.data_df.to_excel(file, index=False)
        return file.getvalue()


# workbook version ?
class ExcelHandlerPyxl(FileformatBase):
    # 지원하는 추가 키워드
    KW_DICT = {
        'load': {

        },
        'dump': {

        }
    }

    def __get_kw_dict(self):
        return ExcelHandlerPyxl.KW_DICT

    def __init__(self):
        self.workbook = None

    def load(self, stream):
        self.workbook = openpyxl.load_workbook(stream)

    def loads(self, string):
        # Not supported in openpyxl
        raise NotImplementedError("ExcelFileFormatHandler does not support loads")

    def get_tree_path(self, tree_path):
        path_keys = [p for p in tree_path.split('/') if p]
        if len(path_keys) == 3:
            sheet_name, row_index, column_index = tree_path
            row_index = int(row_index)
            column_index = int(column_index)
            if sheet_name in self.workbook:
                sheet = self.workbook[sheet_name]
                return sheet.cell(row=row_index, column=column_index).value

    def set_tree_path(self, tree_path, value):
        path_keys = [p for p in tree_path.split('/') if p]
        sheet_name, row_index, column_index = tree_path
        row_index = int(row_index)
        column_index = int(column_index)
        if sheet_name in self.workbook:
            sheet = self.workbook[sheet_name]
            sheet.cell(row=row_index, column=column_index).value = value

    def dump(self, stream):
        self.workbook.save(stream)

    def dumps(self):
        # Not supported in openpyxl
        raise NotImplementedError("ExcelFileFormatHandler does not support dumps")
