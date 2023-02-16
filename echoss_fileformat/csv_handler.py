from .fileformat_handler import FileformatHandler
# pd 로 대체
import pandas as pd
# import csv
from typing import Union, Dict, Literal
import io


class CsvHandler(FileformatHandler):
    # 지원하는 추가 키워드
    KW_DICT = {
        'load': {
            'usecols': None
        },
        'dump': {

        }
    }

    def __init__(self, encoding='utf-8', delimiter=',', quotechar='"', escapechar='\\'):
        self.data_df = None
        self.encoding = encoding
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.escapechar = escapechar

    def get_kw_dict(self) -> dict:
        return CsvHandler.KW_DICT

    def make_kw_dict(self, kw_name: str, kw_dict: dict) -> dict:
        """내부 메쏘드로 지원 키워드 사전에서 해당 키워드 사전 생성.

        디폴트 사전을 복사 후 kw_dict 가 있으면 신규 값으로 변경

        Args:
            kw_name (str): 지원 키워드 사전의 유형별 이름
            kw_dict (dict): 신규 값이 들어간 키워드 사전

        Returns:
            (dict) 결과 키위드 사전
        """
        if kw_dict is None:
            kw_dict = {}
        handler_kw_dict = self.get_kw_dict()
        if kw_name in handler_kw_dict:
            copy_dict = handler_kw_dict[kw_name].copy()
            for k in kw_dict:
                if k in copy_dict:
                    copy_dict[k] = kw_dict[k]
            return copy_dict
        else:
            return {}

    # def load(self, file_or_filename: Union[io.TextIOWrapper, io.BytesIO, str]) -> pd.DataFrame:
    def load(self, file_or_filename, header=0, skiprows=0, nrows=None, usecols=None, kw_dict=None):
        kwargs = CsvHandler.__make_kw_dict('load', kw_dict)
        self.data_df = pd.read_csv(
            file_or_filename,
            encoding=self.encoding,
            delimiter=self.delimiter,
            quotechar=self.quotechar,
            escapechar=self.escapechar,
            header=header,
            skiprows=skiprows,
            nrows=nrows,
            usecols=usecols,
            **kwargs
        )
        return self.data_df

    def loads(self, str_or_bytes, header=0, skiprows=0, nrows=None, usecols=None, kw_dict=None):
        stream = io.StringIO(str_or_bytes)
        return self.load(stream, header, skiprows, nrows, usecols, kw_dict)

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

    def dump(self, file_or_filename, quoting=0, kw_dict=None):
        kwargs = CsvHandler.__make_kw_dict('dump', kw_dict)
        if self.data_df:
            self.data_df.to_csv(
                file_or_filename,
                encoding=self.encoding,
                delimiter=self.delimiter,
                quotechar=self.quotechar,
                escapechar=self.escapechar,
                quoting=quoting,
                **kwargs
            )

    def dumps(self, quoting=0, kw_dict=None):
        kwargs = CsvHandler.__make_kw_dict('dump', kw_dict)
        if self.data_df:
            csv_str = self.data_df.to_csv(
                encoding=self.encoding,
                delimiter=self.delimiter,
                quotechar=self.quotechar,
                escapechar=self.escapechar,
                quoting=quoting,
                **kwargs
            )
            return csv_str
        else:
            return None
