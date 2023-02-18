import logging

from .fileformat_handler import FileformatHandler
import io
import json
import pandas as pd
import numpy as np
from typing import Union, Dict, Literal


logger = logging.getLogger(__name__)


class JsonHandler(FileformatHandler):
    """JSON file handler

    학습데이터로 JSON 파일은 전체가 'array' 이거나, object 의 특정 키 값이 'array' 라고 추정
    또는 한줄이 JSON 형태인 'multiline' 파일일 수 있음
    json_type 'object' 는 학습데이터가 아닌 전체 파일을 읽어들일 떄에만 사용
    """
    TYPE_ARRAY = 'array'
    TYPE_MULTILINE = 'multiline'
    TYPE_OBJECT = 'object'

    # 지원하는 추가 키워드 : JSON
    support_kw = {
        'load': {
            'json_type': TYPE_ARRAY,
            'data_key': '',
            'encoding': 'utf-8',
            'error_log': 'error.log'
        },
        'dump:': {
            'json_type': TYPE_ARRAY,
            'data_key': '',
            'encoding': 'utf-8',
            'error_log': 'error.log'
        }
    }

    def __init__(self, json_type: Union['array', 'multiline', 'object'], data_key: str = '', encoding='utf-8'):
        """Initialize json file format

        Args:
            json_type (Union['object', 'array', 'multiline']): 'object' for one json object, 'array' for json array, 'multiline' for objects each lines
            data_key (str): if empty use whole file, else use only key value. for example 'data'
        """
        super().__init__(encoding)
        self.json_type = json_type
        self.data_key = data_key

    def append_json_data(self, json_obj) -> None:
        """'multiline' json_type 내부메쏘드 data_obj 에 json_obj 추가

        Args:
            json_obj: 추가할 json object
        """
        if not self.data_key:
            self.origin_obj.append(json_obj)
        elif self.data_key in json_obj:
            self.origin_obj.append(json_obj[self.data_key])
        else:
            self.failed_obj.append(json_obj)

    # 내부 함수 for object and array json_type
    def update_json_data(self, json_obj) -> None:
        """내부메쏘드 json_obj 처리 결과 반영

        Args:
            json_obj: 설정할 json object

        """
        if self.json_type == JsonHandler.TYPE_ARRAY:
            # 전체가 배열일 경우
            if isinstance(json_obj, list):
                if not self.data_key:
                    self.pass_list = json_obj
                else:
                    self.pass_list = [e for e in json_obj if self.data_key in e]
                    self.fail_list = [e for e in json_obj if self.data_key not in e]
            # 전체는 객체이지만 data_key 가 배열일 경우
            elif self.data_key in json_obj and isinstance(json_obj[self.data_key], list):
                self.pass_list = json_obj[self.data_key]
            else:
                self.fail_list.append(json_obj)
        elif self.json_type == JsonHandler.TYPE_MULTILINE:
            if not self.data_key:
                self.pass_list.append(json_obj)
            elif self.data_key in json_obj:
                self.pass_list.append(json_obj[self.data_key])
            else:
                self.fail_list.append(json_obj)
        elif self.json_type == JsonHandler.TYPE_OBJECT:
            if not self.data_key:
                self.pass_list.append(json_obj)
            elif self.data_key in json_obj:
                self.pass_list.append(json_obj[self.data_key])
            else:
                self.fail_list.append(json_obj)

    def load(self, file_or_filename: Union[io.TextIOWrapper, io.BytesIO, str]) -> None:
        """파일 객체나 파일명에서 JSON 데이터 읽기
        Args:
            file_or_filename (file-like object): file or s3 stream object which support .read() function

        Returns:
            json object (dict) if json_type is 'object'
            array of dict if json_type is 'array' or 'multiline'
        """
        # file_or_filename 클래스 유형에 따라서 처리 방법이 다름
        if isinstance(file_or_filename, str):
            try:
                fp = open(file_or_filename, 'r', encoding=self.encoding)
                mode = 'text'
            except Exception as e:
                logger.error(f"{file_or_filename} is not exist filename or can not open by encoding={self.encoding}")
                logger.exception(e)
                return
        elif isinstance(file_or_filename, io.TextIOWrapper):
            fp = file_or_filename
            mode = 'text'
        elif isinstance(file_or_filename, io.BytesIO):
            fp = file_or_filename
            mode = 'binary'
        else:
            logger.error(f"{file_or_filename} is not file obj ")

        if 'text' == mode:
            if self.json_type == JsonHandler.TYPE_OBJECT or self.json_type == JsonHandler.TYPE_ARRAY:
                try:
                    root_json = json.load(fp)
                    self.update_json_data(root_json)
                except Exception as e:
                    file_bytes = fp.read()
                    self.fail_list.append(file_bytes)
                    logger.exception(e)
            elif self.json_type == JsonHandler.TYPE_MULTILINE:
                for line_str in fp:
                    try:
                        json_obj = json.loads(line_str)
                        self.update_json_data(json_obj)
                    except Exception as e:
                        self.fail_list.append(line_str)
        if 'binary' == mode:
            if self.json_type == JsonHandler.TYPE_OBJECT or self.json_type == JsonHandler.TYPE_ARRAY:
                try:
                    root_json = json.load(fp)
                    self.update_json_data(root_json)
                except Exception as e:
                    file_bytes = fp.read()
                    self.fail_list.append(file_bytes)
                    logger.exception(e)
            if self.json_type == JsonHandler.TYPE_MULTILINE:
                try:
                    lines = file_or_filename.splitlines()
                except Exception as e:
                    logger.exception(e)
                    file_bytes = file_or_filename.read()
                    self.fail_list.append(file_bytes)
                    return

                for line_bytes in lines:
                    try:
                        line_str = line_bytes.decode(self.encoding)
                        json_obj = json.loads(line_str)
                        self.update_json_data(json_obj)
                    except Exception as e:
                        self.fail_list.append(line_bytes)
                        logger.exception(e)
        # close opened file if filename
        if isinstance(file_or_filename, str) and fp:
            fp.close()

    def loads(self, str_or_bytes: Union[str, bytes]):
        """문자열이나 bytes 에서 JSON 객체 읽기

        데이터 처리 결과는 객체 내부에 성공 목록과 실패 목록으로 저장됨

        Args:
            str_or_bytes (str, bytes): text모드 string 또는 binary모드 bytes

        """
        if isinstance(str_or_bytes, str):
            text_file = io.StringIO(str_or_bytes)
            self.load(text_file)
        elif isinstance(str_or_bytes, bytes):
            binary_file = io.BytesIO(str_or_bytes)
            self.load(binary_file)

    def get_tree_path(self, tree_path):
        path_keys = [p for p in tree_path.split('/') if p]
        if self.data:
            json_obj = self.data
            if JsonHandler.TYPE_OBJECT == self.json_type:
                for key in path_keys:
                    if key in json_obj:
                        json_obj = json_obj[key]
                    else:
                        return None
                return json_obj
            elif JsonHandler.TYPE_ARRAY == self.json_type or JsonHandler.TYPE_MULTILINE == self.json_type:
                results = []
                json_list = self.data
                for a in json_list:
                    json_obj = a
                    for key in path_keys:
                        if key in json_obj:
                            json_obj = json_obj[key]
                        else:
                            json_obj = None
                            break
                    if json_obj:
                        results.append(json_obj)
                return results
        else:
            logger.info(f"data is not exist")

        if JsonHandler.TYPE_OBJECT == self.json_type:
            return None
        else:
            return []

    def to_pandas(self) -> pd.DataFrame:
        """JSON 파일 처리 결과를 pd.DataFrame 형태로 받음

        내부적으로 추가할 데이터(pass_list)가 있으면 추가하여 새로운 pd.DataFrame 생성
        실패 목록(fail_list)가 있으면 파일로 저장
        학습을 위한 dataframe 이기 떄문에 dot('.') 문자로 normalize 된 flatten 컬럼과 값을 가진다.

        Returns: pandas 데이터프레임

        """
        if self.pass_list:
            try:
                append_df = pd.json_normalize(self.pass_list)
                merge_df = pd.concat(self.data, append_df, ignore_index=True)
                self.data = merge_df
            except Exception as e:
                logger.exception(e)
                self.fail_list.extend(self.pass_list)
            finally:
                self.pass_list.clear()
        if self.fail_list:
            try:
                fp = open(self.error_log, mode='ab')
            except Exception as e:
                logger.exception(e)

            if fp:
                for fail in self.fail_list:
                    try:
                        fp.write(fail + '\n')
                    except Exception as e:
                        logger.exception(e)
                self.fail_list.clear()
        return self.data

    def dump(self, file_or_filename):
        if self.data:
            json.dump(self.data, file_or_filename)

    def dumps(self):
        if self.data:
            return json.dumps(self.data)

    # 'array' 나 'multiline' 유형으로 모두 같은 key:value 형태로 되어 있다고 가정
    def to_pandas(self):
        self.data_df = pd.DataFrame.from_dict(self.data)
        return self.df

    def to_csv(self, filename):
        if self.data:
            if self.data_dirty or not self.data_df:
                self.to_pandas()
                self.data_dirty = False

            if self.data_df:
                self.data_df.to_csv(filename, encoding=self.encoding, index=False)







