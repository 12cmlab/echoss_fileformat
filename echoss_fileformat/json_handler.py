import logging

import io
import json
import pandas as pd
# import numpy as np
from typing import Union, Dict, Literal

from .fileformat_base import FileformatBase

logger = logging.getLogger(__name__)


class JsonHandler(FileformatBase):
    """JSON file handler

    학습데이터로는 json_type 'array' 와 'multiline' 만 사용 권고

    전체 JSON 파일을 한번에 읽어서 처리하는 'array' 와 'object' 는 전체를 list 또는 object 로 처리함.
    JSON 파일 각 줄을 하나의 JSON object 형태로 읽어들이는 경우 'multiline' 로 선언

    특정 키만 학습데이터로 사용할 경우에는 data_key 로 키를 지정하여 처리되는 값을 지정
    (예: 'data' 또는 'message')
    """
    TYPE_ARRAY = 'array'
    TYPE_MULTILINE = 'multiline'
    TYPE_OBJECT = 'object'

    def __init__(self, json_type: Literal['array', 'multiline', 'object'], encoding='utf-8', error_log='error.log'):
        """Initialize json file format

        Args:
            json_type (): Literal['array', 'multiline', 'object']
        """
        super().__init__(encoding=encoding, error_log=error_log)
        self.json_type = json_type

    def load(self, file_or_filename: Union[io.TextIOWrapper, io.BytesIO, str], data_key: str = None):
        """파일 객체나 파일명에서 JSON 데이터 읽기
        Args:
            file_or_filename (file-like object): file or s3 stream object which support .read() function
            data_key (str): if empty use whole file, else use only key value. for example 'data'
        Returns:
            list of json object, which passing load json processing till now
        """
        open_mode = self._decide_rw_open_mode('load')
        # file_or_filename 클래스 유형에 따라서 처리 방법이 다름
        fp, mode, opened = self._get_file_obj(file_or_filename, open_mode)

        if self.json_type == JsonHandler.TYPE_OBJECT or self.json_type == JsonHandler.TYPE_ARRAY:
            try:
                root_json = json.load(fp)
                self._update_json_data(root_json, data_key)
            except Exception as e:
                file_bytes = fp.read()
                self.fail_list.append(file_bytes)
                logger.error(f"{fp=}, mode='{mode}' {opened=} json_type='{self.json_type}' load raise {e}")
        elif self.json_type == JsonHandler.TYPE_MULTILINE:
            for line in fp:
                try:
                    if 'text' == mode:
                        line_str = line
                        line_obj = json.loads(line_str)
                        self._update_json_data(line_obj, data_key)
                    elif 'binary' == mode:
                        line_str = line.decode(self.encoding)
                        line_obj = json.loads(line_str)
                        self._update_json_data(line_obj, data_key)
                except Exception as e:
                    self.fail_list.append(line)
                    logger.error(f"{fp=}, mode='{mode}' {opened=} json_type='{self.json_type}' load raise {e}")
        # close opened file if filename
        if opened and fp:
            fp.close()

    def loads(self, str_or_bytes: Union[str, bytes], data_key: str = None):
        """문자열이나 bytes 에서 JSON 객체 읽기

        데이터 처리 결과는 객체 내부에 성공 목록과 실패 목록으로 저장됨

        Args:
            str_or_bytes (str, bytes): text 모드 string 또는 binary 모드 bytes
            data_key (str): if empty use whole file, else use only key value. for example 'data'
        """
        try:
            if isinstance(str_or_bytes, str):
                file_obj = io.StringIO(str_or_bytes)
                self.load(file_obj, data_key=data_key)
            elif isinstance(str_or_bytes, bytes):
                file_obj = io.BytesIO(str_or_bytes)
                self.load(file_obj, data_key=data_key)
        except Exception as e:
            self.fail_list.append(str_or_bytes)
            logger.error(f"'{str_or_bytes}' loads raise {e}")

    def to_pandas(self) -> pd.DataFrame:
        """클래스 내부메쏘드 JSON 파일 처리 결과를 pd.DataFrame 형태로 받음

        내부적으로 추가할 데이터(pass_list)가 있으면 추가하여 새로운 pd.DataFrame 생성
        실패 목록(fail_list)가 있으면 파일로 저장
        학습을 위한 dataframe 이기 떄문에 dot('.') 문자로 normalize 된 flatten 컬럼과 값을 가진다.

        Returns: pandas DataFrame
        """
        if len(self.pass_list) > 0:
            try:
                append_df = pd.json_normalize(self.pass_list)
                merge_df = pd.concat([self.data_df, append_df], ignore_index=True)
                self.data_df = merge_df
            except Exception as e:
                logger.error(f"pass_list[{len(self.pass_list)}] to_pandas raise {e}")
                self.fail_list.extend(self.pass_list)
            finally:
                self.pass_list.clear()
        if len(self.fail_list) > 0:
            error_fp = None
            try:
                error_fp = open(self.error_log, mode='ab')
            except Exception as e:
                logger.error(f"fail_list[{len(self.fail_list)}] error log append raise {e}")

            if error_fp:
                for fail in self.fail_list:
                    try:
                        fail_str = None
                        if isinstance(fail, dict):
                            fail_str = json.dumps(fail, ensure_ascii=False, separators=(',', ':'))
                        elif isinstance(fail, list):
                            fail_str = str(fail)
                        elif not isinstance(fail, str):
                            fail_str = str(fail)

                        if fail_str:
                            fail_bytes = fail_str.encode(self.encoding)
                            error_fp.write(fail_bytes)
                            error_fp.write(b'\n')
                    except Exception as e:
                        logger.exception(e)
                error_fp.close()
                self.fail_list.clear()
        return self.data_df

    def dump(self, file_or_filename: Union[io.TextIOWrapper, io.BytesIO, str], data=None, data_key=None) -> None:
        """데이터를 JSON 파일로 쓰기

        파일은 text, binary 모드 파일객체이거나 파일명 문자열
        Args:
            file_or_filename (file, str): 파일객체 또는 파일명, text 모드는 TextIOWrapper, binary 모드는 BytesIO 사용
            data: use this data instead of self.data_df if provide 기능 확장성과 호환성을 위해서 남김
            data_key (str): if empty use whole file, else use only key value. for example 'data'

        Returns:
            없음
        """
        fp = None
        mode = ''
        opened = False
        try:
            open_mode = self._decide_rw_open_mode('dump')
            # file_or_filename 클래스 유형에 따라서 처리 방법이 다름
            fp, mode, opened = self._get_file_obj(file_or_filename, open_mode)
            if not data:
                # dataframe 에 추가할 것 있으면 concat
                data = self.to_pandas()
        except Exception as e:
            self.fail_list.append(data)
            logger.error(f"{fp=}, mode='{mode}' {opened=} '{self.json_type}' dump raise {e}")

        # json_type 구분
        if self.json_type == JsonHandler.TYPE_ARRAY:
            try:
                # dataframe -> json array
                if isinstance(data, pd.DataFrame):
                    json_list = data.to_dict('records')
                # data is list -> json array
                elif isinstance(data, list):
                    json_list = data
                else:
                    json_list = []
                    self.fail_list.append(data)
                    logger.error(f"{fp=}, mode='{mode}' {opened=} '{self.json_type}' but {type(data)} is not list")

                if data_key:
                    # dump_key = f"'{data_key}'"
                    json.dump({data_key: json_list}, fp)
                else:
                    json.dump(json_list, fp)
            except Exception as e:
                self.fail_list.append(data)
                logger.error(f"{fp=}, mode='{mode}' {opened=} '{self.json_type}' dump raise {e}")

        # 'multiline' 유형에서는 강제로 binary 모드를 사용한다
        elif self.json_type == JsonHandler.TYPE_MULTILINE:
            # data 형태 구분: dataframe, list, object
            try:
                # dataframe -> json array
                if isinstance(data, pd.DataFrame):
                    json_list = data.to_dict('records')
                # data is list -> json array
                elif isinstance(data, list):
                    json_list = data
                # if use data_key case and list
                elif isinstance(data, dict):
                    json_list = [data]
                else:
                    json_list = None
                    logger.error(f"{fp=}, mode='{mode}' {opened=} '{self.json_type}' no support {type(data)}")

                if json_list and len(json_list) > 0:
                    for row in json_list:
                        try:
                            if data_key:
                                # dump_key = f"'{data_key}'"
                                json_obj = {data_key: row}
                            else:
                                json_obj = row

                            # 결과적으로 mode 에 관계없이 binary 로 저장하게됨
                            json_str = json.dumps(json_obj, ensure_ascii=False, separators=(',', ':'))
                            json_bytes = json_str.encode(self.encoding)
                            fp.write(json_bytes)
                            fp.write(b'\n')
                        except Exception as e:
                            self.fail_list.append(row)
                            logger.error(f"{fp=}, mode='{mode}' {opened=} json_type='{self.json_type}' raise {e}")
            except Exception as e:
                self.fail_list.append(data)
                logger.error(f"{fp=}, mode='{mode}' {opened=} json_type='{self.json_type}' dump raise {e}")

        if self.json_type == JsonHandler.TYPE_OBJECT:
            try:
                json_obj = None
                # dataframe -> json object (dict)
                if isinstance(data, pd.DataFrame):
                    data_dict = data.to_dict('records')
                    if isinstance(data_dict, list):
                        if len(data_dict) == 1:
                            json_obj = data_dict[0]
                        else:
                            json_obj = data_dict
                    elif isinstance(data_dict, dict):
                        json_obj = data_dict
                else:
                    json_obj = data

                # if use data_key case
                if data_key:
                    dump_key = f"'{data_key}'"
                    json.dump({dump_key: json_obj}, fp)
                else:
                    json.dump(json_obj, fp)
            except Exception as e:
                self.fail_list.append(data)
                logger.error(f"{fp=}, mode='{mode}' {opened=} '{self.json_type}' dump raise {e}")

        if opened and fp:
            fp.close()

    def dumps(self, mode: Literal['text', 'binary'] = 'text', data=None, data_key='') -> Union[str, bytes]:
        """JSON 데이터를 문자열 또는 바이너리 형태로 출력

        파일은 text, binary 모드 파일객체이거나 파일명 문자열
        Args:
            mode (str): 출력 모드 'text' 또는 'binary' 선택
            data (): 출력할 데이터, 생략되면 self.data_df 사용
            data_key (str): 출력을 json object 로 한번 더 감쌀 경우에 사용

        Returns:
            데이터를 'text' 모드에서는 문자열, 'binary' 모드에서는 bytes 출력
        """
        try:
            file_obj = None
            if 'text' == mode:
                file_obj = io.StringIO()
            elif 'binary' == mode:
                file_obj = io.BytesIO()
            if file_obj:
                self.dump(file_obj, data=data, data_key=data_key)
                return file_obj.getvalue()
        except Exception as e:
            logger.error(f"mode='{mode}' json_type='{self.json_type}' dumps raise {e}")
        return ""

    """
    클래스 내부 메쏘드 
    """

    # 내부 함수 for object and array json_type
    def _update_json_data(self, json_obj, data_key) -> None:
        """내부메쏘드 json_obj 처리 결과 반영

        Args:
            json_obj: 설정할 json object

        """
        # data_key 처리
        if data_key:
            if data_key in json_obj:
                json_value = json_obj[data_key]
                if isinstance(json_value, str):
                    json_obj = json.loads(json_value)
                elif isinstance(json_value, dict):
                    pass
                else:
                    self.fail_list.append(json_obj)
                    raise TypeError(f"json_obj['{data_key}'] {type(json_value)} not supported")
            else:
                self.fail_list.append(json_obj)
                raise TypeError(f"json_obj['{data_key}'] must exist")

        # json_obj 처리
        if self.json_type == JsonHandler.TYPE_ARRAY:
            # json_array 가 진짜 array (list) 인지 검사
            if isinstance(json_obj, list):
                self.pass_list.extend(json_obj)
            else:
                self.fail_list.append(json_obj)
                raise TypeError(f"json_obj['{data_key}'] in {self.json_type=} must be a list")
        elif self.json_type == JsonHandler.TYPE_MULTILINE:
            if isinstance(json_obj, dict):
                self.pass_list.append(json_obj)
            else:
                self.fail_list.append(json_obj)
                raise TypeError(f"json_obj['{data_key}'] in {self.json_type=} must be a dict")
        elif self.json_type == JsonHandler.TYPE_OBJECT:
            self.pass_list.append(json_obj)

    def _decide_rw_open_mode(self, method_name) -> str:
        """내부메쏘드 json_type 과 method_name 에 따라서 파일 일기/쓰기 오픈 모드 결정

        Args:
            method_name: 'load' or 'dump'

        Returns: 'r', 'w', 'rb', 'wb'
        """
        if 'dump' == method_name:
            if self.json_type == JsonHandler.TYPE_MULTILINE:
                return 'wb'
            else:
                return 'w'
        elif 'load' == method_name:
            if self.json_type == JsonHandler.TYPE_MULTILINE:
                return 'rb'
            else:
                return 'r'
        else:
            raise TypeError(f"method_name='{method_name}'] not supported yet.")
