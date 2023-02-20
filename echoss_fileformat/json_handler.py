import logging

from .fileformat_base import FileformatBase
import io
import json
import pandas as pd
import numpy as np
from typing import Union, Dict, Literal


logger = logging.getLogger(__name__)


class JsonHandler(FileformatBase):
    """JSON file handler

    학습데이터로 JSON 파일은 전체가 'array' 이거나, object 의 특정 키 값이 'array' 라고 추정
    또는 한줄이 JSON 형태인 'multiline' 파일일 수 있음
    json_type 'object' 는 학습데이터가 아닌 전체 파일을 읽어들일 떄에만 사용
    """
    TYPE_ARRAY = 'array'
    TYPE_MULTILINE = 'multiline'
    TYPE_OBJECT = 'object'

    # # 실제 json 메쏘드 호출 시 지원하는 추가 키워드
    # support_kw = {
    #     'load': {
    #         'cls': None,
    #     },
    #     'dump:': {
    #         'cls': None,
    #     }
    # }

    def __init__(self, json_type: Union['array', 'multiline', 'object'], encoding='utf-8', error_log='error.log'):
        """Initialize json file format

        Args:
            json_type (Union['object', 'array', 'multiline']): 'object' for one json object, 'array' for json array, 'multiline' for objects each lines
        """
        super().__init__(encoding=encoding, error_log=error_log)
        self.json_type = json_type
        self.data_key = ''

    # def _append_json_data(self, json_obj) -> None:
    #     """'multiline' json_type 내부메쏘드 data_obj 에 json_obj 추가
    #
    #     Args:
    #         json_obj: 추가할 json object
    #     """
    #     if not self.data_key:
    #         self.origin_obj.append(json_obj)
    #     elif self.data_key in json_obj:
    #         self.origin_obj.append(json_obj[self.data_key])
    #     else:
    #         self.failed_obj.append(json_obj)

    # 내부 함수 for object and array json_type
    def _update_json_data(self, json_obj) -> None:
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
                if isinstance(json_obj[self.data_key], list):
                    self.pass_list.extend(json_obj[self.data_key])
                else:
                    self.fail_list.append(json_obj[self.data_key])
                    raise TypeError('json_obj[self.data_key] must be a list')
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

    def _decide_rw_open_mode(self, method_name) -> str :
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
        else:  #  'load'
            if self.json_type == JsonHandler.TYPE_MULTILINE:
                return 'rb'
            else:
                return 'r'

    def load(self, file_or_filename: Union[io.TextIOWrapper, io.BytesIO, str], data_key: str = '') -> list:
        """파일 객체나 파일명에서 JSON 데이터 읽기
        Args:
            file_or_filename (file-like object): file or s3 stream object which support .read() function
            data_key (str): if empty use whole file, else use only key value. for example 'data'
        Returns:
            list of json object, which passing load json processings
        """
        self.data_key = data_key
        # kw_dict = self._make_kw_dict('load', kwargs)
        open_mode = self._decide_rw_open_mode('load')
        # file_or_filename 클래스 유형에 따라서 처리 방법이 다름
        fp, mode, opened = self._get_file_obj(file_or_filename, open_mode)

        if self.json_type == JsonHandler.TYPE_OBJECT or self.json_type == JsonHandler.TYPE_ARRAY:
            try:
                root_json = json.load(fp)
                self._update_json_data(root_json)
            except Exception as e:
                file_bytes = fp.read()
                self.fail_list.append(file_bytes)
                logger.error(f"{fp=}, mode='{mode}' {opened=} json_type='{self.json_type}' load raise {e}")
        elif self.json_type == JsonHandler.TYPE_MULTILINE:
            for line in fp:
                try:
                    if 'text' == mode:
                        line_str = line
                    elif 'binary' == mode:
                        line_str = line.decode(self.encoding)
                    json_obj = json.loads(line_str)
                    self._update_json_data(json_obj)
                except Exception as e:
                    self.fail_list.append(line)
                    logger.error(f"{fp=}, mode='{mode}' {opened=} json_type='{self.json_type}' load raise {e}")
        # close opened file if filename
        if opened and fp:
            fp.close()
        return self.pass_list

    def loads(self, str_or_bytes: Union[str, bytes]):
        """문자열이나 bytes 에서 JSON 객체 읽기

        데이터 처리 결과는 객체 내부에 성공 목록과 실패 목록으로 저장됨

        Args:
            str_or_bytes (str, bytes): text모드 string 또는 binary모드 bytes

        """
        try:
            if isinstance(str_or_bytes, str):
                file_obj = io.StringIO(str_or_bytes)
            elif isinstance(str_or_bytes, bytes):
                file_obj = io.BytesIO(str_or_bytes)
        except Exception as e:
            self.fail_list.append(str_or_bytes)
            logger.error(f"'{str_or_bytes}' loads raise {e}")
        finally:
            return self.load(file_obj)

    def dump(self, file_or_filename: Union[io.TextIOWrapper, io.BytesIO, str], data=None) -> None:
        """데이터를 JSON 파일로 쓰기

        파일은 text, binary 모드 파일객체이거나 파일명 문자열
        Args:
            file_or_filename (file, str): 파일객체 또는 파일명, 파일객체는 text모드는 TextIOWrapper, binary모드는 BytestIO사용
            data: use this data instead of self.data if provide 기능 확장성과 호환성을 위해서 남김

        Returns:
            없음
        """
        fp = None
        mode=''
        opened=False
        try:
            open_mode = self._decide_rw_open_mode('dump')
            # file_or_filename 클래스 유형에 따라서 처리 방법이 다름
            fp, mode, opened = self._get_file_obj(file_or_filename, open_mode)

            # data의 형태 구분: dataframe, list, object
            if not data:
                # dataframe 에 추가할 것 있으면 concat
                data = self._to_pandas()
        except Exception as e:
            self.fail_list.append(data)
            logger.error(f"{fp=}, mode='{mode}' {opened=} json_type='{self.json_type}' dump raise {e}")

        if self.json_type == JsonHandler.TYPE_OBJECT:
            try:
                json_obj = None
                # dataframe -> json object (dict)
                if isinstance(data, pd.DataFrame):
                    data_dict = data.to_dict('records')
                    if isinstance(data_dict, list) and len(data_dict) > 0:
                        json_obj = data_dict[0]
                    elif isinstance(data_dict, dict):
                        json_obj = data_dict
                # data is list -> json array
                elif isinstance(data, list):
                    logger.error(f"{fp=}, mode='{mode}' {opened=} json_type='{self.json_type}' not dict but list")
                elif isinstance(data, dict):
                    json_obj = data

                # if use data_key case and list
                if json_obj and self.data_key:
                    if self.data_key in json_obj:
                        json_obj = data[self.data_key]
                    else:
                        logger.error(f"{fp=}, data_key={self.data_key} is not exist in {json_obj}")
                if json_obj:
                    json.dump(json_obj, fp)
                else:
                    logger.error(f"{fp=}, mode='{mode}' {opened=} json_type='{self.json_type}' but json object=None")
            except Exception as e:
                self.fail_list.append(data)
                logger.error(f"{fp=}, mode='{mode}' {opened=} json_type='{self.json_type}' dump raise {e}")
        elif self.json_type == JsonHandler.TYPE_ARRAY:
            try:
                # dataframe -> json array
                if isinstance(data, pd.DataFrame):
                    data.to_json(fp, orient='records')
                # data is list -> json array
                elif isinstance(data, list):
                    json.dump(data, fp)
                # if use data_key case and list
                elif isinstance(data, dict) and self.data_key and self.data_key in data:
                    data_value = data[self.data_key]
                    if isinstance(data_value, list):
                        json.dump(data_value, fp)
                else:
                    logger.error(f"{fp=}, mode='{mode}' {opened=} json_type='{self.json_type}' but not list")
            except Exception as e:
                self.fail_list.append(data)
                logger.error(f"{fp=}, mode='{mode}' {opened=} json_type='{self.json_type}' dump raise {e}")
        elif self.json_type == JsonHandler.TYPE_MULTILINE:
            try:
                # dataframe -> json array
                if isinstance(data, pd.DataFrame):
                    data_list = data.to_dict('records')
                # data is list -> json array
                elif isinstance(data, list):
                    data_list = data
                # if use data_key case and list
                elif isinstance(data, dict) and self.data_key and self.data_key in data:
                    data_value = data[self.data_key]
                    if isinstance(data_value, list):
                        data_list = data_value
                else:
                    logger.error(f"{fp=}, mode='{mode}' {opened=} json_type='{self.json_type}' but can't multiline")

                if data_list and len(data_list) > 0:
                    for row in data_list:
                        try:
                            json.dump(row, fp)
                        except Exception as e:
                            self.fail_list.append(row)
                            logger.error(f"{fp=}, mode='{mode}' {opened=} json_type='{self.json_type}' raise {e}")
            except Exception as e:
                self.fail_list.append(data)
                logger.error(f"{fp=}, mode='{mode}' {opened=} json_type='{self.json_type}' dump raise {e}")

        if opened and fp:
            fp.close()

    def dumps(self, mode: Literal['text', 'binary'] = 'text', data=None) -> Union[str, bytes]:
        try:
            if 'text' == mode:
                file_obj = io.StringIO()
            elif 'binary' == mode:
                file_obj = io.BytesIO()

            self.dump(self.data, file_obj, data=data)
            return file_obj.getvalue()
        except Exception as e:
            logger.error(f"mode='{mode}' json_type='{self.json_type}' dumps raise {e}")


    """
    클래스 내부 메쏘드 
    """

    def _to_pandas(self) -> pd.DataFrame:
        """클래스 내부메쏘드 JSON 파일 처리 결과를 pd.DataFrame 형태로 받음

        내부적으로 추가할 데이터(pass_list)가 있으면 추가하여 새로운 pd.DataFrame 생성
        실패 목록(fail_list)가 있으면 파일로 저장
        학습을 위한 dataframe 이기 떄문에 dot('.') 문자로 normalize 된 flatten 컬럼과 값을 가진다.

        Returns: pandas DataFrame
        """
        if len(self.pass_list) > 0:
            try:
                append_df = pd.json_normalize(self.pass_list)
                merge_df = pd.concat([self.data, append_df], ignore_index=True)
                self.data = merge_df
            except Exception as e:
                logger.error(f"pass_list[{len(self.pass_list)}] _to_pandas raise {e}")
                self.fail_list.extend(self.pass_list)
            finally:
                self.pass_list.clear()
        if len(self.fail_list) > 0:
            try:
                with open(self.error_log, mode='ab') as error_fp:
                    for fail in self.fail_list:
                        try:
                            error_fp.write(fail)
                            error_fp.write('\n')
                        except Exception as e:
                            logger.exception(e)
            except Exception as e:
                logger.error(f"fail_list[{len(self.fail_list)}] error log append raise {e}")
            finally:
                self.fail_list.clear()
        return self.data
