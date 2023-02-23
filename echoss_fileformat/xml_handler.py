import io
import json
import logging
import pandas as pd
from typing import Dict, Literal, Optional, Union
import xml.etree.ElementTree as ET

from .fileformat_base import FileformatBase

logger = logging.getLogger(__name__)


class XmlHandler(FileformatBase):
    """XML file handler

    XML 은 json_type 과 같은 구분이 없이 전체 파일을 하나의 tree로 처리함
    ('multiline' 형태는 지원하지 않음)

    'array' 유형은 전체 XML 읽기 후 트리구조를 따라가면서 dictionary 로 만들어서 처리함
    특정 키만 학습데이터로 사용할 경우에는 data_key 로 키를 지정하여 처리되는 값을 지정

    'object' 는 학습 데이터가 아니라 메타 정보를 읽기 위해서 사용. dataframe 으로 바꾸지 않고 그대로 사욤
    """

    def __init__(self, processing_type: Literal['array', 'object'] = 'array',
                 encoding='utf-8', error_log='error.log'):
        """Initialize XML file format

        Args:
            processing_type (): Literal['array', 'object'] XML 은 'multiline' 지원 안함
        """
        super().__init__(processing_type=processing_type, encoding=encoding, error_log=error_log)

        # root 노드의 tag 는 필수 이므로 기본값을 'data' 로 설정. top level 노드의 tag 값도 필수 'row' 기본값
        self.root_tag = 'data'
        self.child_tag = 'row'

    def load(self, file_or_filename: Union[io.TextIOWrapper, io.BytesIO, io.BufferedIOBase, str],
             data_key: str = None, usecols: list = None) -> Optional[ET.Element]:
        """파일 객체나 파일명에서 JSON 데이터 읽기

        Args:
            file_or_filename (file-like object): file or s3 stream object which support .read() function
            data_key (str): if empty use whole file, else use only key value. for example 'data'
            usecols (list]): 전체 키 사용시 None, 이름의 리스트 ['foo', 'bar', 'baz'] 처럼 사용

        Returns:
            list of json object, which passing load json processing till now
        """
        try:
            open_mode = self._decide_rw_open_mode('load')

            # file_or_filename 클래스 유형에 따라서 처리 방법이 다른 것을 일원화
            fp, mode, opened = self._get_file_obj(file_or_filename, open_mode)

            # 전체 파일을 일고 나머지 처리
            tree = ET.parse(fp)
            root = tree.getroot()
            # root 의 tag 값을 기억
            self.root_tag = root.tag
        except Exception as e:
            self.fail_list.append(str(file_or_filename))
            logger.error(f"'{file_or_filename}' load raise {e}")
            raise e

        # 'object' 처리 유형의 파일 처리는 루트 트리를 바로 리턴
        if self.processing_type == FileformatBase.TYPE_OBJECT:
            return root

        # 'array' 처리 유형의 파일 처리
        try:          # data_key 과 usecols 확인
            if not data_key:
                data_nodes = root
            else:
                data_nodes = root.findall(data_key)
        except Exception as e:
            self.fail_list.append(str(file_or_filename))
            logger.error(f"'{file_or_filename}' load raise {e}")
            raise e

        for child in data_nodes:
            node_dict = {}
            try:
                if usecols:
                    for key in usecols:
                        node_dict[key] = child.find(key).text
                else:
                    for node in child:
                        node_dict[node.tag] = node.text
                self.pass_list.append(node_dict)
                self.child_tag = child.tag
            except Exception as e:
                self.fail_list.append(str(child))
                logger.error(f"'{file_or_filename}' load raise {e}")

        if opened and fp:
            fp.close()

    def loads(self, str_or_bytes: Union[str, bytes],
              data_key: str = None, usecols: list = None) -> Optional[ET.Element]:
        """문자열이나 bytes 에서 XML 객체 읽기

        데이터 처리 결과는 객체 내부에 성공 목록과 실패 목록으로 저장됨

        Args:
            str_or_bytes (str, bytes): text 모드 string 또는 binary 모드 bytes
            data_key (str): if empty use whole file, else use only key value. for example 'data'
            usecols (list]): 전체 키 사용시 None, 이름의 리스트 ['foo', 'bar', 'baz'] 처럼 사용
        """
        try:
            if isinstance(str_or_bytes, str):
                file_obj = io.StringIO(str_or_bytes)
                root = self.load(file_obj, data_key=data_key, usecols=usecols)
            elif isinstance(str_or_bytes, bytes):
                file_obj = io.BytesIO(str_or_bytes)
                root = self.load(file_obj, data_key=data_key, usecols=usecols)
        except Exception as e:
            self.fail_list.append(str_or_bytes)
            logger.error(f"'{str_or_bytes}' loads raise {e}")

        # 'object' 처리 유형의 파일 처리는 루트 트리를 바로 리턴
        if self.processing_type == FileformatBase.TYPE_OBJECT:
            return root

    def to_pandas(self) -> pd.DataFrame:
        """클래스 내부메쏘드 JSON 파일 처리 결과를 pd.DataFrame 형태로 받음

        내부적으로 추가할 데이터(pass_list)가 있으면 추가하여 새로운 pd.DataFrame 생성
        실패 목록(fail_list)가 있으면 파일로 저장
        학습을 위한 dataframe 이기 떄문에 dot('.') 문자로 normalize 된 flatten 컬럼과 값을 가진다.

        Returns: pandas DataFrame
        """
        if self.processing_type == FileformatBase.TYPE_OBJECT:
            logger.error(f"{self.processing_type} not support to_pandas() method")
            return None

        if len(self.pass_list) > 0:
            try:
                append_df = pd.DataFrame(self.pass_list)
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

    def dump(self, file_or_filename: Union[io.TextIOWrapper, io.BytesIO, str],
             data=None, root_tag=None, child_tag=None) -> None:
        """데이터를 JSON 파일로 쓰기

        파일은 text, binary 모드 파일객체이거나 파일명 문자열
        Args:
            file_or_filename (file, str): 파일객체 또는 파일명, text 모드는 TextIOWrapper, binary 모드는 BytesIO 사용
            data: use this data instead of self.data_df if provide 기능 확장성과 호환성을 위해서 남김
            root_tag (str): XML 파일은 전체를 묶을 루트 노드가 필요. 지정하지 않으면 이전 load()에서 얻은 값이나 초기값 'data' 사용
            child_tag (str): 루트 노드 아래 자식노드도 이름 필요. 지정하지 않으면 이전 load()에서 얻은 값이나 초기값 'row' 사용
        Returns:
            없음
        """
        if self.processing_type == FileformatBase.TYPE_OBJECT:
            if not data:
                raise TypeError(f"'{self.processing_type=}' dump() must have data parameter")
            if not (isinstance(data, ET.Element) or (root_tag and child_tag)):
                raise TypeError(f"'{self.processing_type=}' dump() must have root_tag and child_tag by type(Element)")

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

        if not root_tag:
            root_tag = self.root_tag
        if not child_tag:
            child_tag = self.child_tag

        try:
            if isinstance(data, ET.Element):
                data.write(fp, encoding='utf-8', xml_declaration=True)
            # dataframe -> json array
            elif isinstance(data, pd.DataFrame):
                df: pd.DataFrame = data
                # dataframe 에서 직접 XML 생성
                root = ET.Element(root_tag)
                for i in range(len(df)):
                    row = ET.SubElement(root, child_tag)
                    for column in df.columns:
                        value = str(df[column].iloc[i])
                        element = ET.SubElement(row, column)
                        element.text = value

                    tree = ET.ElementTree(root)
                    tree.write(fp, encoding=self.encoding, xml_declaration=True)

            # data is list -> json array
            elif isinstance(data, list) and all(isinstance(item, dict) for item in data):
                # list 에서 직접 XML 생성
                root = ET.Element(root_tag)
                for item in data:
                    row = ET.SubElement(root, child_tag)
                    for key in dict:
                        element = ET.SubElement(row, key)
                        element.text = dict[key]

                    tree = ET.ElementTree(root)
                    tree.write(fp, encoding=self.encoding, xml_declaration=True)
            else:
                dict_list = []
                self.fail_list.append(data)
                logger.error(f"{fp=}, mode='{mode}' {opened=} but {type(data)} is supported")
        except Exception as e:
            self.fail_list.append(data)
            logger.error(f"{fp=}, mode='{mode}' {opened=} '{self.json_type}' dump raise {e}")

        if opened and fp:
            fp.close()

    def dumps(self, mode: Literal['text', 'binary'] = 'text',
              data=None, root_tag=None, child_tag=None) -> Union[str, bytes]:
        """JSON 데이터를 문자열 또는 바이너리 형태로 출력

        파일은 text, binary 모드 파일객체이거나 파일명 문자열
        Args:
            mode (str): 출력 모드 'text' 또는 'binary' 선택
            data (): 출력할 데이터, 생략되면 self.data_df 사용
            root_tag (str): XML 파일은 전체를 묶을 루트 노드가 필요. 지정하지 않으면 이전 load()에서 얻은 값이나 초기값 'data' 사용
            child_tag (str): 루트 노드 아래 자식노드도 이름 필요. 지정하지 않으면 이전 load()에서 얻은 값이나 초기값 'row' 사용

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
                self.dump(file_obj, data=data, root_tag=root_tag, child_tag=child_tag)
                return file_obj.getvalue()
        except Exception as e:
            logger.error(f"mode='{mode}' json_type='{self.json_type}' dumps raise {e}")
        return ""

    """
    클래스 내부 메쏘드 
    """

    def _decide_rw_open_mode(self, method_name) -> str:
        """내부메쏘드 json_type 과 method_name 에 따라서 파일 일기/쓰기 오픈 모드 결정

        메쏘드에 입력된 매개변수가 filename 이라서 open() 호출 시에 mode 문자열을 결정하기위해서 사용

        Args:
            method_name: 'load' or 'dump'

        Returns:
            Literal['r', 'w', 'rb', 'wb']
        """
        if 'dump' == method_name:
            return 'w'
        elif 'load' == method_name:
            return 'r'
        else:
            raise TypeError(f"'{self.processing_type}' method_name='{method_name}'] not supported yet.")

