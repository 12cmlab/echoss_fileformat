"""
    echoss AI Bigdata Center Solution - file format utilty
"""
import io
import logging
import pandas as pd
from typing import Dict, Literal, Optional, Union

logger = logging.getLogger(__name__)


class FileformatBase:
    """AI 학습을 위한 파일 포맷 지원 기반 클래스

    JSON, CSV, XML and excel file format handler 부모 클래스
    클래스 공통 내부 메쏘드를 제외하면 클래스 공개 메쏘드는 모두 자식 클래스에서 구현

    내부 자료 구조는 다를 수 있지만,
    외부 연동은 pandas dataframe 만 사용함

    클래스 공개 메쏘드는 AI 학습에 필요한 최소한 기능에만 집중.
    세부 기능과 다양한 확장은 관련 패키지를 직정 사용 권고

    학습데이터로는 processing_type 'array' 와 'multiline' 만 사용 권고

    'array' 는 전체 객체가 array 형태로 list of dictionary 로 누적 처리
    'multiline' JSON 파일에서만 사용. 파일의 각 줄을 하나의 JSON object(dict) 형태로  읽어서 누적 처리

    특정 키만 학습데이터로 사용할 경우에는 자식클래스 구현마다 다름. data_key 또는 usecols 사용

    'object' 는 학습데이터가 아닌 메타 정보 파일에만 사용 권고. 처리 후에 내부 저장하지 않고 즉시 1개의 객체(구현마다 다름)로 리턴
    """
    TYPE_ARRAY = 'array'
    TYPE_MULTILINE = 'multiline'
    TYPE_OBJECT = 'object'

    def __init__(self, processing_type='array', encoding='utf-8', error_log='error.log'):
        """       
        Args:
            processing_type (): Literal['array', 'multiline', 'object']
            encoding: 파일 인코팅 
            error_log: 파일 처리 실패 시 에러 저장 파일명 
        """
        self.processing_type = processing_type;
        self.data_df = pd.DataFrame()
        self.encoding = encoding
        self.error_log = error_log
        self.pass_list = []
        self.fail_list = []

    def load(self, file_or_filename: Union[io.TextIOWrapper, io.BytesIO, str]) -> Optional[object]:
        """파일에서 데이터를 읽기

        파일 처리 결과는 객체 내부에 성공 목록과 실패 목록으로 저장됨

        Args:
            file_or_filename (file, str): 파일객체 또는 파일명

        """
        pass

    def loads(self, str_or_bytes: Union[str, bytes])  -> Optional[object]:
        """문자열이나 binary 을 데이터로 읽기

        파일 처리 결과는 객체 내부에 성공 목록과 실패 목록으로 저장됨

        Args:
            str_or_bytes (str, bytes): text 모드 string 또는 binary 모드 bytes

        """
        pass

    def to_pandas(self) -> pd.DataFrame:
        """파일 처리 결과를 모두 반영하여 pd.DataFrame 형태로 출력함

        파일 포맷에 따라서 내부 구현이 달라짐

        Returns: pandas 데이터프레임

        """
        pass

    def dump(self, file_or_filename: Union[io.TextIOWrapper, io.BytesIO, str], data=None) -> None:
        """데이터를 파일로 쓰기

        파일은 text, binary 모드 파일객체이거나 파일명 문자열
        Args:
            file_or_filename (file, str): 파일객체 또는 파일명, 파일객체는 text모드는 TextIOWrapper, binary모드는 BytestIO사용
            data: use this data instead of self.data if provide 기능 확장성과 호환성을 위해서 남김

        Returns:
            없음
        """
        pass

    def dumps(self, mode: Literal['text', 'binary'] = 'text', data=None) -> Union[str, bytes]:
        """데이터를 문자열 형태로 출력

        Args:
            mode (): 출력 모드 'text' 또는 'binary' 선택
            data (): 출력할 데이터, 생략되면 self.data 사용

        Returns:
            데이터를 text 모드에서는 str, 'binary' 모드에서는 bytes 타입 출력
        """
        pass

    """
    
    클래스 내부 메쏘드 
    
    """

    def _get_file_obj(self, file_or_filename, open_mode: str):
        """클래스 내부 메쏘드 file_or_filename 의 instance type 을 확인하여 사용하기 편한 file object 로 변환

        Args:
            file_or_filename: file 관련 객체 또는 filename

            open_mode(): Literal['r', 'w', 'a', 'rb', 'wb', 'ab']

        Returns: file_obj, mode, opened
            file_obj: file object to read, write and split lines

            mode: 'text' or 'binary'

            opened: True if file is opened in this method, False else
        """
        # file_or_filename 클래스 유형에 따라서 처리 방법이 다름
        opened = False
        if open_mode not in ['r', 'w', 'a', 'rb', 'wb', 'ab']:
            raise TypeError(f"{open_mode=} is not supported")

        if isinstance(file_or_filename, io.TextIOWrapper):
            fp = file_or_filename
            mode = 'text'
        # AWS s3 use io.BytesIO
        elif isinstance(file_or_filename, io.BytesIO):
            fp = file_or_filename
            mode = 'binary'
        # open 'rb' use io.BufferedIOBase (BufferedReader or BufferedWriter)
        elif isinstance(file_or_filename, io.BufferedIOBase):
            fp = file_or_filename
            # fp = io.BytesIO(file_or_filename.read())
            if 'b' in fp.mode:
                mode = 'binary'
            else:
                mode = 'text'
        elif isinstance(file_or_filename, str):
            try:
                if 'b' in open_mode:
                    fp = open(file_or_filename, open_mode)
                    mode = 'binary'
                else:
                    fp = open(file_or_filename, open_mode, encoding=self.encoding)
                    mode = 'text'
            except Exception as e:
                logger.error(f"{file_or_filename} is not exist filename or can not open mode='{open_mode}' encoding={self.encoding} {e}")
                raise e
            else:
                opened = True
        else:
            raise TypeError(f"{file_or_filename} is not file obj")
        return fp, mode, opened
