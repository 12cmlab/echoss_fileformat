"""
    echoss AI Bigdata Center Solution - file format utilty
"""
import io
import logging
from typing import Union, Dict, Literal
import pandas as pd

logger = logging.getLogger(__name__)


class FileformatBase:
    """AI 학습을 위한 파일 포맷 지원 기반 클래스

    JSON, CSV, XML and excel file format handler 부모 클래스
    클래스 공통 내부 메쏘드를 제외하면 클래스 공개 메쏘드는 모두 자식 클래스에서 구현

    클래스 공개 메쏘드는 AI학습에 필요한 최소한 기능에만 집중.
    세부 기능과 다양한 확장은 관련 패키지를 직정 사용 권고
    """
    # 최소기능에 집중하고 kwargs 관련 기능은 사용하지 않는 것으로 결정
    # support_kw: dict = {
    #     'load': {
    #     },
    #     'dump:': {
    #     }
    # }

    def __init__(self, encoding='utf-8', error_log='error.log'):
        """       
        Args:
            encoding: 파일 인코팅 
            error_log: 파일 처리 실패 시 에러 저장 파일명 
        """
        self.data = pd.DataFrame()
        self.encoding = encoding
        self.error_log = error_log
        self.pass_list = []
        self.fail_list = []

    def load(self, file_or_filename: Union[io.TextIOWrapper, io.BytesIO, str]):
        """파일에서 데이터를 읽기

        파일 처리 결과는 객체 내부에 성공 목록과 실패 목록으로 저장됨

        Args:
            file_or_filename (file, str): 파일객체 또는 파일명, 파일객체는 text모드는 TextIOWrapper, binary모드는 BytestIO사용

        """
        pass

    def loads(self, str_or_bytes: Union[str, bytes]):
        """문자열이나 binary을 데이터 읽기

        파일 처리 결과는 객체 내부에 성공 목록과 실패 목록으로 저장됨

        Args:
            str_or_bytes (str, bytes): text모드 string 또는 binary모드 bytes

        """
        pass

    def to_pandas(self) -> pd.DataFrame:
        """파일 처리 결과를 pd.DataFrame 형태로 받음.

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

    def dumps(self, mode: Literal['text', 'binary'] = 'text', data=None ) -> Union[str, bytes]:
        """데이터를 문자열 형태로 출력

        Args:
            mode (): 출력 모드 'text' 또는 'binary' 선택
            data (): 출력할 데이터, 생략되면 self.data 사용
        Returns:
            데이터를 text모드에서는 문자열, 'binary'모드에서는 bytes로 출력
        """
        pass

    def get_data(self, need_update = True) -> pd.DataFrame:
        """dataframe data get

        Args:
            need_update (Bool): need_update (Bool): update processing pass_list and fail_list first? default True

        Returns: data (pd.DataFrame)

        """
        if need_update:
            df = self._to_pandas()
        return df

    def set_data(self, data: pd.DataFrame, need_update = True) -> None:
        """dataframe data set

        Args:
            data (): set 할 dataframe
            need_update (Bool): update processing pass_list and fail_list first? default True
        """
        if need_update:
            self.to_pandas()
        self.data = data

    """
    
    클래스 내부 메쏘드 
    
    """

    # 사용하는 았는 것으로 정리
    # def _make_kw_dict(self, method_type_name: str, kw_dict: dict) -> dict:
    #     """내부 메쏘드로 서브클래스에 선언된 지원 키워드 사전 획득
    #
    #     Returns:
    #         서브클래스의 지원 키워드 사전
    #     """
    #     copy_dict = {}
    #     # handler_kw_dict = self.get_kw_dict()
    #     if method_type_name in self.support_kw:
    #         copy_dict = self.support_kw[method_type_name].copy()
    #         for k in kw_dict:
    #             if k in copy_dict:
    #                 copy_dict[k] = kw_dict[k]
    #         return copy_dict
    #     return copy_dict

    def _get_file_obj(self, file_or_filename, open_mode: str):
        """클래스 내부 메쏘드 file_or_filename 의 instance type을 확인하여 사용하기 편한 file object 로 변환

        Args:
            file_or_filename: file 관련 객체 또는 filename

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
            mode = 'binary'
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

    def _to_pandas(self):
        """클래스 내부 메쏘드 처리 데이터를 dataframe 의 변환하여 저장. 자식 클래스에서 각각 구현
        """
        pass