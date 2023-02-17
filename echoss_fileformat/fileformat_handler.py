"""
    echoss AI Bigdata Center Solution - file format utilty
"""
import io
from typing import Union, Dict, Literal
import pandas as pd


class FileformatHandler:
    """AI 학습을 위한 파일 포맷 지원

    JSON, CSV, XML and excel file format handler의 부모 클래스

    Attributes:
        data ():  데이터 객체가 dataframe 일 경우 사용. 외부적으로 사용
        data_obj (): 파일 내용이 담긴 원천 데이터 객체. 내부적으로 사용
        encoding (): 파일 인코딩 'utf-8' 기본
        support_kw (dict): 클래스에서 지원하는 함수별 지원 키워드: 디폴트값 선언
    """
    data: pd.DataFrame
    data_obj: object
    support_kw: dict = {
        'load': {

        },
        'dump:': {

        }
    }

    def __init__(self, encoding='utf-8'):
        self.data = None
        self.data_obj = None
        self.encoding = encoding
        pass

    def make_kw_dict(self, method_type_name: str, kw_dict: dict) -> dict:
        """내부 메쏘드로 서브클래스에 선언된 지원 키워드 사전 획득

        Returns:
            서브클래스의 지원 키워드 사전
        """
        copy_dict = {}
        # handler_kw_dict = self.get_kw_dict()
        if method_type_name in self.support_kw:
            copy_dict = self.support_kw[method_type_name].copy()
            for k in kw_dict:
                if k in copy_dict:
                    copy_dict[k] = kw_dict[k]
            return copy_dict
        return copy_dict

    def load(self, file_or_filename: Union[io.TextIOWrapper, io.BytesIO, str]) -> pd.DataFrame:
        """파일에서 데이터를 읽기

        Args:
            file_or_filename (file, str): 파일객체 또는 파일명, 파일객체는 text모드는 TextIOWrapper, binary모드는 BytestIO사용

        Returns:
            dataframe

        """
        pass

    def loads(self, str_or_bytes: Union[str, bytes]) -> pd.DataFrame:
        """문자열이나 binary을 데이터 읽기

        Args:
            str_or_bytes (str, bytes): text모드 string 또는 binary모드 bytes

        Returns:
            dataframe

        """
        pass

    def dump(self, file_or_filename: Union[io.TextIOWrapper, io.BytesIO, str], data=None) -> None:
        """데이터를 파일로 쓰기

        파일은 text, binary 모드 파일객체이거나 파일명 문자열
        Args:
            file_or_filename (file, str): 파일객체 또는 파일명, 파일객체는 text모드는 TextIOWrapper, binary모드는 BytestIO사용

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

