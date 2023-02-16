import abc
import io
from typing import Union, Dict, Literal
import pandas as pd


class FileformatHandler(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def make_kw_dict(self, kw_name: str, kw_dict: dict) -> dict:
        """내부 가상 메쏘드로 서브클래스에 선언된 지원 키워드 사전 획득

        Returns:
            서브클래스의 지원 키워드 사전
        """
        pass

    @abc.abstractmethod
    def load(self, file_or_filename: Union[io.TextIOWrapper, io.BytesIO, str]) -> pd.DataFrame:
        """파일에서 데이터를 읽기

        Args:
            file_or_filename (file, str): 파일객체 또는 파일명, 파일객체는 text모드는 TextIOWrapper, binary모드는 BytestIO사용

        Returns:
            dataframe

        """
        pass

    @abc.abstractmethod
    def loads(self, str_or_bytes: Union[str, bytes]) -> pd.DataFrame:
        """문자열이나 binary을 데이터 읽기

        Args:
            str_or_bytes (str, bytes): text모드 string 또는 binary모드 bytes

        Returns:
            dataframe

        """
        pass

    # @abc.abstractmethod
    # def get_tree_path(self, xpath):
    #     pass
    #
    # @abc.abstractmethod
    # def set_tree_path(self, xpath, new_data):
    #     pass

    @abc.abstractmethod
    def dump(self, file_or_filename: Union[io.TextIOWrapper, io.BytesIO, str]) -> None:
        """데이터를 파일로 쓰기

        파일은 text, binary 모드 파일객체이거나 파일명 문자열
        Args:
            file_or_filename (file, str): 파일객체 또는 파일명, 파일객체는 text모드는 TextIOWrapper, binary모드는 BytestIO사용

        Returns:
            없음
        """
        pass

    @abc.abstractmethod
    def dumps(self, mode: Literal['text', 'binary'] = 'text') -> Union[str, bytes]:
        """데이터를 문자열로 출력

        Args:
            mode (): 출력 모드 'text', 'binary' 선택
        Returns:
            데이터를 text모드에서는 문자열, 'binary'모드에서는 bytes로 출력
        """
        pass

