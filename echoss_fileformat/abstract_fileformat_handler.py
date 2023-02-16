import abc
import io
from typing import Union, Dict, Literal
import pandas as pd


class AbstractFileFormatHandler(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def __get_kw_dict(self) -> dict:
        """내부 가상 메쏘드로 서브클래스에 선언된 지원 키워드 사전 획득

        Returns:
            서브클래스의 지원 키워드 사전
        """
        pass

    def __make_kw_dict(self, kw_name: str, kw_dict: dict) -> dict:
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
        handler_kw_dict = self.__get_kw_dict()
        if kw_name in handler_kw_dict:
            copy_dict = handler_kw_dict[kw_name].copy()
            for k in kw_dict:
                if k in copy_dict:
                    copy_dict[k] = kw_dict[k]
            return copy_dict
        else:
            return {}

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

