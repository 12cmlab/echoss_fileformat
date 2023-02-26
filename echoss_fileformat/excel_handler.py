import io
import logging
import pandas as pd
import openpyxl   # to_excel() 에서 사용하므로 설치는 되어야함
from typing import Union, Literal

from .csv_handler import CsvHandler

logger = logging.getLogger(__name__)


class ExcelHandler(CsvHandler):
    """Excel file handler

    학습데이터로 Excel 파일은 전체 읽기를 기본으로 해서
    해더와 사용 컬럼 지정을 제공한다
    """
    def __init__(self, processing_type='array', encoding='utf-8', error_log='error.log'):
        """Excel 파일 핸들러 초기화

        Args:
            encoding: 문서 인코딩 'utf-8' 기본값
            error_log: 에러 발생 시에 저장되는 파일 'error.log' 기본값
        """
        super().__init__(processing_type=processing_type, encoding=encoding, error_log=error_log)

    def load(self, file_or_filename: Union[io.TextIOWrapper, io.BytesIO, io.BufferedIOBase, str],
             sheet_name=0, skiprows=0, header=0, nrows=None, usecols=None ):
        """Excel 파일 읽기

        Args:
            file_or_filename (file-like object): file object or file name
            sheet_name: 1개의 sheet 만 지정. 0으로 시작하는 일련 번호 또는 쉬트 이름. None 이면 첫 쉬트 사용
            skiprows (Union[int, list]) : 데이터가 시작되는 row index 또는 배열 지정.
                header 보다 먼저 적용되고, header의 인덱스는 이 처리 결과의 인덱스

            header (Union[int, list]): 헤더로 사용될 1부터 시작되는 row index, 멀티헤더인 경우에는 [1, 2, 3] 형태로 사용
            nrows (int): skiprows 부터 N개의 데이터 row 만 읽을 경우 숫자 지정
            usecols (Union[int, list]): 전체 컬럼 사용시 None, 컬럼 번호나 이름의 리스트 [0, 1, 2] or ['foo', 'bar', 'baz']
        """
        mode = self._check_file_or_filename(file_or_filename)

        try:
            df = pd.read_excel(
                file_or_filename,
                sheet_name=sheet_name,
                header=header,
                skiprows=skiprows,
                nrows=nrows,
                usecols=usecols,
                parse_dates=True,
                engine='openpyxl'
            )

            # delete index column
            df = df.reset_index(drop=True)

            self.pass_list.append(df)
        except Exception as e:
            self.fail_list.append(str(file_or_filename))
            logger.error(f"{file_or_filename} '{mode}' load raise {e}")

    def loads(self, str_or_bytes: Union[str, bytes],
              sheet_name=0, header=0, skiprows=0, nrows=None, usecols=None):
        """문자열이나 bytes 에서 Excel 읽기

        Args:
            str_or_bytes (str, bytes): text 모드 string 또는 binary 모드 bytes
            sheet_name: 1개의 sheet 만 지정. 0으로 시작하는 일련 번호 또는 쉬트 이름. None 이면 첫 쉬트 사용
            header (Union[int, list]): 헤더로 사용될 1부터 시작되는 row index, 멀티헤더인 경우에는 [1, 2, 3] 형태로 사용
            skiprows (Union[int, list]) : 데이터가 시작되는 row index 또는 배열 지정.
                header 보다 먼저 적용되고, header의 인덱스는 이 처리 결과의 인덱스

            nrows (int): skiprows 부터 N개의 데이터 row 만 읽을 경우 숮자 지정
            usecols (Union[int, list]): 전체 컬럼 사용시 None, 컬럼 번호나 이름의 리스트 [0, 1, 2] or ['foo', 'bar', 'baz']
        """
        file_obj = None
        try:
            if isinstance(str_or_bytes, str):
                file_obj = io.StringIO(str_or_bytes)
            elif isinstance(str_or_bytes, bytes):
                file_obj = io.BytesIO(str_or_bytes)
            if file_obj:
                self.load(file_obj,
                          sheet_name=sheet_name, skiprows=skiprows, header=header, nrows=nrows, usecols=usecols)
        except Exception as e:
            self.fail_list.append(str_or_bytes)
            logger.error(f"'{str_or_bytes}' loads raise {e}")

    #
    # def to_pandas() 는 data_list 에 dataframe 을 저장하는 방식이 CsvHandler 와 동일하여 따로 정의하지 않음
    #

    def dump(self, file_or_filename, sheet_name='Sheet1', data: pd.DataFrame = None) -> None:
        """데이터를 Excel 파일로 쓰기

        파일은 text, binary 모드 파일객체이거나 파일명 문자열
        Args:
            file_or_filename (file, str): 파일객체 또는 파일명
            sheet_name: 쉬트 이름.
            data: dataframe 으로 설정시 사용. 기존 유틸리티의 호환성을 위해서 남김
        """
        try:
            if data is None:
                df = self.to_pandas()
            else:
                df = data

            self._check_file_or_filename(file_or_filename)

            # df.to_excel(
            #     file_or_filename,
            #     sheet_name=sheet_name,
            #     index=False,
            #     engine = 'xlsxwriter'
            # )

            # write to Excel file
            with pd.ExcelWriter(file_or_filename) as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=True)
                # df.to_excel(writer, sheet_name=sheet_name, index=False)
        except Exception as e:
            logger.error(f"'{str(file_or_filename)}' dump raise {e}")

    def dumps(self, mode: Literal['text', 'binary'] = 'text', sheet_name='Sheet1',
              data: pd.DataFrame = None) -> Union[str, bytes]:
        """데이터를 CSV 파일로 쓰기

        파일은 text, binary 모드 파일객체이거나 파일명 문자열
        Args:
            mode (str): 출력 모드 'text' 또는 'binary' 선택. 기본 'text' 는 문자열 출력
            sheet_name: 쉬트 이름.
            data: 내장 dataframe 대신 사용할 data. 기존 유틸리티의 호환성을 위해서 남김
        """
        if 'text' == mode:
            file_obj = io.StringIO()
        elif 'binary' == mode:
            file_obj = io.BytesIO()

        try:
            self.dump(file_obj, sheet_name=sheet_name, data=data)
        except Exception as e:
            logger.error(f"mode='{mode}' dumps raise {e}")

        return file_obj.getvalue()
