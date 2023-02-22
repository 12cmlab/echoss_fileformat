from .fileformat_base import FileformatBase
import io
import logging
import pandas as pd
from typing import Union, Dict, Literal

logger = logging.getLogger(__name__)


class CsvHandler(FileformatBase):
    """CSV file handler

    학습데이터로 CSV 파일은 전체 읽기를 기본으로 해서
    해더와 사용 컬럼 지정을 제공한다
    """

    def __init__(self, delimiter=',', quotechar='"', escapechar='\\', encoding='utf-8', error_log='error.log'):
        """CSV 파일 핸들러 초기화 메쏘드

        Args:
            encoding: 파일 인코딩.
            delimiter: 컬럼 구분자
            quotechar: 인용 문자
            escapechar: 예외처리 문자
        """
        super().__init__(encoding=encoding, error_log=error_log)
        self.encoding = encoding
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.escapechar = escapechar

    def load(self, file_or_filename: Union[io.TextIOWrapper, io.BytesIO, io.BufferedIOBase, str],
             header=0, skiprows=0, nrows=None, usecols=None):
        """CSV 파일 읽기

        Args:
            file_or_filename (file-like object): file object or file name
            header (Union[int, list]): 헤더로 사용될 1부터 시작되는 row index, 멀티헤더인 경우에는 [1, 2, 3] 형태로 사용
            skiprows (int) : 데이터가 시작되는 row index 지정
            nrows (int): skiprows 부터 N개의 데이터 row 만 읽을 경우 숮자 지정
            usecols (Union[int, list]): 전체 컬럼 사용시 None, 컬럼 번호나 이름의 리스트 [0, 1, 2] or ['foo', 'bar', 'baz']
        """
        try:
            self._check_file_or_filename(file_or_filename)

            df = pd.read_csv(
                file_or_filename,
                encoding=self.encoding,
                sep=self.delimiter,
                quotechar=self.quotechar,
                escapechar=self.escapechar,
                header=header,
                skiprows=skiprows,
                nrows=nrows,
                usecols=usecols,
                infer_datetime_format=True,
                on_bad_lines='warn'
            )

            self.pass_list.append(df)
        except Exception as e:
            self.fail_list.append(str(file_or_filename))
            logger.error(f"{file_or_filename} load raise {e}")

    def loads(self, str_or_bytes: Union[str, bytes], header=0, skiprows=0, nrows=None, usecols=None):
        """문자열이나 bytes 에서 CSV 읽기

        Args:
            str_or_bytes (str, bytes): text 모드 string 또는 binary 모드 bytes
            header (Union[int, list]): 헤더로 사용될 1부터 시작되는 row index, 멀티헤더인 경우에는 [1, 2, 3] 형태로 사용
            skiprows (int) : 데이터가 시작되는 row index 지정
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
                self.load(file_obj, header=header, skiprows=skiprows, nrows=nrows, usecols=usecols)
        except Exception as e:
            self.fail_list.append(str_or_bytes)
            logger.error(f"'{str_or_bytes}' loads raise {e}")

    def to_pandas(self) -> pd.DataFrame:
        """클래스 내부메쏘드 CSV 파일 처리 결과를 pd.DataFrame 형태로 pass_list 에 저장

        내부적으로 추가할 데이터(pass_list)가 있으면 모두 통합하여 새로운 dataframe 을 생성함
        실패 목록(fail_list)가 있으면 파일로 저장
        학습을 위한 dataframe 이기 떄문에 dot('.') 문자로 normalize 된 flatten 컬럼과 값을 가진다.

        Returns: pandas DataFrame
        """
        if len(self.pass_list) > 0:
            try:
                if len(self.data_df) > 0:
                    df_list = [self.data_df].extend(self.pass_list)
                else:
                    df_list = self.pass_list
                merge_df = pd.concat(df_list, ignore_index=True)
                self.data_df = merge_df
            except Exception as e:
                logger.error(f"pass_list[{len(self.pass_list)}] _to_pandas raise {e}")
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
                        if not isinstance(fail, str):
                            fail_str = str(fail)
                        else:
                            fail_str = fail
                        fail_bytes = fail_str.encode(self.encoding)
                        error_fp.write(fail_bytes)
                        error_fp.write(b'\n')
                    except Exception as e:
                        logger.exception(e)
                error_fp.close()
                self.fail_list.clear()
        return self.data_df

    def dump(self, file_or_filename, quoting=0, data: pd.DataFrame = None):
        """데이터를 CSV 파일로 쓰기

        파일은 text, binary 모드 파일객체이거나 파일명 문자열
        Args:
            file_or_filename (file, str): 파일객체 또는 파일명

            quoting (int): 인용문자 사용 빈도에 대한 정책 0: QUOTE_MINIMAL, 1: QUOTE_ALL, 2: QUOTE_NONNUMERIC, 3: QUOTE_NONE

            data: dataframe 으로 설정시 사용. 기존 유틸리티의 호환성을 위해서 남김
        """
        if not data:
            df = self.to_pandas()
        else:
            df = data

        df.to_csv(
            file_or_filename,
            encoding=self.encoding,
            sep=self.delimiter,
            quotechar=self.quotechar,
            escapechar=self.escapechar,
            quoting=quoting,
            index=False
        )

    def dumps(self, mode: Literal['text','binary'] = 'text', quoting=0, data: pd.DataFrame = None) -> Union[str, bytes]:
        """데이터를 CSV 파일로 쓰기

        파일은 text, binary 모드 파일객체이거나 파일명 문자열
        Args:
            mode (str): 출력 모드 'text' 또는 'binary' 선택
            quoting (int): 인용문자 사용 빈도에 대한 정책 0: QUOTE_MINIMAL, 1: QUOTE_ALL, 2: QUOTE_NONNUMERIC, 3: QUOTE_NONE
            data: use this data instead of self.data if provide 기능 확장성과 호환성을 위해서 남김
        Returns:
            없음
        """
        if 'text' == mode:
            file_obj = io.StringIO()
        elif 'binary' == mode:
            file_obj = io.BytesIO()

        try:
            self.dump(file_obj, quoting=quoting, data=data)
        except Exception as e:
            logger.error(f"mode='{mode}' '{quoting}' dumps raise {e}")
        return file_obj.getvalue()

    """
    클래스 내부 메쏘드   
    """

    def _check_file_or_filename(self, file_or_filename):
        """파일 변수의 유형 체크
        Args:
            file_or_filename: file object or file name

        Returns: (fp, mode)
            fp : file obj if exist, or None
            mode : Union['binary', 'text', 'str']
        """
        fp = None
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
            mode = 'str'
        # elif isinstance(file_or_filename, Path)  # 향후 pathlib.Path 객체 사용 검토
        else:
            raise TypeError(f"{file_or_filename} is not file-like obj")
        return mode


