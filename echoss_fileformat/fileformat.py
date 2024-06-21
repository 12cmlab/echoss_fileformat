"""
    echoss AI Bigdata Center Solution - file format utilty (static version)
"""
import os

import pandas as pd
import wcwidth

from echoss_fileformat.csv_handler import CsvHandler
from echoss_fileformat.echoss_logger import get_logger
from echoss_fileformat.excel_handler import ExcelHandler
from echoss_fileformat.feather_handler import FeatherHandler
from echoss_fileformat.json_handler import JsonHandler
from echoss_fileformat.xml_handler import XmlHandler

logger = get_logger("echoss_fileformat")
EMPTY_DATAFRAME = pd.DataFrame()


class FileUtil:
    """AI 학습을 위한 파일 포맷 지원 static 클래스

    JSON, CSV, XML and excel file format handler static 클래스

    use load()/dump() to  read/write file format as pandas Dataframe
    - use ".json" extension for normal json object, use ".jsonl" for json line format
    use laods()/dumps() to read/write file format as String : deprecated
    if want to explict file format use appendix _csv/_json/_jsonl/_xls/_xlsx ...

    특정 키만 학습데이터로 사용할 경우에는 자식클래스 구현마다 다름. data_key 또는 usecols 사용
    """
    WORKING_HANDLER = None

    def __init__(self):
        pass

    def __str__(self):
        return f"('handler': {FileUtil.WORKING_HANDLER})"


    @staticmethod
    def load(file_path: str, **kwargs) -> pd.DataFrame:
        """파일에서 데이터를 읽기

        파일 처리 결과는 객체 내부에 성공 목록과 실패 목록으로 저장됨

        Args:
            file_path (str): 파일명
            kwargs : option key value args

        """
        _, ext = os.path.splitext(file_path)
        ext = ext.lower()

        if ".csv" == ext:
            return FileUtil.read_csv(file_path, **kwargs)
        elif ".xls" == ext:
            return FileUtil.read_excel(file_path, **kwargs)
        elif ".xlsx" == ext:
            return FileUtil.read_excel(file_path, **kwargs)
        elif ".json" == ext:
            return FileUtil.read_json(file_path, **kwargs)
        elif ".jsonl" == ext:
            return FileUtil.read_jsonl(file_path, **kwargs)
        elif ".xml" == ext:
            return FileUtil.read_xml(file_path, **kwargs)
        else:
            logger.error("File format {ext} is not supported")
            return EMPTY_DATAFRAME

    @staticmethod
    def read_csv(file_path: str, **kwargs) -> pd.DataFrame:
        processing_type = kwargs.pop('processing_type', 'object')
        handler = CsvHandler(processing_type=processing_type)
        df = handler.load(file_path, **kwargs)
        return df

    @staticmethod
    def read_excel(file_path: str, **kwargs) -> pd.DataFrame:
        processing_type = kwargs.pop('processing_type', 'object')
        handler = ExcelHandler(processing_type=processing_type)
        handler.load(file_path, **kwargs)
        df = handler.to_pandas()
        return df

    @staticmethod
    def read_feather(file_path: str, **kwargs) -> pd.DataFrame:
        processing_type = kwargs.pop('processing_type', 'object')
        handler = FeatherHandler(processing_type=processing_type)
        handler.load(file_path, **kwargs)
        df = handler.to_pandas()
        return df

    @staticmethod
    def read_json(file_path: str, **kwargs) -> pd.DataFrame:
        processing_type = kwargs.pop('processing_type', 'object')
        handler = JsonHandler(processing_type=processing_type)
        handler.load(file_path, **kwargs)
        df = handler.to_pandas()
        return df

    @staticmethod
    def read_jsonl(file_path: str, **kwargs) -> pd.DataFrame:
        processing_type = kwargs.pop('processing_type', 'multiline')
        handler = JsonHandler(processing_type='multiline')
        handler.load(file_path, **kwargs)
        df = handler.to_pandas()
        return df

    @staticmethod
    def read_xml(file_path: str, **kwargs) -> pd.DataFrame:
        processing_type = kwargs.pop('processing_type', 'object')
        handler = XmlHandler(processing_type=processing_type)
        handler.load(file_path, **kwargs)
        df = handler.to_pandas()
        return df

    @staticmethod
    def dump(df: pd.DataFrame, file_path: str, **kwargs) -> None:
        """데이터를 파일로 쓰기

        파일은 text, binary 모드 파일객체이거나 파일명 문자열
        Args:
            df (DataFrame) : write dataframe
            file_path (str): 파일명

        Returns:
            없음
        """
        _, ext = os.path.splitext(file_path)
        ext = ext.lower()

        if ".csv" == ext:
            FileUtil.to_csv(df, file_path, **kwargs)
        elif ".xls" == ext:
            FileUtil.to_excel(df, file_path, **kwargs)
        elif ".xlsx" == ext:
            FileUtil.to_excel(df, file_path, **kwargs)
        elif ".json" == ext:
            FileUtil.to_json(df, file_path, **kwargs)
        elif ".jsonl" == ext:
            FileUtil.to_jsonl(df, file_path, **kwargs)
        elif ".xml" == ext:
            FileUtil.to_xml(df, file_path, **kwargs)
        else:
            logger.error("File format {ext} is not supported")

    @staticmethod
    def to_csv(df: pd.DataFrame, file_path: str, **kwargs) -> None:
        processing_type = kwargs.pop('processing_type', 'object')
        handler = CsvHandler(processing_type=processing_type)
        handler.dump(file_path, data=df, **kwargs)

    @staticmethod
    def to_excel(df: pd.DataFrame, file_path: str, **kwargs) -> None:
        processing_type = kwargs.pop('processing_type', 'object')
        handler = ExcelHandler(processing_type=processing_type)
        handler.dump(file_path, data=df, **kwargs)

    @staticmethod
    def to_feather(df: pd.DataFrame, file_path: str, **kwargs) -> None:
        processing_type = kwargs.pop('processing_type', 'object')
        handler = FeatherHandler(processing_type=processing_type)
        handler.dump(file_path, data=df, **kwargs)

    @staticmethod
    def to_json(df: pd.DataFrame, file_path: str, **kwargs) -> None:
        processing_type = kwargs.pop('processing_type', 'object')
        handler = JsonHandler(processing_type=processing_type)
        handler.dump(file_path, data=df, **kwargs)

    @staticmethod
    def to_jsonl(df: pd.DataFrame, file_path: str, **kwargs) -> None:
        processing_type = kwargs.pop('processing_type', 'multiline')
        handler = JsonHandler(processing_type='multiline')
        handler.dump(file_path, data=df, **kwargs)

    @staticmethod
    def to_xml(df: pd.DataFrame, file_path: str, **kwargs) -> None:
        processing_type = kwargs.pop('processing_type', 'object')
        handler = XmlHandler(processing_type=processing_type)
        handler.dump(file_path, data=df, **kwargs)


class PandasUtil:
    """ pandas dataframe utility
    """
    vert_marker = '|'
    horiz_marker = '-'
    corner_marker = '+'

    @staticmethod
    def set_markers(vertical='|', horizontal='-', corner='+'):
        PandasUtil.vert_marker = vertical
        PandasUtil.horiz_marker = horizontal
        PandasUtil.corner_marker = corner

    @staticmethod
    def calculate_display_width(text):
        return sum(wcwidth.wcwidth(char) for char in text)

    @staticmethod
    def adjust_width(s, width):
        display_width = PandasUtil.calculate_display_width(s)
        if display_width <= width:
            return s + ' ' * (width - display_width)
        return s[:width-3] + '...'

    @staticmethod
    def split_columns(df, max_cols):
        if len(df.columns) > max_cols:
            part1 = df.iloc[:, :max_cols // 2]
            part2 = df.iloc[:, -max_cols // 2:]
            ellipsis = pd.DataFrame({i: ['...'] * len(df) for i in range(1)}, columns=['...'])
            df = pd.concat([part1, ellipsis, part2], axis=1)
        return df

    @staticmethod
    def split_rows(df, max_rows):
        if len(df) > max_rows:
            top_part = df.iloc[:max_rows // 2]
            bottom_part = df.iloc[-max_rows // 2:]
            middle_row = pd.DataFrame({col: ['...'] for col in df.columns})
            middle_row.columns = df.columns
            df = pd.concat([top_part, middle_row, bottom_part])
        return df

    @staticmethod
    def to_table(df: pd.DataFrame, index=False, max_cols=20, max_rows=10, col_space=16, max_colwidth=24):
        df = PandasUtil.split_rows(df, max_rows)
        df = PandasUtil.split_columns(df, max_cols)

        # Calculate the widths for each column
        col_widths = {}
        for col in df.columns:
            max_data_width = df[col].astype(str).apply(PandasUtil.calculate_display_width).max()
            col_widths[col] = min(max(max_data_width, PandasUtil.calculate_display_width(col)), max_colwidth)
            col_widths[col] = max(col_widths[col], col_space)

        # Create the table header
        header = [PandasUtil.adjust_width(col, col_widths[col]) for col in df.columns]
        header_line = f' {PandasUtil.vert_marker} '.join(header)
        border_line = PandasUtil.corner_marker + PandasUtil.corner_marker.join([PandasUtil.horiz_marker * (col_widths[col] + 2) for col in df.columns]) + PandasUtil.corner_marker

        # Create the formatted table
        lines = [border_line, f'{PandasUtil.vert_marker} ' + header_line + f' {PandasUtil.vert_marker}', border_line]

        # Create table rows
        for i in range(len(df)):
            row = [PandasUtil.adjust_width(str(df.iloc[i, j]), col_widths[df.columns[j]]) for j in range(len(df.columns))]
            row_line = f' {PandasUtil.vert_marker} '.join(row)
            lines.append(f'{PandasUtil.vert_marker} ' + row_line + f' {PandasUtil.vert_marker}')
            lines.append(border_line)

        return '\n' + '\n'.join(lines) + '\n'

