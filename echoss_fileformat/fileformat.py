"""
    echoss AI Bigdata Center Solution - file format utilty (static version)
"""
import io
import os
import pandas as pd
from typing import Union, Literal, Optional, Dict, Any
import wcwidth
import yaml
import json
import configparser

from echoss_fileformat.csv_handler import CsvHandler
from echoss_fileformat.echoss_logger import get_logger
from echoss_fileformat.excel_handler import ExcelHandler
from echoss_fileformat.feather_handler import FeatherHandler
from echoss_fileformat.json_handler import JsonHandler
from echoss_fileformat.xml_handler import XmlHandler

logger = get_logger("echoss_fileformat")
EMPTY_DATAFRAME = pd.DataFrame()
EMPTY_DICT = dict()

class FileUtil:
    """AI 학습을 위한 파일 포맷 지원 static 클래스

    JSON, CSV, XML and excel file format handler static 클래스

    For data read/write:
    use load()/dump() to  read/write file format as pandas Dataframe
    - use ".json" extension for normal json object, use ".jsonl" for json line format
    if file extension is not normal, use load_csv/load_json/load_jsonl/load_xls/load_xlsx ...
    특정 키만 학습데이터로 사용할 경우에는 data_key 또는 usecols kwargs 사용

    For config read/write:
    use load_config()/dump_config() to read/write config dictionary yaml, json, properties file
    read ".yaml", ".json" or any properties text file and return a dictionary

    For dataframe print:
    to_table() : dataframe to table like string for logging or print
    """

    def __init__(self):
        pass

    @staticmethod
    def supported_file_formats():
        return ["csv", "tsv", "xlsx", "xls", "json"]

    @staticmethod
    def load(file_path: str, file_format=None, **kwargs) -> pd.DataFrame:
        """파일에서 데이터 읽기

        Args:
            file_path (str): 파일명
            file_format (str): explict file format name if is not None
            kwargs : option key value args

        """
        if file_format is None:
            _, ext = os.path.splitext(file_path)
            file_format = ext[1:].lower() if ext else None

        if "csv" == file_format:
            return FileUtil.load_csv(file_path, **kwargs)
        elif "tsv" == file_format:
            return FileUtil.load_tsv(file_path, **kwargs)
        elif "xls" == file_format:
            return FileUtil.load_xls(file_path, **kwargs)
        elif "xlsx" == file_format:
            return FileUtil.load_xlsx(file_path, **kwargs)
        elif "json" == file_format:
            return FileUtil.load_json(file_path, **kwargs)
        elif "jsonl" == file_format:
            return FileUtil.load_jsonl(file_path, **kwargs)
        elif "xml" == file_format:
            return FileUtil.load_xml(file_path, **kwargs)
        elif "parquet" == file_format:
            return pd.read_parquet(file_path, **kwargs)
        elif "feather" == file_format:
            return pd.read_feather(file_path, **kwargs)
        else:
            logger.error(f"File {file_path} format {file_format} is not supported")
            return EMPTY_DATAFRAME

    @staticmethod
    def load_csv(file_or_filename, **kwargs) -> pd.DataFrame:
        handler = FileUtil._init_csv_handler(kwargs)
        df = handler.load(file_or_filename, **kwargs)
        return df

    @staticmethod
    def _init_csv_handler(kwargs):
        # processing_type='array', encoding='utf-8',
        # delimiter=',', quotechar='"', quoting=0, escapechar='\\'
        processing_type = kwargs.pop('processing_type', 'object')
        encoding = kwargs.pop('encoding', 'utf-8')
        delimiter = kwargs.pop('delimiter', ',')
        quotechar = kwargs.pop('quotechar', '"')
        quoting = kwargs.pop('quoting', 0)
        escapechar = kwargs.pop('escapechar', '\\')
        handler = CsvHandler(
            processing_type=processing_type,
            encoding=encoding,
            delimiter=delimiter,
            quotechar=quotechar,
            quoting=quoting,
            escapechar=escapechar
        )
        return handler

    @staticmethod
    def load_tsv(file_or_filename, **kwargs) -> pd.DataFrame:
        kwargs['delimiter'] = '\t'
        handler = FileUtil._init_csv_handler(kwargs)
        df = handler.load(file_or_filename, **kwargs)
        return df

    @staticmethod
    def load_xlsx(file_path: str, **kwargs) -> pd.DataFrame:
        handler = FileUtil._init_excelhandler(kwargs)
        new_engine = 'openpyxl'
        df = handler.load(file_path, engine=new_engine, **kwargs)
        return df

    @staticmethod
    def _init_excelhandler(kwargs):
        # processing_type: str = 'array', encoding='utf-8',
        processing_type = kwargs.pop('processing_type', 'object')
        encoding = kwargs.pop('encoding', 'utf-8')
        handler = ExcelHandler(
            processing_type=processing_type,
            encoding=encoding
        )
        kwargs.pop('engine', 'openpyxl')
        return handler

    @staticmethod
    def load_xls(file_path: str, **kwargs) -> pd.DataFrame:
        handler = FileUtil._init_excelhandler(kwargs)
        old_engine = 'xlrd'
        df = handler.load(file_path, engine=old_engine, **kwargs)
        return df

    @staticmethod
    def load_feather(file_path: str, **kwargs) -> pd.DataFrame:
        handler = FileUtil._init_featherhandler(kwargs)
        handler.load(file_path, **kwargs)
        df = handler.to_pandas()
        return df

    @staticmethod
    def _init_featherhandler(kwargs):
        # processing_type: str = 'array', encoding='utf-8',
        processing_type = kwargs.pop('processing_type', 'object')
        encoding = kwargs.pop('encoding', 'utf-8')
        handler = FeatherHandler(
            processing_type=processing_type,
            encoding=encoding
        )
        return handler

    @staticmethod
    def load_json(file_path: str, **kwargs) -> pd.DataFrame:
        handler = FileUtil._init_jsonhandler(kwargs)
        handler.load(file_path, **kwargs)
        df = handler.to_pandas()
        return df

    @staticmethod
    def _init_jsonhandler(kwargs):
        # processing_type: str = 'array', encoding='utf-8',
        processing_type = kwargs.pop('processing_type', 'object')
        encoding = kwargs.pop('encoding', 'utf-8')
        handler = JsonHandler(
            processing_type=processing_type,
            encoding=encoding
        )
        return handler

    @staticmethod
    def load_jsonl(file_path: str, **kwargs) -> pd.DataFrame:
        kwargs['processing_type'] = 'multiline'
        handler = FileUtil._init_jsonhandler(kwargs)
        handler.load(file_path, **kwargs)
        df = handler.to_pandas()
        return df

    @staticmethod
    def load_xml(file_path: str, **kwargs) -> pd.DataFrame:
        handler = FileUtil._init_xmlhandler(kwargs)
        handler.load(file_path, **kwargs)
        df = handler.to_pandas()
        return df

    @staticmethod
    def _init_xmlhandler(kwargs):
        # processing_type: str = 'array', encoding='utf-8',
        processing_type = kwargs.pop('processing_type', 'object')
        encoding = kwargs.pop('encoding', 'utf-8')
        handler = XmlHandler(
            processing_type=processing_type,
            encoding=encoding
        )
        return handler


    """
    dump dataframe to file format
    """
    @staticmethod
    def dump(df: pd.DataFrame, file_path: str, file_format=None, force_write=False, **kwargs) -> None:
        """데이터를 파일로 쓰기

        파일은 text, binary 모드 파일객체이거나 파일명 문자열
        Args:
            df (DataFrame) : write dataframe
            file_path (str): 파일명
            file_format (str): file extension name if you want explict format
            force_write (bool): overwrite exist file ?

        Returns:
            없음
        """
        if file_format is None:
            _, ext = os.path.splitext(file_path)
            file_format = ext[1:].lower() if ext else None

        if os.path.exists(file_path) and force_write is False:
            logger.error(f"Can not overwrite exist config file [{file_path}] use force_write=True if need")
            return

        if "csv" == file_format:
            FileUtil.dump_csv(df, file_path, **kwargs)
        elif "csv" == file_format:
            FileUtil.dump_tsv(df, file_path, **kwargs)
        elif "xls" == file_format:
            FileUtil.dump_xls(df, file_path, **kwargs)
        elif "xlsx" == file_format:
            FileUtil.dump_xlsx(df, file_path, **kwargs)
        elif "json" == file_format:
            FileUtil.dump_json(df, file_path, **kwargs)
        elif "jsonl" == file_format:
            FileUtil.dump_jsonl(df, file_path, **kwargs)
        elif "xml" == file_format:
            FileUtil.dump_xml(df, file_path, **kwargs)
        elif "parquet" == file_format:
            return df.to_parquet(file_path, **kwargs)
        elif "feather" == file_format:
            return df.to_feather(file_path, **kwargs)
        else:
            logger.error(f"File {file_path} format {file_format} is not supported")

    @staticmethod
    def dump_csv(df: pd.DataFrame, file_or_filename, **kwargs) -> None:
        handler = FileUtil._init_csv_handler(kwargs)
        handler.dump(file_or_filename, data=df, **kwargs)

    @staticmethod
    def dump_tsv(df: pd.DataFrame, file_or_filename, **kwargs) -> None:
        kwargs['delimiter'] = '\t'
        handler = FileUtil._init_csv_handler(kwargs)
        handler.dump(file_or_filename, data=df, **kwargs)

    @staticmethod
    def dump_xls(df: pd.DataFrame, file_or_filename, **kwargs) -> None:
        handler = FileUtil._init_excelhandler(kwargs)
        old_engine = 'xlrd'
        handler.dump(file_or_filename, data=df, engine=old_engine, **kwargs)

    @staticmethod
    def dump_xlsx(df: pd.DataFrame, file_or_filename, **kwargs) -> None:
        handler = FileUtil._init_excelhandler(kwargs)
        new_engine = 'openpyxl'
        handler.dump(file_or_filename, data=df, engine=new_engine, **kwargs)

    @staticmethod
    def dump_feather(df: pd.DataFrame, file_or_filename, **kwargs) -> None:
        handler = FileUtil._init_featherhandler(kwargs)
        handler.dump(file_or_filename, data=df, **kwargs)

    @staticmethod
    def dump_json(df: pd.DataFrame, file_or_filename, **kwargs) -> None:
        kwargs['processing_type'] = 'object'
        handler = FileUtil._init_jsonhandler(kwargs)
        handler.dump(file_or_filename, data=df, **kwargs)

    @staticmethod
    def dump_jsonl(df: pd.DataFrame, file_or_filename, **kwargs) -> None:
        kwargs['processing_type'] = 'multiline'
        handler = FileUtil._init_jsonhandler(kwargs)
        handler.dump(file_or_filename, data=df, **kwargs)

    @staticmethod
    def dump_xml(df: pd.DataFrame, file_or_filename, **kwargs) -> None:
        processing_type = kwargs.pop('processing_type', 'object')
        handler = FileUtil._init_xmlhandler(kwargs)
        handler.dump(file_or_filename, data=df, **kwargs)

    """
    load/dump config file 
    """
    @staticmethod
    def dict_load(file_path: str, file_format: str = None, **kwargs) -> dict:
        """config file read to dict

        Args:
            file_path (str): 파일명
            file_format : file extension name to read
            kwargs : keyword arguments
        Return:
            dict
        """
        if file_format is None:
            _, ext = os.path.splitext(file_path)
            file_format = ext[1:].lower() if ext else None
        if not os.path.exists(file_path):
            logger.error(f"load config file [{file_path}] is not exist")
            return EMPTY_DICT

        if "yaml" == file_format or "yml" == file_format :
            with open(file_path, 'r') as f:
                yaml_dict = yaml.safe_load(f)
                return yaml_dict
        elif "json" == file_format:
            handler = FileUtil._init_jsonhandler(kwargs)
            json_dict = handler.load(file_path)
            return json_dict
        elif "xml" == file_format:
            handler = FileUtil._init_xmlhandler(kwargs)
            root = handler.load(file_path)
            xml_dict = handler.xml_to_dict(root)
            return xml_dict
        elif 'properties' == file_format:
            config = configparser.ConfigParser()
            properties_dict = {}
            with open(file_path, 'r') as f:
                file_content = f.read()
                if not file_content.strip().startswith('['):
                    file_content = '[DEFAULT]\n' + file_content
                config.read_string(file_content)

            if config.sections():  # Check if there are any sections
                for section in config.sections():
                    properties_dict[section] = dict(config.items(section))
            else:
                properties_dict = dict(config.items(config.default_section))

            return properties_dict
        else:
            logger.error(f"Unsupported file format [{file_format}]")
            return EMPTY_DICT

    @staticmethod
    def dict_dump(config: dict, file_path: str, file_format=None, force_write=True, xml_tag=None, **kwargs):
        """config dict write to file

        Args:
            config (dict): config dictionary
            file_path (str): 파일명
            file_format : file extension name to use
            force_write : if True overwrite exist file or just return
            xml_tag (str) : root tag to wrapping dictionary
            kwargs : keyword arguments
        """
        if file_format is None:
            _, ext = os.path.splitext(file_path)
            file_format = ext[1:].lower() if ext else None

        if os.path.exists(file_path) and force_write is False:
            logger.error(f"Can not overwrite exist config file [{file_path}] use force_write=True if need")
            return

        if "yaml" == file_format or "yml" == file_format :
            with open(file_path, 'w') as f:
                yaml.dump(config, f, default_style=False, allow_unicode=True)
        elif "json" == file_format:
            with open(file_path, 'w') as f:
                indent = kwargs.pop('indent', 4)
                json.dump(config, f, indent=indent, **kwargs)
        elif "xml" == file_format:
            handler = FileUtil._init_xmlhandler(kwargs)
            handler.dict_dump(config, xml_tag, file_path)
        elif 'properties' == file_format:
            parser = configparser.ConfigParser()
            have_section = any(isinstance(v, dict) for v in config.values())
            if have_section:
                for section, params in config.items():
                    if isinstance(params, dict):
                        parser.add_section(section)
                        for k, v in params.items():
                            parser.set(section, k, str(v))
                    else:
                        parser.set('DEFAULT', section, str(params))
            else:
                # parser.add_section('DEFAULT')
                for k, v in config.items():
                    parser.set('DEFAULT', k, str(v))
            with open(file_path, 'w') as f:
                if have_section:
                    parser.write(f)
                else:
                    with io.StringIO() as string_file:
                        parser.write(string_file)
                        clean_text = string_file.getvalue().replace("[DEFAULT]\n", "")
                        f.write(clean_text)
        else:
            logger.error(f"Unsupported file format [{file_format}]")

    """
    Print pandas string table format
    """

    v_marker = '|'
    h_marker = '-'
    c_marker = '+'
    use_row_line = True

    @staticmethod
    def to_table(df: pd.DataFrame, index=True, max_cols=16, max_rows=10, col_space=4, max_colwidth=24):
        if index:
            df = df.reset_index()
        df = FileUtil._split_rows(df, max_rows)
        df = FileUtil._split_columns(df, max_cols)
        df = FileUtil._preprocess_dataframe(df)

        # Calculate the widths for each column
        col_widths = {}
        for col in df.columns:
            max_data_width = df[col].astype(str).apply(wcwidth.wcswidth).max()
            head_width = wcwidth.wcswidth(str(col))
            col_widths[col] = min(max(max_data_width, head_width), max_colwidth)
            col_widths[col] = max(col_widths[col], col_space)

        # Create the table header
        header = [FileUtil._adjust_width(str(col), col_widths[col]) for col in df.columns]
        header_line = f' {FileUtil.v_marker} '.join(header)
        border_line = FileUtil.c_marker + FileUtil.c_marker.join(
            [FileUtil.h_marker * (col_widths[col] + 2) for col in df.columns]) + FileUtil.c_marker

        # Create the formatted table
        lines = [border_line, f'{FileUtil.v_marker} ' + header_line + f' {FileUtil.v_marker}', border_line]

        # Create table rows
        for i in range(len(df)):
            row = [FileUtil._adjust_width(str(df.iloc[i, j]), col_widths[df.columns[j]]) for j in
                   range(len(df.columns))]
            row_line = f' {FileUtil.v_marker} '.join(row)
            if FileUtil.use_row_line:
                lines.append(f'{FileUtil.v_marker} ' + row_line + f' {FileUtil.v_marker}')
            lines.append(border_line)

        return '\n' + '\n'.join(lines) + '\n'

    @staticmethod
    def set_markers(vertical='|', horizontal='-', corner='+', use_row_line=True):
        FileUtil.v_marker = vertical
        FileUtil.h_marker = horizontal
        FileUtil.c_marker = corner
        FileUtil.use_row_line = use_row_line

    @staticmethod
    def _adjust_width(s: str, width: int):
        try:
            display_width = wcwidth.wcswidth(s)
            if display_width <= width:
                return s + ' ' * (width - display_width)

            for i in range(width//2-3, len(s)):
                current_width = wcwidth.wcswidth(s[:i])
                if current_width > width - 3:
                    remaining_width = width - current_width
                    return s[:i] + '.' * remaining_width
        except Exception as e:
            logger.error(f"Exception adjust_width({s=}, {width=})")

        return s[:width - 3] + '...'

    @staticmethod
    def _preprocess_dataframe(df):
        # 특수 문자를 \\n 등으로 변환
        def clean_text(text):
            if isinstance(text, str):
                text = text.replace('\r\n', '\\n').replace('\r', '\\n').replace('\n', '\\n')
                text = text.replace('\t', '\\t').replace('\x0b', ' ').replace('\x0c', ' ')
                text = text.replace('\u200b', '')
            return text
        df = df.map(clean_text)
        return df

    @staticmethod
    def _split_columns(df, max_cols):
        if len(df.columns) > max_cols:
            part1 = df.iloc[:, :max_cols // 2]
            part2 = df.iloc[:, -max_cols // 2:]
            mid = pd.DataFrame({'...': ['...'] * len(df)}, index=df.index)
            df = pd.concat([part1, mid, part2], axis=1)
        return df

    @staticmethod
    def _split_rows(df, max_rows):
        if len(df) > max_rows:
            half_max_rows = max_rows // 2
            top_part = df.iloc[:half_max_rows]
            bottom_part = df.iloc[-half_max_rows:]
            middle_row = pd.DataFrame([['...'] * len(df.columns)], columns=df.columns, index=[half_max_rows])
            df = pd.concat([top_part, middle_row, bottom_part])
        return df



