import unittest
import time
import logging
import os
import sys
import pandas as pd
import xlsxwriter

from echoss_fileformat.excel_handler import ExcelHandler


# Define custom formatter with different colors for each log level
class ColoredFormatter(logging.Formatter):
    grey = "\x1b[38;21m"
    yellow = "\x1b[33;21m"
    red = "\x1b[31;21m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    FORMATS = {
        logging.DEBUG: grey + "%(asctime)s %(name)s %(levelname)s - %(message)s" + reset,
        logging.INFO: yellow + "%(asctime)s %(name)s %(levelname)s - %(message)s" + reset,
        logging.WARNING: yellow + "%(asctime)s %(name)s %(levelname)s - %(message)s" + reset,
        logging.ERROR: red + "%(asctime)s %(name)s %(levelname)s - %(message)s" + reset,
        logging.CRITICAL: bold_red + "%(asctime)s %(name)s %(levelname)s - %(message)s" + reset
    }

    def format(self, record):
        log_format = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_format)
        return formatter.format(record)


# configure the logger
# LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s - %(message)s"
# logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
log_handler = logging.StreamHandler(sys.stdout)
log_handler.setFormatter(ColoredFormatter())
logger.addHandler(log_handler)


verbose = False


def table_to_string(df: pd.DataFrame):
    """

    Args:
        df:

    Returns:

    """
    if not verbose:
        return ""
    return df.to_string(index=True, index_names=True, max_cols=20, max_rows=5, justify='left', show_dimensions=True,
                        col_space=16, max_colwidth=24)


class MyTestCase(unittest.TestCase):
    """
        테스트 설정
    """
    def setUp(self):
        """Before test"""
        ids = self.id().split('.')
        self.str_id = f"{ids[-2]}: {ids[-1]}"
        self.start_time = time.perf_counter()
        logger.info(f"setting up test [{self.str_id}] ")

    def tearDown(self):
        """After test"""
        self.end_time = time.perf_counter()
        logger.info(f" tear down test [{self.str_id}] elapsed time {(self.end_time-self.start_time)*1000: .3f}ms \n")

    """
    유닛 테스트 
    """

    def test_basic_excel(self):
        expect_pass = 1
        expect_fail = 0
        load_filename = 'test_data/simple_table.xlsx'
        expect_len = 100
        dump_filename = 'test_data/simple_table_to_delete.xlsx'
        try:
            handler = ExcelHandler()
            handler.load(load_filename)
            pass_size = len(handler.pass_list)
            fail_size = len(handler.fail_list)
            df = handler.to_pandas()
            if df is not None:
                logger.info('\n'+table_to_string(df))
                load_columns = list(df.columns)
                load_len = len(df)
                logger.info(f"expect dataframe len={expect_len} and get {len(df)}")
                self.assertEqual(load_len, expect_len)
            else:
                logger.info('empty dataframe')

            handler.dump(dump_filename)
            exist = os.path.exists(dump_filename)

            if exist:
                check_handler = ExcelHandler()
                check_handler.load(dump_filename)
                check_df = check_handler.to_pandas()
                dump_columns = list(check_df.columns)
                dump_len = len(check_df)
            if exist and 'to_delete' in dump_filename:
                os.remove(dump_filename)

        except Exception as e:
            logger.error(f"\t File load fail by {e}")
            # self.assertTrue(True, f"\t File load fail by {e}")
        else:
            logger.info(f"\t load df len={load_len}, dump df len={dump_len}")
            self.assertEqual(load_len,  dump_len)
            logger.info(f"\t load columns len={len(load_columns)}, dump columns len={len(dump_columns)}")
            self.assertListEqual(load_columns, dump_columns)

    def test_with_options(self):
        expect_shape = (20, 3)

        load_filename = 'test_data/채널지수평가 샘플_v0.1.xlsx'
        dump_filename = 'test_data/채널지수평가 샘플_v0.1_to_delete.xlsx'
        try:
            handler = ExcelHandler()
            handler.load(load_filename, sheet_name='Youtube생산성', skiprows=1, header=0, nrows=20, usecols='B:D')
            df = handler.to_pandas()
            if df is not None:
                logger.info('\n'+table_to_string(df))
                load_columns = list(df.columns)
                load_shape = df.shape
                logger.info(f"expect dataframe shape={expect_shape} and get {load_shape}")
                self.assertEqual(expect_shape, load_shape)
            else:
                logger.error('empty dataframe')

            handler.dump(dump_filename)
            exist = os.path.exists(dump_filename)

            if exist:
                check_handler = ExcelHandler()
                check_handler.load(dump_filename)
                check_df = check_handler.to_pandas()
                dump_columns = list(check_df.columns)
                dump_shape = check_df.shape
            if exist and 'to_delete' in dump_filename:
                os.remove(dump_filename)

        except Exception as e:
            logger.error(f"\t File load fail by {e}")
            # self.assertTrue(True, f"\t File load fail by {e}")
        else:
            logger.info(f"\t load df {load_shape=}, dump df {dump_shape}")
            self.assertEqual(load_shape,  dump_shape)
            logger.info(f"\t load columns {load_columns}, dump columns len={dump_columns}")
            self.assertListEqual(load_columns, dump_columns)

    def test_multi_header_load_skiprows(self):
        expect_shape = (50, 8)
        test_skiprows = [0,    3,      [0, 1, 2], [0, 1, 2]]
        test_header = [[3, 4], [0, 1], [3, 4],    [0, 1]]
        expect_success = [True, True, False, True]
        expect_columns = [('수집항목', '개체번호'), ('과폭', 'cm'), ('과고', 'cm'), ('과중', 'g'), ('당도', 'Brix %'), ('산도', '0-14'), ('경도', 'kgf'), ('수분율', '%')]
        load_filename = 'test_data/multiheader_table.xlsx'
        try:
            for skiprows, header, succeed in zip(test_skiprows, test_header, expect_success):
                handler = ExcelHandler()
                logger.info(f"try load sheet_name='50주차', skiprows={skiprows}, header={header}, nrows=50")
                handler.load(load_filename, sheet_name='50주차', skiprows=skiprows, header=header, nrows=50)
                df = handler.to_pandas()
                if df is not None:

                    load_columns = list(df.columns)
                    load_shape = df.shape

                    logger.debug(f"load df columns={load_columns}")
                    if verbose:
                        logger.debug('\n' + table_to_string(df))
                    logger.debug(f"expect dataframe shape={expect_shape} and get {load_shape}")

                    if succeed:
                        self.assertTrue(expect_shape == load_shape and expect_columns == load_columns)
                    else:
                        self.assertTrue(not (expect_shape == load_shape and expect_columns == load_columns))
                    pass
                else:
                    logger.error('empty dataframe')

        except Exception as e:
            logger.error(f"\t File load fail by {e}")
            # self.assertTrue(True, f"\t File load fail by {e}")

    def test_multi_header_3(self):
        expect_shape = (100, 8)

        load_filename = 'test_data/multiheader_table.xlsx'
        dump_filename = 'test_data/multiheader_table_to_delete.xlsx'
        try:
            handler = ExcelHandler()
            handler.load(load_filename, sheet_name='50주차', skiprows=3, header=[0, 1], nrows=100)

            df = handler.to_pandas()
            if df is not None:
                if verbose:
                    logger.info('\n'+table_to_string(df))
                load_columns = list(df.columns)

                load_shape = df.shape
                logger.info(f"expect dataframe shape={expect_shape} and get {load_shape}")
                self.assertEqual(expect_shape, load_shape)
            else:
                logger.error('empty dataframe')

            handler.dump(dump_filename, sheet_name="학습데이터")
            exist = os.path.exists(dump_filename)

            if exist:
                check_handler = ExcelHandler()
                # sheet_name='50주차', skiprows=1, , nrows=100
                check_handler.load(dump_filename, skiprows=0, header=[0, 1], nrows=100 ),
                check_df = check_handler.to_pandas()
                dump_columns = list(check_df.columns)
                for c in dump_columns:
                    print(f"'[{c}]'")
                print('\n')
                dump_shape = check_df.shape
            if exist and 'to_delete' in dump_filename:
                os.remove(dump_filename)

        except Exception as e:
            logger.error(f"\t File load fail by {e}")
            # self.assertTrue(True, f"\t File load fail by {e}")
        else:
            logger.info(f"\t load df {load_shape=}, dump df {dump_shape}")
            self.assertEqual(load_shape,  dump_shape)
            logger.info(f"\t load columns {load_columns}, dump columns len={dump_columns}")
            self.assertListEqual(load_columns, dump_columns)


if __name__ == '__main__':
    unittest.main(verbosity=2)
