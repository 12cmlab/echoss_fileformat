import unittest
import time
import logging
import os
# from tabulate import tabulate
import pandas as pd

from echoss_fileformat.excel_handler import ExcelHandler

# configure the logger
LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)
# use the logger


def table_to_string(df: pd.DataFrame):
    """

    Args:
        df:

    Returns:

    """
    return df.to_string(index=False, max_cols=10, max_rows=5, justify='left', show_dimensions=True,
                        col_space=8, max_colwidth=24)


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
                logger.info(table_to_string)
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
                logger.info(table_to_string(df))
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

    def test_multi_header_3(self):
        expect_shape = (100, 8)

        load_filename = 'test_data/multiheader_table.xlsx'
        dump_filename = 'test_data/multiheader_table_to_delete.xlsx'
        try:
            handler = ExcelHandler()
            handler.load(load_filename, sheet_name='50주차', skiprows=0, header=[3, 4], nrows=100)

            df = handler.to_pandas()
            if df is not None:
                logger.info(table_to_string(df))
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

    def test_load_mutliline_by_data_key(self):
        json_types = ['object', 'array', 'multiline']
        expect_passes = [0, 0, 15]
        expect_fails = [1, 1, 0]

        for json_type, expect_pass, expect_fail in zip(json_types, expect_passes, expect_fails):
            try:
                handler = ExcelHandler(json_type)
                handler.load('test_data/simple_multiline_object.json', data_key='message')
                pass_size = len(handler.pass_list)
                fail_size = len(handler.fail_list)
            except Exception as e:
                self.assertTrue(True, f"\t {json_type} json_type File load fail by {e}")
            else:
                logger.info(f"\t {json_type} load expect pass {expect_pass} get {pass_size}")
                self.assertTrue(pass_size == expect_pass)
                logger.info(f"\t {json_type} load expect fail {expect_fail} get {fail_size}")
                self.assertTrue(fail_size == expect_fail)

    """
    dump
    """

    def test_dump_complex_by_json_type(self):
        json_types = ['object', 'array']
        data_keys = ['', 'main']
        expect_file_sizes = [35248, 29257]

        load_filename = 'test_data/complex_one_object.json'
        dump_filename = 'test_data/complex_one_object_dump_to_delete.json'

        for json_type, data_key, expect_file_size  in zip(json_types, data_keys, expect_file_sizes):
            try:
                handler = ExcelHandler(json_type)
                handler.load(load_filename, data_key=data_key)
                pass_size = len(handler.pass_list)
                if pass_size > 0:
                    handler.dump(dump_filename)
                    exist = os.path.exists(dump_filename)
                    file_size = os.path.getsize(dump_filename)

                    if exist and 'to_delete' in dump_filename:
                        os.remove(dump_filename)

                    logger.info(f"\t {json_type} dump expect exist True get {exist}")
                    self.assertEqual(True, exist)
                    logger.info(f"\t {json_type} dump expect file_size {expect_file_size} get {file_size}")
                    self.assertEqual(expect_file_size, file_size)
            except Exception as e:
                self.assertTrue(True, f"\t {json_type} json_type File dump fail by {e}")

    def test_dump_mutliline_by_mode(self):
        # multiline 은 내부적으로 binary 로 동작하여 외부 지정이 의미가 없음
        modes = ['text', 'binary']
        expect_file_sizes = [13413, 13413]

        json_type = ExcelHandler.TYPE_MULTILINE
        load_filename = 'test_data/simple_multiline_object.json'

        for mode, expect_file_size in zip(modes, expect_file_sizes):
            try:
                handler = ExcelHandler('multiline')
                if mode == 'text':
                    with open(load_filename, 'r', encoding='utf-8') as fp:
                        handler.load(fp)
                        pass_size = len(handler.pass_list)
                        fail_size = len(handler.fail_list)
                elif mode == 'binary':
                    with open(load_filename, 'rb') as fb:
                        handler.load(fb)
                        pass_size = len(handler.pass_list)
                        fail_size = len(handler.fail_list)
            except Exception as e:
                self.assertTrue(True, f"\t {mode} multiline File load fail by {e}")
            else:
                logger.info(f"\t open mode '{mode}' 'multiline' load {pass_size=}, {fail_size=}")

            try:
                dump_filename = f'test_data/simple_multiline_{mode}_object_to_delete.json'
                handler.dump(dump_filename)
                exist = os.path.exists(dump_filename)
                file_size = os.path.getsize(dump_filename)

                if exist and '_to_delete' in dump_filename:
                    os.remove(dump_filename)

                logger.info(f"\t {json_type} dump expect exist True get {exist}")
                self.assertEqual(True, exist)
                logger.info(f"\t {json_type} dump expect file_size {expect_file_size} get {file_size}")
                self.assertEqual(expect_file_size, file_size)
            except Exception as e:
                self.assertTrue(True, f"\t {mode} multiline File load fail by {e}")


    def test_dump_mutliline_by_data_key(self):
        modes = ['text', 'binary']
        data_keys = ['message', 'message']
        expect_file_sizes = [10513, 10513]

        json_type = ExcelHandler.TYPE_MULTILINE
        load_filename = 'test_data/simple_multiline_object.json'

        for mode, data_key, expect_file_size in zip(modes, data_keys, expect_file_sizes):
            try:
                handler = ExcelHandler('multiline')
                if mode == 'text':
                    with open(load_filename, 'r', encoding='utf-8') as fp:
                        handler.load(fp, data_key=data_key)
                        pass_size = len(handler.pass_list)
                        fail_size = len(handler.fail_list)
                elif mode == 'binary':
                    with open(load_filename, 'rb') as fb:
                        handler.load(fb, data_key=data_key)
                        pass_size = len(handler.pass_list)
                        fail_size = len(handler.fail_list)
            except Exception as e:
                self.assertTrue(True, f"\t {mode} multiline File load fail by {e}")
                # logger.error(True, f"\t {mode} multiline File load fail by {e}")
            else:
                logger.info(f"\t open mode '{mode}' 'multiline' load {pass_size=}, {fail_size=}")

            try:
                dump_filename = f'test_data/simple_multiline_{mode}_object_to_delete.json'
                handler.dump(dump_filename, data_key=data_key)
                exist = os.path.exists(dump_filename)
                file_size = os.path.getsize(dump_filename)

                if exist and '_to_delete' in dump_filename:
                    os.remove(dump_filename)

                logger.info(f"\t {json_type} dump expect exist True get {exist}")
                self.assertEqual(True, exist)
                logger.info(f"\t {json_type} dump expect file_size {expect_file_size} get {file_size}")
                self.assertEqual(expect_file_size, file_size)
            except Exception as e:
                self.assertTrue(True, f"\t {mode} multiline File load fail by {e}")
                # logger.error(f"\t {mode} multiline File load fail by {e}")


if __name__ == '__main__':
    unittest.main(verbosity=2)
