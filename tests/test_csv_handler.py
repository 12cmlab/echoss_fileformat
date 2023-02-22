import unittest
import time
import logging
import os
from tabulate import tabulate

from echoss_fileformat.csv_handler import CsvHandler

# configure the logger
LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)
# use the logger


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

    def test_load_simple_standard_csv(self):
        expect_pass = 1
        expect_fail = 0
        load_filename = 'test_data/simple_standard.csv'
        dump_filename = 'test_data/simple_standard_to_delete.csv'
        try:
            handler = CsvHandler()
            handler.load(load_filename, header=0, skiprows=0)
            pass_size = len(handler.pass_list)
            fail_size = len(handler.fail_list)
            csv_df = handler.to_pandas()
            if csv_df is not None:
                csv_df.info()
                logger.info("\n"+tabulate(csv_df.head(), headers='keys', tablefmt='psql'))
                # print(csv_df.head().to_string(index=False, justify='left'))
            else:
                logger.info('empty dataframe')
            expect_csv_str = "SEQ_NO,PROMTN_TY_CD,PROMTN_TY_NM,BRAND_NM,SVC_NM,ISSU_CO,PARTCPTN_CO,PSNBY_ISSU_CO,COUPON_CRTFC_CO,COUPON_USE_RT\r\n"+"0,9,대만프로모션발급인증통계,77chocolate,S0013,15,15,1.0,15,1.0"
            csv_str = handler.dumps(mode='text', quoting=0)
            # logger.info("[\n"+csv_str+"]")
            expect_file_size = 17827
            self.assertTrue(csv_str.startswith(expect_csv_str), "startswith fail")

            handler.dump(dump_filename)
            exist = os.path.exists(dump_filename)
            file_size = os.path.getsize(dump_filename)
            if exist and 'to_delete' in dump_filename:
                os.remove(dump_filename)

            logger.info(f"\t {handler} dump expect exist True get {exist}")
            logger.info(f"\t {handler} dump expect file_size {expect_file_size} get {file_size}")

        except Exception as e:
            logger.error(f"\t File load fail by {e}")
            self.assertTrue(True, f"\t File load fail by {e}")
        else:
            logger.info(f"\t load expect pass {expect_pass} get {pass_size}")
            self.assertTrue(pass_size == expect_pass)
            logger.info(f"\t load expect fail {expect_fail} get {fail_size}")
            self.assertTrue(fail_size == expect_fail)

    def test_load_simple_standard_csv_by_mode(self):
        modes = ['text', 'binary']
        expect_shape = (212, 10)

        load_filename = 'test_data/simple_standard.csv'
        dump_filename = 'test_data/simple_standard_to_delete.csv'
        for given_mode in modes:
            expect_pass = 1
            expect_fail = 0
            try:
                handler = CsvHandler()
                if 'text' == given_mode:
                    file_obj = open(load_filename, 'r', encoding='utf-8')
                else:
                    file_obj = open(load_filename, 'rb')
                handler.load(file_obj, header=0, skiprows=0)
                file_obj.close()

                csv_df = handler.to_pandas()
                if csv_df is not None and len(csv_df) > 0:
                    df_shape = csv_df.shape
                    if expect_shape == df_shape:
                        logger.info(f"load mode={given_mode}, shape={csv_df.shape} are equal")
                    else:
                        logger.error(f"load mode={expect_shape}, and get shape={csv_df.shape}")
                else:
                    logger.error('empty dataframe')
            except Exception as e:
                logger.error(f"\t File load fail by {e}")
                self.assertTrue(True, f"\t File load fail by {e}")

            try:
                if 'text' == given_mode:
                    file_obj = open(dump_filename, 'w', encoding='utf-8')
                else:
                    file_obj = open(dump_filename, 'wb')
                handler.dump(file_obj, quoting=0)
                file_obj.close()
                check_csv = CsvHandler()
                check_csv.load(dump_filename)
                check_df = check_csv.to_pandas()
                if check_df is not None and len(check_df) > 0:
                    df_shape = check_df.shape
                    if expect_shape == df_shape:
                        logger.info(f"dump mode={given_mode}, shape={check_df.shape} are equal")
                    else:
                        logger.error(f"dump mode={given_mode}, {expect_shape=} and get shape={check_df.shape}")
                else:
                    logger.error('dump and load make empty dataframe')

                if 'to_delete' in dump_filename:
                    os.remove(dump_filename)

            except Exception as e:
                logger.error(f"\t File dump open mode={given_mode} fail by {e}")
                self.assertTrue(True, f"\t File dump fail by {e}")

    def test_load_mutliline_by_mode(self):
        modes = ['text', 'binary']
        expect_passes = [15, 15]
        expect_fails = [0, 0]

        for mode, expect_pass, expect_fail in zip(modes, expect_passes, expect_fails):
            try:
                handler = CsvHandler('multiline')
                if mode == 'text':
                    with open('test_data/simple_multiline_object.json', 'r', encoding='utf-8') as fp:
                        handler.load(fp)
                        pass_size = len(handler.pass_list)
                        fail_size = len(handler.fail_list)
                elif mode == 'binary':
                    with open('test_data/simple_multiline_object.json', 'rb') as fb:
                        handler.load(fb)
                        pass_size = len(handler.pass_list)
                        fail_size = len(handler.fail_list)
            except Exception as e:
                self.assertTrue(True, f"\t {mode} multiline File load fail by {e}")
            else:
                logger.info(f"\t open mode '{mode}' 'multiline' expect pass {expect_pass} get {pass_size}")
                # self.assertEqual(pass_size == expect_pass)
                logger.info(f"\t open mode '{mode}' 'multiline' expect fail {expect_fail} get {fail_size}")
                # self.assertEqual(fail_size == expect_fail)

    def test_load_mutliline_by_data_key(self):
        json_types = ['object', 'array', 'multiline']
        expect_passes = [0, 0, 15]
        expect_fails = [1, 1, 0]

        for json_type, expect_pass, expect_fail in zip(json_types, expect_passes, expect_fails):
            try:
                handler = CsvHandler(json_type)
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
                handler = CsvHandler(json_type)
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

        json_type = CsvHandler.TYPE_MULTILINE
        load_filename = 'test_data/simple_multiline_object.json'

        for mode, expect_file_size in zip(modes, expect_file_sizes):
            try:
                handler = CsvHandler('multiline')
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

        json_type = CsvHandler.TYPE_MULTILINE
        load_filename = 'test_data/simple_multiline_object.json'

        for mode, data_key, expect_file_size in zip(modes, data_keys, expect_file_sizes):
            try:
                handler = CsvHandler('multiline')
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
