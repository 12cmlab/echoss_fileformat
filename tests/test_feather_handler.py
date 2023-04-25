import unittest
import time
import logging
import os

from echoss_fileformat.csv_handler import CsvHandler
from echoss_fileformat.feather_handler import FeatherHandler

from dataframe_print import print_table, print_dataframe, print_taburate

# configure the logger
LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)
# use the logger

verbose = True

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

    def test_load_csv_dump_feather(self):
        expect_pass = 1
        expect_fail = 0
        expect_file_size = 14554
        load_filename = 'test_data/simple_standard.csv'
        dump_filename = 'test_data/simple_standard_to_delete.feather'
        try:
            csv_handler = CsvHandler()
            csv_handler.load(load_filename, header=0, skiprows=0)
            pass_size = len(csv_handler.pass_list)
            fail_size = len(csv_handler.fail_list)
            csv_df = csv_handler.to_pandas()
            expect_csv_str = "SEQ_NO,PROMTN_TY_CD,PROMTN_TY_NM,BRAND_NM,SVC_NM,ISSU_CO,PARTCPTN_CO,PSNBY_ISSU_CO,COUPON_CRTFC_CO,COUPON_USE_RT\r\n"+"0,9,대만프로모션발급인증통계,77chocolate,S0013,15,15,1.0,15,1.0"
            csv_str = csv_handler.dumps()
            # logger.info("[\n"+csv_str+"]")
            self.assertTrue(csv_str.startswith(expect_csv_str), "startswith fail")

            feather_handler = FeatherHandler()
            feather_handler.dump(dump_filename, data=csv_df)
            exist = os.path.exists(dump_filename)
            file_size = os.path.getsize(dump_filename)
            if exist and 'to_delete' in dump_filename:
                os.remove(dump_filename)

            logger.info(f"\t {feather_handler} dump expect exist True get {exist}")
            logger.info(f"\t {feather_handler} dump expect file_size {expect_file_size} get {file_size}")

        except Exception as e:
            logger.error(f"\t File load fail by {e}")
            self.assertTrue(True, f"\t File load fail by {e}")
        else:
            logger.info(f"\t load expect pass {expect_pass} get {pass_size}")
            self.assertTrue(pass_size == expect_pass)
            logger.info(f"\t load expect fail {expect_fail} get {fail_size}")
            self.assertTrue(fail_size == expect_fail)

    def test_load_feather(self):
        load_filename = 'test_data/simple_object.feather'
        dump_filename = 'test_data/simple_object_to_delete.feather'

        expect_pass = 1
        expect_fail = 0
        expect_shape = (10067,54)
        expect_file_size = 706626

        try:
            handler = FeatherHandler()
            read_df = handler.load(load_filename)

        except Exception as e:
            logger.error(f"\t File load fail by {e}")
            self.assertTrue(True, f"\t File load fail by {e}")

        try:
            handler.dump(dump_filename, data=read_df)
            exist = os.path.exists(dump_filename)
            file_size = os.path.getsize(dump_filename)

            if 'to_delete' in dump_filename:
                os.remove(dump_filename)

        except Exception as e:
            logger.error(f"\t File dump fail by {e}")
            self.assertTrue(True, f"\t File dump fail by {e}")
        else:
            logger.info(f"{handler} expect shape={expect_shape}, and get shape={read_df.shape}")
            self.assertEqual(expect_shape, read_df.shape)
            logger.info(f"{handler} dump expect exist True get {exist}")
            self.assertTrue(exist)
            logger.info(f"{handler} dump expect file_size {expect_file_size} get {file_size}")
            self.assertEqual(expect_file_size, file_size)


if __name__ == '__main__':
    unittest.main(verbosity=2)
