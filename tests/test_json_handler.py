import unittest
import time
import logging

from echoss_fileformat.json_handler import JsonHandler

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
        ids = self.id().split('.')
        self.str_id = f"{ids[-2]}: {ids[-1]}"
        self.start_time = time.perf_counter()
        logger.info(f"setting up test [{self.str_id}] ")

    def tearDown(self):
        self.end_time = time.perf_counter()
        logger.info(f" tear down test [{self.str_id}] elapsed time {(self.end_time-self.start_time)*1000: .3f}ms \n")

    """
    유닛 테스트 
    """
    def test_make_kw_dict_empty(self):
        handler = JsonHandler('array')
        kw_dict = {'abc': 'bcf'}
        kw_dict = handler.make_kw_dict('load', kw_dict)

        self.assertTrue('abc' not in kw_dict, "'abc' is not support keyword")
        self.assertDictEqual(handler.support_kw['load'], kw_dict, "result dict is not copy of support_kw ")
        self.assertEqual(handler.support_kw['load'], kw_dict, "Same as above?")

    def test_make_kw_dict_success(self):
        handler = JsonHandler('array')
        kw_dict = handler.make_kw_dict('load', {'encoding': 'ascii'})
        self.assertEqual(kw_dict['encoding'], 'ascii', "check change key value")

    def test_load_not_exist_file(self):
        handler = JsonHandler('object')
        try:
            handler.load('test_data/complex_one_object_not.json')
        except Exception as e:
            pass
        else:
            self.assertTrue(True, "File not exist")

    def test_load_by_json_type(self):
        json_types = ['object', 'array', 'multiline']
        expect_fail = [0, 1, 1942]

        for json_type, expect_fail in zip(json_types, expect_fail):
            try:
                handler = JsonHandler(json_type)
                handler.load('test_data/complex_one_object.json')
                pass_size = len(handler.pass_list)
                fail_size = len(handler.fail_list)
            except Exception as e:
                self.assertTrue(True, f"\t {json_type} json_type File load fail by {e}")
            else:
                logger.info(f"\t {json_type} load fail expect {expect_fail} and given {fail_size}")
                self.assertTrue(fail_size == expect_fail)

    def test_load_by_data_key(self):
        json_types = ['object', 'array', 'multiline']
        expect_fail = [0, 0, 1942]
        handler = JsonHandler('object', data_key='main')

        for json_type, expect_fail in zip(json_types, expect_fail):
            try:
                handler = JsonHandler(json_type)
                handler.load('test_data/complex_one_object.json')
                pass_size = len(handler.pass_list)
                fail_size = len(handler.fail_list)
            except Exception as e:
                self.assertTrue(True, f"\t {json_type} json_type File load fail by {e}")
            else:
                logger.info(f"\t '{json_type}' load fail expect {expect_fail} and given {fail_size}")
                self.assertTrue(fail_size == expect_fail, f"Following test loop canceled {json_types}")


if __name__ == '__main__':
    unittest.main(verbosity=2)
