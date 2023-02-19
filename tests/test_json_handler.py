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
    def test_make_kw_dict_empty(self):
        handler = JsonHandler('array')
        kw_dict = {'abc': 'bcf'}
        kw_dict = handler._make_kw_dict('load', kw_dict)

        self.assertTrue('abc' not in kw_dict, "'abc' is not support keyword")
        self.assertDictEqual(handler.support_kw['load'], kw_dict, "result dict is not copy of support_kw ")
        self.assertEqual(handler.support_kw['load'], kw_dict, "Same as above?")

    def test_make_kw_dict_success(self):
        handler = JsonHandler('array')
        kw_dict = handler._make_kw_dict('load', {'encoding': 'ascii'})
        self.assertEqual(kw_dict['encoding'], 'ascii', "check change key value")

    def test_load_not_exist_file(self):
        handler = JsonHandler('object')
        try:
            handler.load('test_data/complex_one_object_not.json')
        except Exception as e:
            pass
        else:
            self.assertTrue(True, "File not exist")

    def test_load_complex_by_json_type(self):
        json_types = ['object', 'array', 'multiline']
        expect_passes = [1, 0, 0]
        expect_fails = [0, 1, 1942]

        for json_type, expect_pass, expect_fail in zip(json_types, expect_passes, expect_fails):
            try:
                handler = JsonHandler(json_type)
                handler.load('test_data/complex_one_object.json')
                pass_size = len(handler.pass_list)
                fail_size = len(handler.fail_list)
            except Exception as e:
                self.assertTrue(True, f"\t {json_type} json_type File load fail by {e}")
            else:
                logger.info(f"\t {json_type} load expect pass {expect_pass} get {pass_size}")
                self.assertTrue(pass_size == expect_pass)
                logger.info(f"\t {json_type} load expect fail {expect_fail} get {fail_size}")
                self.assertTrue(fail_size == expect_fail)

    def test_load_complex_by_data_key(self):
        json_types = ['object', 'array', 'multiline']
        expect_passes = [1, 102, 0]
        expect_fails = [0, 0, 1942]

        for json_type, expect_pass, expect_fail in zip(json_types, expect_passes, expect_fails):
            try:
                handler = JsonHandler(json_type)
                handler.load('test_data/complex_one_object.json', data_key='main')
                pass_size = len(handler.pass_list)
                fail_size = len(handler.fail_list)
            except Exception as e:
                self.assertTrue(True, f"\t {json_type} json_type File load fail by {e}")
            else:
                logger.info(f"\t {json_type} load expect pass {expect_pass} get {pass_size}")
                self.assertTrue(pass_size == expect_pass)
                logger.info(f"\t {json_type} load expect fail {expect_fail} get {fail_size}")
                self.assertTrue(fail_size == expect_fail)

    def test_load_mutliline_by_mode(self):
        modes = ['text', 'binary']
        expect_passes = [15, 15]
        expect_fails = [0, 0]

        for mode, expect_pass, expect_fail in zip(modes, expect_passes, expect_fails):
            try:
                handler = JsonHandler('multiline')
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
                handler = JsonHandler(json_type)
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


if __name__ == '__main__':
    unittest.main(verbosity=2)
