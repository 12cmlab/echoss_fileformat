import time
import unittest
import logging
from echoss_fileformat.fileformat_handler import FileformatHandler

# configure the logger
LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)
# use the logger


class FileformatHandlerTestCase(unittest.TestCase):
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
        handler = FileformatHandler()
        kw_dict = {'abc': 'bcf'}
        kw_dict = handler.make_kw_dict('load', kw_dict)

        self.assertTrue('abc' not in kw_dict, "'abc' is not support keyword")
        self.assertDictEqual(handler.support_kw['load'], kw_dict, "result dict is not copy of support_kw ")
        self.assertEqual(handler.support_kw['load'], kw_dict, "Same as above?")

    def test_make_kw_dict_success(self):
        handler = FileformatHandler()
        kw_dict = handler.make_kw_dict('load', {'encoding': 'ascii'})
        self.assertEqual(kw_dict['encoding'], 'ascii', "check change key value")  # add assertion here


if __name__ == '__main__':
    unittest.main(verbosity=2)
