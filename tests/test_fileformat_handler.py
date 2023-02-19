import io
import unittest
import logging
import os
import time
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
        kw_dict = handler._make_kw_dict('load', kw_dict)

        self.assertTrue('abc' not in kw_dict, "'abc' is not support keyword")
        self.assertDictEqual(handler.support_kw['load'], kw_dict, "result dict is not copy of support_kw ")
        self.assertEqual(handler.support_kw['load'], kw_dict, "Same as above?")

    def test_make_kw_dict_success(self):
        handler = FileformatHandler()
        kw_dict = handler._make_kw_dict('load', {'encoding': 'ascii'})
        self.assertEqual(kw_dict['encoding'], 'ascii', "check change key value")

    def test_get_file_obj_not_exist(self):
        handler = FileformatHandler()

        # not exist Directory and 'r'
        filename = 'test_data_wrong/complex_one.json'
        open_mode = 'r'
        with self.assertRaises(FileNotFoundError) as context:
            fp, mode, opened = handler._get_file_obj(filename, open_mode)
            if opened:
                fp.close()
        self.assertTrue(filename in str(context.exception))

        # not exist Directory and 'w'
        filename = 'test_data_wrong/complex_one.json'
        open_mode = 'w'
        with self.assertRaises(FileNotFoundError) as context:
            fp, mode, opened = handler._get_file_obj(filename, open_mode)
            if opened:
                fp.close()
        self.assertTrue(filename in str(context.exception))

        # exist Directory and 'r
        filename = 'test_data/complex_one_not_exist.json'
        open_mode = 'r'
        with self.assertRaises(FileNotFoundError) as context:
            fp, mode, opened = handler._get_file_obj(filename, open_mode)
            if opened:
                fp.close()
        self.assertTrue(filename in str(context.exception))

        # exist Directory and 'w'
        filename = 'test_data/complex_one_not_exist_to_delete.json'
        open_mode = 'w'
        try:
            fp, mode, opened = handler._get_file_obj(filename, open_mode)
            if opened:
                fp.close()
                # 임시 파일 삭제
                os.remove(filename)
        except Exception as e:
            self.assertTrue(filename in str(e))

    def test_get_file_obj_open_mode_filename(self):
        handler = FileformatHandler()

        # not exist Directory and 'r'
        open_modes = ['r', 'w', 'rb', 'wb']
        filenames = [
            'test_data/simple_multiline_object.json',
            'test_data/simple_multiline_object_to_delete.json',
            'test_data/simple_multiline_object.json',
            'test_data/simple_multiline_object_to_delete.json',
        ]

        line_list = []
        for filename, open_mode in zip(filenames, open_modes):
            handler = FileformatHandler()
            fp, mode, opened = handler._get_file_obj(filename, open_mode)
            logger.info(f"{fp=} {mode=} {opened=}")

            if open_mode in ['r', 'rb']:
                try:
                    for l in fp:
                        line_list.append(l)
                except Exception as e:
                    logger.info(f"{e}")

            elif open_mode in ['w', 'wb']:
                for l in line_list:
                    fp.write(l)
                line_list.clear()

            if opened:
                fp.close()

            if open_mode in ['r', 'rb']:
                self.assertEqual(len(line_list), 15)
            elif open_mode in ['w', 'wb']:
                self.assertTrue(os.path.exists(filename))
                self.assertTrue(os.path.getsize(filename) > 0)
                if '_to_delete' in filename:
                    os.remove(filename)
        pass

    def test_get_file_obj_open_mode_file_obj(self):
        handler = FileformatHandler()

        # not exist Directory and 'r'
        open_modes = ['r', 'w', 'rb', 'wb']
        filenames = [
            'test_data/simple_multiline_object.json',
            'test_data/simple_multiline_object_to_delete.json',
            'test_data/simple_multiline_object.json',
            'test_data/simple_multiline_object_to_delete.json',
        ]
        expect_instances = [
            io.TextIOWrapper,
            io.TextIOWrapper,
            io.BufferedIOBase,
            io.BufferedIOBase
        ]

        line_list = []
        for filename, open_mode, expect_instance in zip(filenames, open_modes, expect_instances):
            handler = FileformatHandler()

            if 'b' in open_mode:
                fp = open(filename, open_mode)
            else:
                fp = open(filename, open_mode, encoding='utf-8')

            result_fp, mode, opened = handler._get_file_obj(fp, open_mode)
            logger.info(f"{result_fp=} {mode=} {opened=}")

            self.assertTrue(isinstance(fp, expect_instance))

            if fp:
                fp.close()
                if '_to_delete' in filename:
                    os.remove(filename)
        pass


if __name__ == '__main__':
    unittest.main(verbosity=2)
