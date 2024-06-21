import io
import unittest
import os
import time
import pandas as pd

from echoss_fileformat import FileUtil, PandasUtil, get_logger, set_logger_level

logger = get_logger("test_fileutil", backup_count=1)


class FileUtilTestCase(unittest.TestCase):
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
    def test_get_file_obj_not_exist(self):
        # not exist Directory and 'r'
        filename = 'test_data_wrong/complex_one.json'
        open_mode = 'r'
        with self.assertRaises(FileNotFoundError) as context:
            fail_df = FileUtil.load(filename)

        self.assertTrue(filename in str(context.exception))

        # not exist Directory and 'w'
        save_df = pd.DataFrame()
        filename = 'test_data_wrong/complex_one.json'
        open_mode = 'w'
        FileUtil.dump(save_df, filename)

        # exist Directory and 'r
        filename = 'test_data/complex_one_not_exist.json'
        open_mode = 'r'
        with self.assertRaises(FileNotFoundError) as context:
            fail_df = FileUtil.load(filename)

        self.assertTrue(filename in str(context.exception))

        # exist Directory and 'w'
        filename = 'test_data/complex_one_not_exist_to_delete.json'
        open_mode = 'w'
        try:
            FileUtil.dump(save_df, filename)
        except Exception as e:
            self.assertTrue(filename in str(e))

        os.remove(filename)

    def test_jsonl_multiline_file(self):

        open_modes = ['r', 'w']
        filenames = [
            'test_data/simple_multiline_object.jsonl',
            'test_data/simple_multiline_object_to_delete.json',
        ]

        read_df = pd.DataFrame()

        line_list = []
        for filename, open_mode in zip(filenames, open_modes):
            if open_mode in ['r']:
                try:
                    read_df = FileUtil.load(filename, data_key="message")
                except Exception as e:
                    logger.error (f"load error : {e}")

            elif open_mode in ['w', 'wb']:
                try:
                    FileUtil.dump(read_df, filename)
                except Exception as e:
                    logger.error (f"load error : {e}")

            if open_mode in ['r']:
                PandasUtil.set_markers(vertical='#', horizontal='=', corner='*')
                logger.info(f"read_df = {PandasUtil.to_table(read_df, max_cols=8)}")

                logger.info(f"assertEqual({len(line_list)}, 15)")
                # self.assertEqual(len(line_list), 15)
            elif open_mode in ['w', 'wb']:
                logger.info(f"{os.path.exists(filename)=} and {(os.path.getsize(filename) > 0)=} are all True ")
                self.assertTrue(os.path.exists(filename))
                self.assertTrue(os.path.getsize(filename) > 0)
                if '_to_delete' in filename:
                    os.remove(filename)
        pass


if __name__ == '__main__':
    unittest.main(verbosity=2)
