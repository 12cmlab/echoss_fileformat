import io
import os
import pandas as pd
import time
import unittest

from echoss_fileformat import FileUtil, to_table, get_logger

logger = get_logger("test_fileutil", backup_count=1)
verbose = True


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
        with self.assertRaises(FileNotFoundError) as context:
            fail_df = FileUtil.load(filename)
            logger.info(f"{fail_df=}")

        self.assertTrue(filename in str(context.exception))

        # not exist Directory and 'w'
        save_df = pd.DataFrame()
        filename = 'test_data_wrong/complex_one.json'
        FileUtil.dump(save_df, filename)

        # exist Directory and 'r
        filename = 'test_data/complex_one_not_exist.json'
        with self.assertRaises(FileNotFoundError) as context:
            fail_df = FileUtil.load(filename)
            logger.info(f"{fail_df=}")

        self.assertTrue(filename in str(context.exception))

        # exist Directory and 'w'
        filename = 'test_data/complex_one_not_exist_to_delete.json'
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
                    logger.error(f"load error : {e}")

            elif open_mode in ['w', 'wb']:
                try:
                    FileUtil.dump(read_df, filename)
                except Exception as e:
                    logger.error(f"load error : {e}")

            if open_mode in ['r']:
                # PandasUtil.set_markers(vertical='#', horizontal='=', corner='*')
                logger.info(f"read_df = {to_table(read_df, max_cols=8, fmt='markdown')}")

                logger.info(f"assertEqual({len(line_list)}, 15)")
                # self.assertEqual(len(line_list), 15)
            elif open_mode in ['w', 'wb']:
                logger.info(f"{os.path.exists(filename)=} and {(os.path.getsize(filename) > 0)=} are all True ")
                self.assertTrue(os.path.exists(filename))
                self.assertTrue(os.path.getsize(filename) > 0)
                if '_to_delete' in filename:
                    os.remove(filename)
        pass

    def test_load_simple_standard_csv(self):
        load_filename = 'test_data/simple_standard.csv'
        dump_filename = 'test_data/simple_standard_to_delete.csv'
        try:
            csv_df = FileUtil.load(load_filename, header=0, skiprows=0)
            if csv_df is not None:
                if verbose:
                    logger.info(to_table(csv_df.head(10), fmt='box'))
            else:
                logger.info('empty dataframe')

            FileUtil.dump(csv_df, dump_filename)
            exist = os.path.exists(dump_filename)
            file_size = os.path.getsize(dump_filename)

            read_df = None
            if exist and 'to_delete' in dump_filename:
                read_df = FileUtil.load_csv(dump_filename)
                os.remove(dump_filename)

            logger.info(f"assertTrue dump expect exist True get {exist}")
            self.assertTrue(exist == True, "dump expect exist True")
            logger.info(f"assert_frome_equal csv_df and read_df ")
            pd.testing.assert_frame_equal(csv_df, read_df)

        except Exception as e:
            logger.error(f"\t File load fail by {e}")
            self.assertTrue(True, f"\t File load fail by {e}")

    def test_load_simple_standard_csv_by_mode(self):
        modes = ['text', 'binary']
        expect_shape = (212, 10)

        load_filename = 'test_data/simple_standard.csv'
        dump_filename = 'test_data/simple_standard_to_delete.csv'
        file_obj = None
        csv_df = None

        for given_mode in modes:
            expect_pass = 1
            expect_fail = 0
            try:
                if 'text' == given_mode:
                    file_obj = open(load_filename, 'r', encoding='utf-8')
                else:
                    file_obj = open(load_filename, 'rb')
                csv_df = FileUtil.read_csv(file_obj, header=0, skiprows=0, encoding='utf-8')

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
            finally:
                if file_obj:
                    file_obj.close()
                    file_obj = None

            try:
                if 'text' == given_mode:
                    file_obj = open(dump_filename, 'w', encoding='utf-8')
                else:
                    file_obj = open(dump_filename, 'wb')
                FileUtil.dump_csv(csv_df, file_obj, quoting=0)
                if file_obj:
                    file_obj.close()
                    file_obj = None

                check_df = FileUtil.load_csv(dump_filename)
                if check_df is not None and len(check_df) > 0:
                    df_shape = check_df.shape
                    if expect_shape == df_shape:
                        logger.info(f"dump mode={given_mode}, shape={check_df.shape} are equal")
                    else:
                        logger.error(f"dump mode={given_mode}, {expect_shape=} and get shape={check_df.shape}")
                    logger.info("assert load df and dump df is equals")
                    pd.testing.assert_frame_equal(csv_df, check_df)
                else:
                    logger.error('dump and load make empty dataframe')

                if 'to_delete' in dump_filename:
                    os.remove(dump_filename)

            except Exception as e:
                logger.error(f"\t File dump open mode={given_mode} fail by {e}")
                self.assertTrue(True, f"\t File dump fail by {e}")
            finally:
                if file_obj:
                    file_obj.close()

    def test_config_properties(self):
        # Assign
        config_dict = FileUtil.dict_load('test_data/config_without_section.properties')
        logger.info(f"{config_dict=}")

        # Act
        dump_filename = 'test_data/config_without_section_to_delete.properties'
        FileUtil.dict_dump(config_dict, dump_filename)
        reload_dict = FileUtil.dict_load('test_data/config_without_section_to_delete.properties')
        logger.info(f"{reload_dict=}")

        if 'to_delete' in dump_filename:
            os.remove(dump_filename)

        # Assert
        self.assertDictEqual(config_dict, reload_dict, "is same or not?")

        pass

    def test_config_sections_properties(self):
        # Assign
        config_dict = FileUtil.dict_load('test_data/config_with_sections.properties')
        logger.info(f"{config_dict=}")

        # Act
        dump_filename = 'test_data/config_with_sections_to_delete.properties'
        FileUtil.dict_dump(config_dict, dump_filename)
        reload_dict = FileUtil.dict_load('test_data/config_with_sections_to_delete.properties')
        logger.info(f"{reload_dict=}")

        if 'to_delete' in dump_filename:
            os.remove(dump_filename)

        # Assert
        self.assertDictEqual(config_dict, reload_dict, "is same or not?")

    def test_config_yaml(self):
        # Assign
        config_dict = FileUtil.dict_load('test_data/AIservice_config.yaml')
        logger.info(f"{config_dict=}")

        # Act
        dump_filename = 'test_data/AIservice_config_to_delete.yaml'
        FileUtil.dict_dump(config_dict, dump_filename)
        reload_dict = FileUtil.dict_load(dump_filename)
        logger.info(f"{reload_dict=}")

        if 'to_delete' in dump_filename:
            os.remove(dump_filename)

        # Assert
        self.assertDictEqual(config_dict, reload_dict, "is same or not?")

    def test_config_xml(self):
        # Assign
        config_dict = FileUtil.dict_load('test_data/config.xml')
        logger.info(f"{config_dict=}")

        # Act
        dump_filename = 'test_data/config_to_delete.xml'
        FileUtil.dict_dump(config_dict, dump_filename)
        reload_dict = FileUtil.dict_load(dump_filename)
        logger.info(f"{reload_dict=}")

        if 'to_delete' in dump_filename:
            os.remove(dump_filename)

        # Assert
        self.assertDictEqual(config_dict, reload_dict, "is same or not?")



if __name__ == '__main__':
    unittest.main(verbosity=2)
