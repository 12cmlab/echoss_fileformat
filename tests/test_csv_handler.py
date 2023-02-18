import unittest

from echoss_fileformat.csv_handler import CsvHandler


class MyTestCase(unittest.TestCase):

    def test_make_kw_dict_empty(self):
        csv_handler = CsvHandler()
        kw_dict = csv_handler.make_kw_dict('load', {'abc': 'bcf'})
        self.assertEqual('abc' in kw_dict, False)  # add assertion here

    def test_make_kw_dict_success(self):
        csv_handler = CsvHandler()
        kw_dict = csv_handler.make_kw_dict('load', {'usecols': 'bcf'})
        self.assertEqual(kw_dict['usecols'] == 'bcf', True)  # add assertion here


if __name__ == '__main__':
    unittest.main(verbosity=2)
