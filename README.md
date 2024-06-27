# echoss_fileformat v1.1

# File Format Handlers

This project provides file format handler packages for JSON, CSV, XML, and Excel files. The packages provide an abstraction layer to load and save data in these file formats using a unified API.

## Version History
- v1.0 : Object method
- v1.1 : static method 

## Installation

To install the package, use pip:
pip install echoss_fileformat

To upgrade the installed package, use pip:
pip install echoss_fileformat -U

## Usage

1. static call 

python
from echoss_fileformat import FileUtil, PandasUtil, get_logger, set_logger_level

# FileUtil Load test_data from a file
csv_df = FileUtil.load('test_data.csv')
excel_df = FileUtil.load('weather_data.xls')
json_df = FileUtil.load('test_data.json', data_key = 'data')
jsonl_df = FileUtil.load('test_multiline_json.jsonl', data_key = 'data')

# FileUtil Save DataFrame to a file
FileUtil.dump(df, 'save/test_data_1.csv')
FileUtil.load(df, 'weather_data.xlsx')
FileUtil.load('test_data_2.jsonl')

# PandasUtil DataFrame to table-like string
logger = get_logger("echoss_fileformat_test")
set_logger_level("DEBUG")
logger.debug(PandasUtil.to_table(df))


파일 포맷 읽기 :
- FileUtil.load(file_path: str, **kwargs) : 파일 확장자 기준으로 매칭되는 파일포맷으로 읽음
  * 확장자 .csv : 기본csv 파일 포맷으로 읽기. 컬럼 구분자는 콤마(,) 사용.
  * 확장자 .tsv : 컬럼 구분자 탭(\t)인 csv 파일 포맷으로 읽기.
  * 확장자 .xls .xlsx : excel 파일 포맷으로 읽기
  * 확장자 .json : 파일 전체가 1개의 json 객체인 파일포맷으로읽기 (전체가 JSON array 인경우는 객체로 감싸야함)
  * 확자자 .jsonl : 라인 하나가 json  객체인 multi line json 파일 형태
  * 확장자 .xml : xml 파일포맷으로 읽기
- FileUtil.read_csv(filename_or_file, **kwargs) :  csv 파일 포맷으로 파일명  또는 file-like object 로 읽음
- FileUtil.read_excel(filename_or_file, **kwargs) :  excel 파일 포맷으로 파일명  또는 file-like object 로 읽음
- FileUtil.read_json(filename_or_file, **kwargs) :  json 파일 포맷으로 파일명  또는 file-like object 로 읽음
- FileUtil.read_jsonl(filename_or_file, **kwargs) :  jsonl 파일 포맷으로 파일명  또는 file-like object 로 읽음
- FileUtil.read_xml(filename_or_file, **kwargs) :  csv 파일 포맷으로 파일명  또는 file-like object 로 읽음

파일 포맷 쓰기 :
- FileUtil.dump(df: pd.DataFrame, file_path: str, **kwargs) : 파일 확장자 기준으로 매칭되는 파일포맷으로 쓰기
  * 확장자 .csv : 기본csv 파일 포맷으로 쓰기. 컬럼 구분자는 콤마(,) 사용.
  * 확장자 .tsv : 컬럼 구분자 탭(\t)인 csv 파일 포맷으로 쓰기.
  * 확장자 .xls .xlsx : excel 파일 포맷으로 쓰기
  * 확장자 .json : 파일 전체가 1개의 json 객체인 파일포맷으로쓰기 (전체가 JSON array 인경우는 객체로 감싸야함)
  * 확자자 .jsonl : 라인 하나가 json  객체인 multi line json 파일 형태
  * 확장자 .xml : xml 파일포맷으로 쓰기
- FileUtil.to_csv(filename_or_file, **kwargs) :  csv 파일 포맷으로 파일명  또는 file-like object 로 쓰기
- FileUtil.to_tsv(filename_or_file, **kwargs) :  tsv 파일 포맷으로 파일명  또는 file-like object 로 쓰기
- FileUtil.to_xls(filename_or_file, **kwargs) :  클래식 excel 파일 포맷으로 파일명  또는 file-like object 로 쓰기
- FileUtil.to_xlsx(filename_or_file, **kwargs) :  신규 excel 파일 포맷으로 파일명  또는 file-like object 로 쓰기
- FileUtil.to_excel(filename_or_file, **kwargs) :  파일명에 따라 excel 포맷을 결정하여 파일명  또는 file-like object 로 쓰기
- FileUtil.to_json(filename_or_file, **kwargs) :  json 파일 포맷으로 파일명  또는 file-like object 로 쓰기
- FileUtil.to_jsonl(filename_or_file, **kwargs) :  jsonl 파일 포맷으로 파일명  또는 file-like object 로 쓰기
- FileUtil.to_xml(filename_or_file, **kwargs) :  csv 파일 포맷으로 파일명  또는 file-like object 로 쓰기


2. Object 
- 학습데이터가 아닌 메타데이터 객체로 읽어들일 경우

handler = CsvHandler('object')

- 학습데이터로 읽어들이는 경우 

handler = ExcelHandler()
또는 handler = ExcelHandler('array')

- JSON 파일 중에서 각 줄이 하나의 json 객체일 경우

handler = JsonHandler('multiline')


The package provides an abstraction layer to load and save data in JSON, CSV, XML, and Excel formats. The API includes the following methods:

* `load(file_or_filename, **kwargs)`: Load data from a file.
* `loads(bytes_or_str, **kwargs)`: Load data from a string.
* `dump(file_or_filename, data = None, **kwargs)`: Save data to a file.
* `dumps(data = None, **kwargs)`: Save data to a string.

The following example demonstrates how to load data from a CSV file and save it as a JSON file:

```python
from echoss_fileformat import CsvHandler, JsonHandler

# Load test_data from a CSV file
csv_handler = CsvHandler()
data = csv_handler.load('test_data.csv', header=[0, 1])

# Save test_data as a JSON file
json_handler = JsonHandler('array')
json_handler.load( 'test_data_1.json', data_key = 'data')
json_handler.load( 'test_data_2.json', data_key = 'data')
json_handler.dump( 'test_data_all.json')
```

## Contributing
Contributions are welcome! If you find a bug or want to suggest a new feature, please open an issue on the GitHub repository.

## License
This project is licensed under the MIT License. See the LICENSE file for more information.

## Credits
This project was created by 12cm. Special thanks to 12cm R&D for their contributions to the project.
