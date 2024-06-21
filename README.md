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
