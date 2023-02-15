# echoss_fileformat

# File Format Handlers

This project provides file format handler packages for JSON, CSV, XML, and Excel files. The packages provide an abstraction layer to load and save data in these file formats using a unified API.

## Installation

To install the package, use pip:
pip install echoss_fileformat

## Usage

The package provides an abstraction layer to load and save data in JSON, CSV, XML, and Excel formats. The API includes the following methods:

* `load(file, **kwargs)`: Load data from a file.
* `loads(string, **kwargs)`: Load data from a string.
* `dump(data, file, **kwargs)`: Save data to a file.
* `dumps(data, **kwargs)`: Save data to a string.

The following example demonstrates how to load data from a CSV file and save it as a JSON file:

```python
from echoss_fileformat import csv_handler, json_handler

# Load test_data from a CSV file
data = csv_handler.load('test_data.csv')

# Save test_data as a JSON file
json_handler.dump(data, 'test_data.json')
```

## Contributing
Contributions are welcome! If you find a bug or want to suggest a new feature, please open an issue on the GitHub repository.

## License
This project is licensed under the MIT License. See the LICENSE file for more information.

## Credits
This project was created by 12cm. Special thanks to 12cm R&D for their contributions to the project.
