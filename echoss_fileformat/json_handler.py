import io

from abstract_fileformat_handler import AbstractFileFormatHandler
import json
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger('echoss_fileformat')

class JsonHandler(AbstractFileFormatHandler):
    def __init__(self, json_type='object', encoding='utf-8', use_key=''):
        """Initialize json file format
        Args:
            json_type (str): 'object' for one object, 'multiline' for objects each lines, 'array' for array
            use_node (str): if not empty, under the node name will be used. for example 'data'
        """
        self.data = None
        self.json_type = json_type
        self.encoding = encoding
        self.use_key = use_key

    # 내부 함수
    def append_json_data(self, json_obj):
        if not self.use_key:
            self.data.append(json_obj)
        elif self.use_key in json_obj:
            self.data.append(json_obj[self.use_key])
        else:
            self.error_data.append(json_obj)

    def set_json_data(self, json_obj):
        if self.json_type == 'object':
            if not self.use_key:
                self.data = json_obj
            elif self.use_key in json_obj:
                self.data = json_obj[self.use_key]
            else:
                self.error_data.append(json_obj)

        elif self.json_type == 'array':
            if not self.use_key:
                self.data = json_obj
            elif isinstance(json_obj, list):
                self.data = [e for e in json_obj if self.use_key in e]
            else:
                self.error_data = json_obj

    def load(self, from_stream):
        """
        Args:
            from_stream (file-like object): file or s3 stream object which support .read() function
            encoding (str): only need in multiline file load,
            **kwargs ():

        Returns:
            json object (dict) if json_type is 'object'
            array of dict if json_type is 'array' or 'multiline'
        """
        if self.json_type == 'multiline':
            self.data = []
            self.error_data = []
            if isinstance(from_stream, io.TextIOWrapper):
                for i, line_str in enumerate(from_stream):
                    try:
                        json_obj = json.loads(line_str)
                        self.append_json_data(json_obj)
                    except Exception as e:
                        self.error_data.append(line_str)
                        logger.exception(e)
            elif isinstance(from_stream, io.BytesIO):
                try:
                    lines = from_stream.splitlines()
                except Exception as e:
                    logger.exception(e)
                    return None

                for i, line_bytes in enumerate(lines):
                    try:
                        line_str = line_bytes.decode(self.encoding)
                        json_obj = json.loads(line_str)
                        self.append_json_data(json_obj)
                    except Exception as e:
                        self.error_data.append(line_str)
                        logger.exception(e)
        elif self.json_type == 'object':
            self.data = None
            self.error_data = None
            try:
                root_json = json.load(from_stream)
                if type(root_json) == list:
                    logger.error(f"given json type 'object' but is 'array'")
                    self.error_data = root_json
                else:
                    self.set_json_data(root_json)
            except Exception as e:
                logger.exception(e)
        elif self.json_type == 'array':
            self.data = []
            self.error_data = None
            try:
                root_json = json.load(from_stream)
                if type(root_json) != list:
                    logger.error(f"given json type 'array' but is 'object'")
                    self.error_data = root_json
                else:
                    self.set_json_data(root_json)
            except Exception as e:
                logger.exception(e)
        return self.data

    def loads(self, from_string):
        if self.json_type == 'multiline':
            self.data = []
            self.error_data = []
            for i, line_str in enumerate(from_string):
                try:
                    json_obj = json.loads(line_str)
                    self.append_json_data(json_obj)
                except Exception as e:
                    self.error_data.append(line_str)
                    logger.exception(e)
        elif self.json_type == 'object':
            self.data = None
            self.error_data = None
            try:
                root_json = json.loads(from_string)
                if type(root_json) == list:
                    logger.error(f"given json type 'object' but is 'array'")
                    self.error_data = root_json
                else:
                    self.set_json_data(root_json)
            except Exception as e:
                logger.exception(e)
        elif self.json_type == 'array':
            self.data = []
            self.error_data = None
            try:
                root_json = json.loads(from_string)
                if type(root_json) != list:
                    logger.error(f"given json type 'array' but is 'object'")
                    self.error_data = root_json
                else:
                    self.set_json_data(root_json)
            except Exception as e:
                logger.exception(e)
        return self.data

    def get(self, xpath):
        return self.data.get(xpath)

    def update(self, xpath, new_data):
        self.data[xpath] = new_data

    def dump(self, to_stream):
        json.dump(self.data, to_stream)

    def dumps(self):
        return json.dumps(self.data)
