import io

from abstract_fileformat_handler import AbstractFileFormatHandler
import json
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger('echoss_fileformat')


class JsonHandler(AbstractFileFormatHandler):
    TYPE_OBJECT = 'object'
    TYPE_ARRAY = 'array'
    TYPE_MULTILINE = 'multiline'

    # 지원하는 추가 키워드
    KW_DICT = {
        'load': {

        },
        'dump': {

        }
    }

    def __get_kw_dict(self):
        return JsonHandler.KW_DICT
    
    def __init__(self, json_type=TYPE_OBJECT, encoding='utf-8', use_key=''):
        """Initialize json file format
        Args:
            json_type (str): 'object' for one object, 'multiline' for objects each lines, 'array' for array
            use_key (str): if not empty, under the node name will be used. for example 'data'
        """
        self.data = None
        self.error_data = None
        self.json_type = json_type
        self.encoding = encoding
        self.use_key = use_key
        self.data_df = None
        self.data_dirty = False

    # 내부 함수 for multiline json_type
    def append_json_data(self, json_obj):
        if not self.use_key:
            self.data.append(json_obj)
        elif self.use_key in json_obj:
            self.data.append(json_obj[self.use_key])
        else:
            self.error_data.append(json_obj)

    # 내부 함수 for object and array json_type
    def set_json_data(self, json_obj):
        if self.json_type == JsonHandler.TYPE_OBJECT:
            if not self.use_key:
                self.data = json_obj
            elif self.use_key in json_obj:
                self.data = json_obj[self.use_key]
            else:
                self.error_data.append(json_obj)

        elif self.json_type == JsonHandler.TYPE_ARRAY:
            if not self.use_key:
                self.data = json_obj
            elif isinstance(json_obj, list):
                self.data = [e for e in json_obj if self.use_key in e]
            else:
                self.error_data = json_obj

    def load(self, file_or_filename):
        """
        Args:
            file_or_filename (file-like object): file or s3 stream object which support .read() function

        Returns:
            json object (dict) if json_type is 'object'
            array of dict if json_type is 'array' or 'multiline'
        """
        if self.json_type == JsonHandler.TYPE_MULTILINE:
            self.data = []
            self.error_data = []
            if isinstance(file_or_filename, io.TextIOWrapper):
                for i, line_str in enumerate(file_or_filename):
                    try:
                        json_obj = json.loads(line_str)
                        self.append_json_data(json_obj)
                    except Exception as e:
                        self.error_data.append(line_str)
                        logger.exception(e)
            elif isinstance(file_or_filename, io.BytesIO):
                try:
                    lines = file_or_filename.splitlines()
                except Exception as e:
                    logger.exception(e)
                    return None

                for i, line_bytes in enumerate(lines):
                    try:
                        line_str = line_bytes.decode(self.encoding)
                        json_obj = json.loads(line_str)
                        self.append_json_data(json_obj)
                    except Exception as e:
                        self.error_data.append(line_bytes)
                        logger.exception(e)
        elif self.json_type == JsonHandler.TYPE_OBJECT:
            self.data = None
            self.error_data = None
            try:
                root_json = json.load(file_or_filename)
                if type(root_json) == list:
                    logger.error(f"given json type {JsonHandler.TYPE_OBJECT} but is {JsonHandler.TYPE_ARRAY}")
                    self.error_data = root_json
                else:
                    self.set_json_data(root_json)
            except Exception as e:
                logger.exception(e)
        elif self.json_type == JsonHandler.TYPE_ARRAY:
            self.data = []
            self.error_data = None
            try:
                root_json = json.load(file_or_filename)
                if type(root_json) != list:
                    logger.error(f"given json type {JsonHandler.TYPE_ARRAY} but is {JsonHandler.TYPE_OBJECT}")
                    self.error_data = root_json
                else:
                    self.set_json_data(root_json)
            except Exception as e:
                logger.exception(e)
        return self.data

    def loads(self, str_or_bytes):
        if self.json_type == JsonHandler.TYPE_MULTILINE:
            self.data = []
            self.error_data = []
            for i, line_str in enumerate(str_or_bytes):
                try:
                    json_obj = json.loads(line_str)
                    self.append_json_data(json_obj)
                except Exception as e:
                    self.error_data.append(line_str)
                    logger.exception(e)
        elif self.json_type == JsonHandler.TYPE_OBJECT:
            self.data = None
            self.error_data = None
            try:
                root_json = json.loads(str_or_bytes)
                if type(root_json) == list:
                    logger.error(f"given json type {JsonHandler.TYPE_OBJECT} but is {JsonHandler.TYPE_ARRAY}")
                    self.error_data = root_json
                else:
                    self.set_json_data(root_json)
            except Exception as e:
                logger.exception(e)
        elif self.json_type == JsonHandler.TYPE_ARRAY:
            self.data = []
            self.error_data = None
            try:
                root_json = json.loads(str_or_bytes)
                if type(root_json) != list:
                    logger.error(f"given json type {JsonHandler.TYPE_ARRAY} but is {JsonHandler.TYPE_OBJECT}")
                    self.error_data = root_json
                else:
                    self.set_json_data(root_json)
            except Exception as e:
                logger.exception(e)
        return self.data

    def get_tree_path(self, tree_path):
        path_keys = [p for p in tree_path.split('/') if p]
        if self.data:
            json_obj = self.data
            if JsonHandler.TYPE_OBJECT == self.json_type:
                for key in path_keys:
                    if key in json_obj:
                        json_obj = json_obj[key]
                    else:
                        return None
                return json_obj
            elif JsonHandler.TYPE_ARRAY == self.json_type or JsonHandler.TYPE_MULTILINE == self.json_type:
                results = []
                json_list = self.data
                for a in json_list:
                    json_obj = a
                    for key in path_keys:
                        if key in json_obj:
                            json_obj = json_obj[key]
                        else:
                            json_obj = None
                            break
                    if json_obj:
                        results.append(json_obj)
                return results
        else:
            logger.info(f"data is not exist")

        if JsonHandler.TYPE_OBJECT == self.json_type:
            return None
        else:
            return []

    def set_tree_path(self, tree_path, new_data):
        set_count = 0
        path_keys = [p for p in tree_path.split('/') if p]
        if len(path_keys) == 0:
            self.data = new_data
            set_count += 1
        elif self.data:
            dict_obj = None
            json_obj = self.data
            if JsonHandler.TYPE_OBJECT == self.json_type:
                for key in path_keys:
                    if key in json_obj:
                        dict_obj = json_obj
                        json_obj = json_obj[key]
                    else:
                        dict_obj = None
                        break
                if dict_obj:
                    dict_obj[path_keys[-1]] = new_data
                    set_count += 1
            elif JsonHandler.TYPE_ARRAY == self.json_type or JsonHandler.TYPE_MULTILINE == self.json_type:
                json_list = self.data
                for a in json_list:
                    dict_obj = None
                    json_obj = a
                    for key in path_keys:
                        if key in json_obj:
                            dict_obj = json_obj
                            json_obj = json_obj[key]
                        else:
                            dict_obj = None
                            break
                    if dict_obj:
                        dict_obj[path_keys[-1]] = new_data
                        set_count += 1
        else:
            logger.info(f"data is not exist")
        if set_count > 0:
            self.data_dirty = True
            logger.info(f"set_tree_path modify [{set_count}] data")

    def dump(self, file_or_filename):
        if self.data:
            json.dump(self.data, file_or_filename)

    def dumps(self):
        if self.data:
            return json.dumps(self.data)

    # 'array' 나 'multiline' 유형으로 모두 같은 key:value 형태로 되어 있다고 가정
    def to_pandas(self):
        self.data_df = pd.DataFrame.from_dict(self.data)
        return self.df

    def to_csv(self, filename):
        if self.data:
            if self.data_dirty or not self.data_df:
                self.to_pandas()
                self.data_dirty = False

            if self.data_df:
                self.data_df.to_csv(filename, encoding=self.encoding, index=False)







