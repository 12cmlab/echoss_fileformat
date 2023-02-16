from .fileformat_handler import FileformatHandler
import xml.etree.ElementTree as ET
import io
import pandas as pd


class XmlHandler(FileformatHandler):
    # 지원하는 추가 키워드
    KW_DICT = {
        'load': {

        },
        'dump': {

        }
    }

    def __get_kw_dict(self) -> dict:
        return XmlHandler.KW_DICT

    def __init__(self):
        self.tree = None

    def load(self, file_or_filename):
        self.tree = ET.parse(file_or_filename)

    def loads(self, str_or_bytes):
        self.tree = ET.fromstring(str_or_bytes)

    def get_tree_path(self, xpath):
        for element in self.tree.iter():
            if element.text == xpath:
                return element
        return None

    def set_tree_path(self, xpath, new_data):
        element = self.get_tree_path(xpath)
        if element is not None:
            element.text = new_data
        else:
            new_element = ET.Element(xpath)
            new_element.text = new_data
            self.tree.getroot().append(new_element)

    def dump(self, file_or_filename):
        self.tree.write(file_or_filename)

    def dumps(self):
        return ET.tostring(self.tree.getroot(), encoding="unicode")
