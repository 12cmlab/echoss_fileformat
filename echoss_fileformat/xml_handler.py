from abstract_fileformat_handler import AbstractFileFormatHandler
import xml.etree.ElementTree as ET
import io
import pandas as pd


class XmlHandler(AbstractFileFormatHandler):
    def __init__(self):
        self.tree = None

    def load(self, from_stream):
        self.tree = ET.parse(from_stream)

    def loads(self, from_string):
        self.tree = ET.fromstring(from_string)

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

    def dump(self, to_stream):
        self.tree.write(to_stream)

    def dumps(self):
        return ET.tostring(self.tree.getroot(), encoding="unicode")
