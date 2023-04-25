from setuptools import setup

setup(
    name='echoss_fileformat',
    version='0.1',
    packages=['echoss_fileformat'],
    url='https://github.com/12cmlab/echoss_fileformat.git',
    license='LGPL',
    author='ckkim',
    author_email='ckkim@12cm.co.kr',
    description='echoss AI Bigdata Solution - File Format Handlers',
    install_requires=[
        'pandas',
        'pyarrow',
        'openpyxl',
        'lxml'
    ],
)
