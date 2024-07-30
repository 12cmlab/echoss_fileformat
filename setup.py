from setuptools import setup, find_packages

setup(
    name="echoss_fileformat",
    version="1.1.5",
    author="12cm",
    author_email="your.email@12cm.com",
    description="File format handler packages for JSON, CSV, XML, and Excel files",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url="",
    packages=find_packages(),
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules"
    ],
    python_requires='>=3.8',
    install_requires=[
        "setuptools>=67.4.0",
        "pandas>=1.5.3",
        "numpy>=1.22.3",
        "pyarrow<16,>=4.0.0",
        "openpyxl>=3.1.0",
        "xlrd>=1.2.0",
        "wcwidth>=0.2.13",
        "lxml>=5.0.1"
    ]
)
