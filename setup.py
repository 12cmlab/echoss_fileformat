from setuptools import setup

setup(
            name="echoss_fileformat",
    version="1.1.0",
    author="12cm",
    author_email="your.email@12cm.com",
    description="File format handler packages for JSON, CSV, XML, and Excel files.",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url="",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules"
    ],
    python_requires='>=3.6',
    install_requires=[
        "setuptools>=67.4.0",
        "pandas>=1.5.3",
        "numpy>=1.22.3",
        "openpyxl>=3.1.0",
        "wcwidth>=0.2.13",
        "lxml>=5.0.1"
    ]
)
