#!/usr/bin/env python

from os import path, walk
import io
import sys
from setuptools import setup, find_packages

with io.open('README.pypi', 'r', encoding='utf-8') as f:
    ABOUT = f.read()

NAME = "Orange3-WorldHappiness"

VERSION = "0.1.10"

AUTHOR = 'Bioinformatics Laboratory, FRI UL'
AUTHOR_EMAIL = 'contact@orange.biolab.si'

URL = 'http://orange.biolab.si/download'
DESCRIPTION = "Orange3 add-on for retrieving socioeconomic data"
LONG_DESCRIPTION = ABOUT
LICENSE = "GPL3+"

CLASSIFIERS = [
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Education',
    'Intended Audience :: Science/Research',
    'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
    'Programming Language :: Python :: 3 :: Only'
]

KEYWORDS = [
    'orange3 add-on',
    'orange3-world-happiness'
]

PACKAGES = find_packages()

PACKAGE_DATA = {
    'orangecontrib.worldhappiness.widgets': ['icons/*'],
}

DATA_FILES = [
    # Data files that will be installed outside site-packages folder
]

INSTALL_REQUIRES = [
    'Orange3>=3.31',
    'pandas',
    'pymongo',
    'wbgapi',
    'dnspython'
]

ENTRY_POINTS = {
    'orange3.addon':
        ('Orange3-WorldHappiness = orangecontrib.worldhappiness',),
    'orange.widgets':
        ('World Happiness = orangecontrib.worldhappiness.widgets',),
    "orange.canvas.help":
        ('html-index = orangecontrib.worldhappiness.widgets:WIDGET_HELP_PATH',)
}

NAMESPACE_PACKAGES = ["orangecontrib"]


def include_documentation(local_dir, install_dir):
    global DATA_FILES

    doc_files = []
    for dirpath, _, files in walk(local_dir):
        doc_files.append(
            (
                dirpath.replace(local_dir, install_dir),
                [path.join(dirpath, f) for f in files],
            )
        )
    DATA_FILES.extend(doc_files)


if __name__ == '__main__':
    include_documentation('doc/_build/html', 'help/orange3-worldhappiness')
    setup(
        name=NAME,
        version=VERSION,
        author=AUTHOR,
        author_email=AUTHOR_EMAIL,
        url=URL,
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        long_description_content_type='text/markdown',
        license=LICENSE,
        packages=PACKAGES,
        package_data=PACKAGE_DATA,
        data_files=DATA_FILES,
        install_requires=INSTALL_REQUIRES,
        entry_points=ENTRY_POINTS,
        keywords=KEYWORDS,
        classifiers=CLASSIFIERS,
        namespace_packages=NAMESPACE_PACKAGES,
        zip_safe=False,
        extras_require={
            'test': ['coverage'],
            'doc': ['sphinx', 'recommonmark', 'sphinx_rtd_theme', 'docutils'],
        },
    )
