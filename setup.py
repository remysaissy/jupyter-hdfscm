#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function

import io
import re
from glob import glob
from os.path import basename
from os.path import dirname
from os.path import join
from os.path import splitext

from setuptools import find_packages
from setuptools import setup


def read(*names, **kwargs):
    with io.open(
        join(dirname(__file__), *names),
        encoding=kwargs.get('encoding', 'utf8')
    ) as fh:
        return fh.read()


setup(
    name='jupyter-omnicm',
    version='0.0.3',
    license='Apache Software License 2.0',
    description='jupyter-omnicm is a flexible content manager system for Jupyter notebooks.',
    long_description='%s\n%s' % (
        re.compile('^.. start-badges.*^.. end-badges', re.M | re.S).sub('', read('README.rst')),
        re.sub(':[a-z]+:`~?(.*?)`', r'``\1``', read('CHANGELOG.rst'))
    ),
    author='Rémy Saissy',
    author_email='remy.saissy@gmail.com',
    url='https://github.com/remysaissy/jupyter-omnicm',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    py_modules=[splitext(basename(path))[0] for path in glob('src/*.py')],
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        # complete classifier list: http://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: Unix',
        'Operating System :: POSIX',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Utilities',
    ],
    project_urls={
        'Documentation': 'https://jupyter-omnicm.readthedocs.io/',
        'Changelog': 'https://jupyter-omnicm.readthedocs.io/en/latest/changelog.html',
        'Issue Tracker': 'https://github.com/remysaissy/jupyter-omnicm/issues',
    },
    keywords=[
        # eg: 'keyword1', 'keyword2', 'keyword3',
    ],
    python_requires='>=3.6',
    install_requires=[
        'tornado', 'traitlets', 'notebook', 'ipython_genutils', 'nbformat',
        'pyarrow'  # HDFS Support
    ]
)
