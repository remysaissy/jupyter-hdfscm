========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |docs|
    * - tests
      - | |travis| |appveyor| |requires|
        | |codecov|
    * - package
      - | |version| |wheel| |supported-versions| |supported-implementations|
        | |commits-since|
.. |docs| image:: https://readthedocs.org/projects/jupyter-omnicm/badge/?style=flat
    :target: https://readthedocs.org/projects/jupyter-omnicm
    :alt: Documentation Status

.. |travis| image:: https://travis-ci.org/remysaissy/jupyter-omnicm.svg?branch=master
    :alt: Travis-CI Build Status
    :target: https://travis-ci.org/remysaissy/jupyter-omnicm

.. |appveyor| image:: https://ci.appveyor.com/api/projects/status/github/remysaissy/jupyter-omnicm?branch=master&svg=true
    :alt: AppVeyor Build Status
    :target: https://ci.appveyor.com/project/remysaissy/jupyter-omnicm

.. |requires| image:: https://requires.io/github/remysaissy/jupyter-omnicm/requirements.svg?branch=master
    :alt: Requirements Status
    :target: https://requires.io/github/remysaissy/jupyter-omnicm/requirements/?branch=master

.. |codecov| image:: https://codecov.io/github/remysaissy/jupyter-omnicm/coverage.svg?branch=master
    :alt: Coverage Status
    :target: https://codecov.io/github/remysaissy/jupyter-omnicm

.. |version| image:: https://img.shields.io/pypi/v/jupyter-omnicm.svg
    :alt: PyPI Package latest release
    :target: https://pypi.org/project/jupyter-omnicm

.. |commits-since| image:: https://img.shields.io/github/commits-since/remysaissy/jupyter-omnicm/v0.0.0.svg
    :alt: Commits since latest release
    :target: https://github.com/remysaissy/jupyter-omnicm/compare/v0.0.0...master

.. |wheel| image:: https://img.shields.io/pypi/wheel/jupyter-omnicm.svg
    :alt: PyPI Wheel
    :target: https://pypi.org/project/jupyter-omnicm

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/jupyter-omnicm.svg
    :alt: Supported versions
    :target: https://pypi.org/project/jupyter-omnicm

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/jupyter-omnicm.svg
    :alt: Supported implementations
    :target: https://pypi.org/project/jupyter-omnicm


.. end-badges

jupyter-omnicm is a flexible content manager system for Jupyter notebooks.

* Free software: Apache Software License 2.0

It currently supports the HDFS Content Manager.

Installation
============

::

    pip install jupyter-omnicm

Documentation
=============


https://jupyter-omnicm.readthedocs.io/


Development
===========

To run the all tests run::

    tox

Note, to combine the coverage data from all the tox environments run:

.. list-table::
    :widths: 10 90
    :stub-columns: 1

    - - Windows
      - ::

            set PYTEST_ADDOPTS=--cov-append
            tox

    - - Other
      - ::

            PYTEST_ADDOPTS=--cov-append tox
