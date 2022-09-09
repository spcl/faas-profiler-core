#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FaaS-Profiler Core
"""

import setuptools

with open("requirements.txt", "r") as fh:
    requirements = fh.read().splitlines()

setuptools.setup(
    name='faas-profiler-core',
    version='0.2.17',
    url='https://github.com/spcl/faas-profiler-core',
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=requirements)