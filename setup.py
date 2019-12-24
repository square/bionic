#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

import os
from runpy import run_path

# This appears to be the least annoying Python-version-agnostic way of loading
# an external file.
extras_require = run_path(os.path.join(
    os.path.dirname(__file__), 'bionic', 'extras.py'))['extras_require']

with open('README.md') as readme_file:
    readme = readme_file.read()

requirements = [
    'PyYAML',
    'numpy',
    'pandas',
    'pyarrow',
    'pyrsistent',
]

setup(
    name='bionic',
    version='0.6.3',
    description=(
        'A Python framework for building, running, and sharing data science '
        'workflows'),
    long_description=readme,
    long_description_content_type="text/markdown",
    license='Apache License 2.0',
    author='Janek Klawe',
    author_email='janek@squareup.com',
    url='https://github.com/square/bionic',
    packages=find_packages(),
    include_package_data=True,
    install_requires=requirements,
    extras_require=extras_require,
    python_requires='>=3.6',
    zip_safe=False,
    keywords='bionic',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],
)
