#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

requirements = [
    'six',
    'pyrsistent',
    'pathlib2',
    'PyYAML',
    'dill',
    'Pillow',
    'pyarrow',
    'pandas',
    'ipython',
    'sklearn',
    'matplotlib',
    'future',
    'jupyter',
    'jupyterlab',
    'ipdb',
    'Cython',
    'networkx',
    'pydot',
    'hsluv',
]

extras_require = {
    'dev': ['pytest', 'flake8']
}

setup(
    name='workflows',
    version='0.1.0',
    description='prototype',
    long_description=readme,
    author='',
    author_email='',
    packages=find_packages(),
    include_package_data=True,
    install_requires=requirements,
    extras_require=extras_require,
    zip_safe=False,
    keywords='workflows',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ],
)
