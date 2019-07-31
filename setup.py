#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

requirements = [
    'Pillow',
    'PyYAML',
    'dill',
    'future',
    'google-cloud-storage',
    'hsluv',
    'ipython',
    'jupyter',
    'matplotlib',
    'networkx',
    'pandas',
    'pathlib2',
    'pyarrow',
    'pydot',
    'pyrsistent',
    'scikit-learn',
    'six',
]

extras_require = {
    'dev': [
        'pytest', 'flake8',
        'sphinx', 'sphinx_rtd_theme', 'sphinx-autobuild', 'nbsphinx',
        'bumpversion',
    ]
}

setup(
    name='bionic',
    version='0.3.1',
    description='prototype',
    long_description=readme,
    author='',
    author_email='',
    packages=find_packages(),
    include_package_data=True,
    install_requires=requirements,
    extras_require=extras_require,
    zip_safe=False,
    keywords='bionic',
    classifiers=[
        'Development Status :: 2 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
    ],
)
