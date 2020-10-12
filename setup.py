from os import path
from setuptools import find_packages, setup

from pathlib import Path


def strip_comments(l):
    return l.split('#', 1)[0].strip()


def _pip_requirement(req, *root):
    if req.startswith('-r '):
        _, path = req.split()
        return reqs(*root, *path.split('/'))
    return [req]


def _reqs(*f):
    path = (Path.cwd() / 'requirements').joinpath(*f)
    with path.open() as fh:
        reqs = [strip_comments(l) for l in fh.readlines()]
        return [_pip_requirement(r, *f[:-1]) for r in reqs if r]


def reqs(*f):
    return [req for subreq in _reqs(*f) for req in subreq]


# read the contents of your README file
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()


setup(
    name='treeschema',
    version='1.0.3',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    description='A python client that simplifies interactions with the Tree Schema REST API.',
    url='https://github.com/tree-schema/tree-schema-python',
    author='Tree Schema',
    author_email='info@treeschema.com',
    packages=find_packages(),
    zip_safe=False,
    install_requires=reqs('base.txt'),
    tests_require=reqs('test.txt'),
    extras_require={
        'test': reqs('test.txt')
    },
    project_urls={
        'Bug Reports': 'https://github.com/tree-schema/tree-schema-python/issues',
        'Source': 'https://github.com/tree-schema/tree-schema-python',
        'Documentation': 'https://developer.treeschema.com/python-client',
    },
    keywords=[
        'data lineage',
        'metadata'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ],
)