import os
from setuptools import setup, find_packages


def get_version():
    with open(os.path.join('elasticmagic', 'version.py'), 'rt') as version_file:
        return version_file.readline().split('=')[1].strip().strip("'\"")

def parse_requirements(req_file_path):
    with open(os.path.join('requirements', req_file_path)) as req_file:
        return req_file.read().splitlines()

setup(
    name="elasticmagic",
    version=get_version(),
    author="Alexander Koval",
    author_email="kovalidis@gmail.com",
    description=("Python orm for elasticsearch."),
    license="Apache License 2.0",
    keywords="elasticsearch dsl",
    url="https://github.com/anti-social/elasticmagic",
    packages=find_packages(exclude=["tests"]),
    install_requires=parse_requirements("base.txt"),
    tests_require=parse_requirements("test.txt"),
    extras_require={
        "geo": parse_requirements("geo.txt"),
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Internet :: WWW/HTTP :: Indexing/Search",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
