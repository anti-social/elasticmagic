import os
from setuptools import setup, find_packages


def get_version():
    with open(os.path.join('elasticmagic', 'version.py'), 'rt') as version_file:
        return version_file.readline().split('=')[1].strip().strip("'\"")


setup(
    name="elasticmagic",
    version=get_version(),
    author="Alexander Koval",
    author_email="kovalidis@gmail.com",
    description=("Python orm for elasticsearch."),
    license="BSD",
    keywords="solr solar pysolr",
    url="https://github.com/anti-social/elasticmagic",
    packages=find_packages(exclude=["tests"]),
    install_requires=[
        "elasticsearch>=1.2.0,<1.3.0",
        "python-dateutil",
    ],
    tests_require=[
        "nose",
        "mock",
    ],
    extras_require={
        "geo": [
            "python-geohash==0.8.5"
        ],
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
