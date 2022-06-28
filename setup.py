from setuptools import setup, find_packages


setup(
    name="elasticmagic",
    version="0.0.0a0",
    author="Alexander Koval",
    author_email="kovalidis@gmail.com",
    description=("Python orm for elasticsearch."),
    license="Apache License 2.0",
    keywords="elasticsearch dsl",
    url="https://github.com/anti-social/elasticmagic",
    packages=find_packages(exclude=["tests"]),
    install_requires=[
        "elasticsearch>=6.0.0,<8.0",
        "python-dateutil",
    ],
    extras_require={
        "geo": [
            "python-geohash",
        ],
        "async": [
            "elasticsearch-py-async",
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
