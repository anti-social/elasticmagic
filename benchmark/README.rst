Benchmarking
============

This tool is intended to find bottlenecks in this library.

Script has two modes -- generate sample data & process sample data;

Generating sample data
----------------------

To generate data use following command:

.. code-block:: bash

   python benchmark/run.py sample -o sample.json

See ``--help`` for available options.

Usefull option ``-s/--size`` -- sets sample data magnitude, default is 2 (100 docs)

Processing sample data
----------------------

Run:

.. code-block:: bash

   python benchmark/run.py run simple -i sample.json


Some results
------------

``searchResult`` instantiation time, ms:

+---------+----------+-----------+-----------+
|         | -s 2     | -s 3      | -s 4      |
+---------+----------+-----------+-----------+
| -t hits | 2.862    | 22.314    | 224.592   |
+---------+----------+-----------+-----------+
| -t aggs | 0.934    | 4.032     | 33.442    |
+---------+----------+-----------+-----------+
| -t all  | 3.287    | 26.199    | 253.697   |
+---------+----------+-----------+-----------+

Command run:

.. code-block:: bash

   $ python benchmark/run.py sample -s S -t T | python benchmark/run.py run simple
