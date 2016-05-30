# Benchmark result processing;
import sys
import argparse
import inspect
import json
import time
import cProfile
import gc
import coverage

from collections import OrderedDict

from elasticmagic import (
    Document, Field,
    SearchQuery,
    MatchAll,
    )
from elasticmagic.result import SearchResult
from elasticmagic.types import (
    Boolean, Integer, Float, String, Date,
    List,
    )
from elasticmagic.agg import Terms


def setup():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(help='Valid commands')
    for command, setup, handler in [('sample', sample_setup, gen_sample),
                                    ('run', run_setup, run)]:
        sub_ap = sub.add_parser(command, help=handler.__doc__)
        sub_ap.set_defaults(action=handler)
        setup(sub_ap)
    return ap


def sample_setup(ap):
    ap.add_argument('-p', '--pretty', dest='indent',
                    action='store_const', const=2,
                    default=None,
                    help="generate indented JSON")
    ap.add_argument('-o', '--output', dest='output',
                    type=argparse.FileType('w'), default=sys.stdout,
                    help="Output file")
    ap.add_argument('-s', '--size', dest='size',
                    type=lambda x: 10**int(x),
                    default=1000,
                    help="Population size, power of 10, default: 3")
    ap.add_argument('-t', '--type', dest='type',
                    choices=['all', 'hits', 'aggs'],
                    default='all',
                    help="Fields to generate.")


def run_setup(ap):
    ap.add_argument('test', choices=['simple'], help="Test to run")
    ap.add_argument('-i', '--input', dest='input',
                    type=argparse.FileType('r'), default=sys.stdin,
                    help="Input file")
    ap.add_argument('-p', '--profile', dest='profile',
                    action='store_true', default=False)


def main():
    ap = setup()
    options = ap.parse_args()
    if not hasattr(options, 'action'):
        ap.print_help()
        return
    return options.action(options)


# Actions


def gen_sample(options):
    """Generate sample population."""

    def unwrap_gen(obj):
        if inspect.isgenerator(obj):
            return list(obj)
        return obj

    def write(item, *items):
        obj = OrderedDict(filter(None, (item,) + items))
        json.dump(obj,
                  options.output,
                  ensure_ascii=False,
                  default=unwrap_gen,
                  indent=options.indent)

    def hits_gen():
        return OrderedDict((
            ('total', options.size),
            ('max_score', 0),
            ('hits', gen_simple_document(options.size)),
            ))

    def aggs_gen():
        return {
            "terms": {
                "doc_count_error_upper_bound": 0,
                "sum_other_doc_count": options.size,
                "buckets": gen_terms_buckets(options.size),
                },
            }

    hits = options.type in ['all', 'hits']
    aggs = options.type in ['all', 'aggs']

    dumb_hits = {'total': options.size, 'hits': [], 'max_score': 0}

    write(("took", 0),
          ("timed_out", False),
          ("_shards", {
              "total": 1,
              "successful": 1,
              "failed": 0,
              }),
          ("hits", hits_gen()) if hits else ("hits", dumb_hits),
          ("aggregations", aggs_gen()) if aggs else (),
          )


def run(options):
    """Run benchmark."""
    prof = cProfile.Profile()
    cov = coverage.Coverage()

    times = OrderedDict.fromkeys(['data_load', 'json_loads', 'searchResult'])
    start = time.monotonic() * 1000

    raw_data = options.input.read()
    times['data_load'] = time.monotonic() * 1000 - start
    start = time.monotonic() * 1000

    raw_results = json.loads(raw_data)
    times['json_loads'] = time.monotonic() * 1000 - start

    query = SearchQuery(MatchAll(),
                        doc_cls=SimpleDocument)
    if 'aggregations' in raw_results:
        query = query.aggs(terms=Terms(SimpleDocument.integer_0))
    gc.disable()
    if options.profile:
        cov.start()
        prof.enable()

    SearchResult(
        raw_results,
        query._aggregations,
        doc_cls=query._get_doc_cls(),
        instance_mapper=query._instance_mapper)
    times['searchResult'] = time.monotonic() * 1000 - start
    if options.profile:
        prof.disable()
        cov.stop()
        gc.enable()

    for key, duration in times.items():
        print("Took {} {:10.3f}ms".format(key, duration))

    if options.profile:
        prof.print_stats('cumulative')
        cov.report()
        cov.html_report()


class SimpleDocument(Document):
    __doc_type__ = 'simple'

    boolean_0 = Field(Boolean)
    integer_0 = Field(Integer)
    float_0 = Field(Float)
    string_0 = Field(String)
    date_0 = Field(Date)

    boolean_1 = Field(Boolean)
    integer_1 = Field(Integer)
    float_1 = Field(Float)
    string_1 = Field(String)
    date_1 = Field(Date)


class ListsDocument(Document):
    __doc_type__ = 'lists'

    boolean_0 = Field(List(Boolean))
    integer_0 = Field(List(Integer))
    float_0 = Field(List(Float))
    string_0 = Field(List(String))
    date_0 = Field(List(Date))

    boolean_1 = Field(List(Boolean))
    integer_1 = Field(List(Integer))
    float_1 = Field(List(Float))
    string_1 = Field(List(String))
    date_1 = Field(List(Date))


_INDEX = 'test'


def gen_simple_document(N):
    for i in range(N):
        yield {
            '_index': _INDEX,
            '_type': 'simple',
            '_id': i,
            '_source': {
                'boolean_0': bool(i % 2),
                'integer_0': i,
                'float_0': i / (10 ** len(str(i))),
                'string_0': str(i),
                'date_0': None,
                'boolean_1': bool(1 + i % 2),
                'integer_1': -i,
                'float_1': i / (10 ** len(str(i))),
                'string_1': str(i),
                'date_1': None,
                }
            }


def gen_terms_buckets(N):
    for i in range(N):
        yield {
            "key": i,
            "doc_count": i,
            }


def gen_lists_document(N):
    K = len(N * .1)
    for i in range(N):
        yield {
            '_index': _INDEX,
            '_type': 'lists',
            '_id': i,
            '_source': {
                'boolean_0': [bool(i*a % 2) for a in range(K)],
                'integer_0': [i] * K,
                'float_0': [(i / (10 ** len(str(i))))] * K,
                'string_0': [str(i)] * K,
                'date_0': None,
                'boolean_1': [bool(1 + i % 2)] * K,
                'integer_1': [-i] * K,
                'float_1': [(i / (10 ** len(str(i))))] * K,
                'string_1': [str(i)] * K,
                'date_1': None,
                }
            }


if __name__ == '__main__':
    main()
