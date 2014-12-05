from .util import clean_params
from .result import Result


def multi_search(client, queries, params):
    params = clean_params(params)
    body = []
    for q in queries:
        query_header = {}
        if q._index:
            query_header['index'] = q._index._name
        doc_type = q._get_doc_type()
        if doc_type:
            query_header['doc_type'] = doc_type
        if q._routing:
            query_header['routing'] = q._routing
        if q._search_type:
            query_header['search_type'] = q._search_type
        body += [query_header, q.to_dict()]

    raw_results = client.msearch(body=body, **params)['responses']
    results = []
    for raw, q in zip(raw_results, queries):
        print raw
        results.append(
            Result(raw, q._aggregations,
                   doc_cls=q._get_doc_cls(),
                   instance_mapper=q._instance_mapper)
        )
    return results


def bulk(client, actions, params):
    body = []
    for act in actions:
        body.append(act.get_meta())
        source = act.get_source()
        if source is not None:
            body.append(source)
    return client.bulk(body=body, **clean_params(params))
