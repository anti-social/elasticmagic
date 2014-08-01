from elasticmagic import SearchQuery, Params, Term, Sort, agg
from elasticmagic.expression import Fields

from .base import BaseTestCase


class SearchQueryTest(BaseTestCase):
    def test(self):
        f = Fields()

        self.assert_expression(
            SearchQuery(),
            {
            }
        )

        self.assert_expression(
            SearchQuery(Term(f.user, 'kimchy')).limit(10).offset(0),
            {
                "from": 0,
                "size": 10,
                "query": {
                    "term": {"user": "kimchy"}
                }
            }
        )

        self.assert_expression(
            SearchQuery(Term(f.user, 'kimchy')).filter(f.age >= 16),
            {
                "query": {
                    "filtered": {
                        "query": {
                            "term": {"user": "kimchy"}
                        },
                        "filter": {
                            "range": {
                                "age": {"gte": 16}
                            }
                        }
                    }
                }
            }
        )

        self.assert_expression(
            SearchQuery(None).order_by(
                f.opinion_rating.desc(missing='_last'),
                f.opinion_count.desc(),
                f.id
            ),
            {
                "sort": [
                    {
                        "opinion_rating": {
                            "order": "desc",
                            "missing": "_last"
                        }
                    },
                    {
                        "opinion_count": "desc"
                    },
                    "id"
                ]
            }
        )

        self.assert_expression(
            SearchQuery().fields(f.name, f.company),
            {
                "_source": ["name", "company"]
            }
        )

    def test_aggregations(self):
        f = Fields()

        self.assert_expression(
            SearchQuery().aggregation(min_price=agg.Min(f.price)),
            {
                "aggregations": {
                    "min_price": {
                        "min": {"field": "price"}
                    }
                }
            }
        )

        self.assert_expression(
            SearchQuery().aggregation(genders=agg.Terms(f.gender)),
            {
                "aggregations": {
                    "genders": {
                        "terms": {"field": "gender"}
                    }
                }
            }
        )

        self.assert_expression(
            SearchQuery().aggregation(type=agg.Terms(f.type).aggs(min_price=agg.Min(f.price))),
            {
                "aggregations": {
                    "type": {
                        "terms": {"field": "type"},
                        "aggregations": {
                            "min_price": {
                                "min": {"field": "price"}
                            }
                        }
                    }
                }
            }
        )

        self.assert_expression(
            SearchQuery().aggregation(
                top_tags=(
                    agg.Terms(f.tags, size=3)
                    .aggs(top_tag_hits=agg.TopHits(sort=f.last_activity_date.desc(),
                                                   size=1,
                                                   _source=Params(include=[f.title])))
                )
            ),
            {
                "aggregations": {
                    "top_tags": {
                        "terms": {
                            "field": "tags",
                            "size": 3
                        },
                        "aggregations": {
                            "top_tag_hits": {
                                "top_hits": {
                                    "sort": {
                                        "last_activity_date": "desc"
                                    },
                                    "_source": {
                                        "include": "title"
                                    },
                                    "size" : 1
                                }
                            }
                        }
                    }
                }
            }  
        )
        self.assert_expression(
            SearchQuery().aggregation(
                top_sites=(
                    agg.Terms(f.domain, order=Sort('top_hit', 'desc'))
                    .aggs(top_tags_hits=agg.TopHits(),
                          top_hit=agg.Max(script='_doc.score'))
                )
            ),
            {
                "aggregations": {
                    "top_sites": {
                        "terms": {
                            "field": "domain",
                            "order": {
                                "top_hit": "desc"
                            }
                        },
                        "aggregations": {
                            "top_tags_hits": {
                                "top_hits": {}
                            },
                            "top_hit" : {
                                "max": {
                                    "script": "_doc.score"
                                }
                            }
                        }
                    }
                }
            }
        )
