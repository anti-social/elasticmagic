import unittest

from .base import BaseTestCase


@unittest.skip
class ScriptTest(BaseTestCase):

    def test(self):
        self.assert_expression(
            script.doc[ShopDocument.type].value,
            "doc['type'].value"
        )
        self.assert_expression(
            script.doc[ShopDocument.location].distance(30.5, 51.2),
            "doc['location'].distance(30.5, 51.2)"
        )
        self.assert_expression(
            script.doc[ShopDocument.rank] * 100 + 20,
            "doc['rank'] * 100 + 20"
        )
        self.assert_expression(
            script._fields[ShopDocument.holydays].values,
            "_fields['holidays'].values"
        )
        self.assert_expression(
            script._source.employee.person.name,
            "_source.employee.person.name"
        )
        self.assert_expression(
            script.sin(0),
            "sin(0)"
        )
        self.assert_expression(
            script.sin(script.toRadians(30)),
            "sin(toRadians(30))"
        )
        self.assert_expression(
            script.log(script._score * 2) + 8,
            "log(_score * 2) + 8"
        )
        self.assert_expression(
            script._index.num_docs(),
            "_index.numDocs()"
        )
        self.assert_expression(
            script._index[ShopDocument.region].docCount(),
            "_index['region'].docCount()"
        )
        self.assert_expression(
            script._index[ShopDocument.region][19].tf(),
            "_index['region'][19].tf()"
        )
