.. _quick_start:

===========
Quick Start
===========

.. testsetup:: python

   from unittest.mock import patch

   from elasticsearch import Elasticsearch
   from elasticsearch.client import IndicesClient

   info_patch = patch.object(
       Elasticsearch, 'info',
       return_value={
           'version': {
               'number': '6.7.2',
               'distribution': 'elasticsearch',

           }
       }
   )
   info_patch.__enter__()

   put_mapping_patch = patch.object(IndicesClient, 'put_mapping')

   index_patch = patch.object(Elasticsearch, 'bulk',
       return_value={
           'took': 5,
           'errors': False,
           'items': [],
       }
   )
               
   search_hits_patch = patch.object(Elasticsearch, 'search',
       return_value={
           'hits': {
               'total': 1,
               'max_score': 1,
               'hits': [
                   {
                       '_id': '1234',
                       '_type': 'product',
                       '_index': 'test',
                       '_score': 1.0,
                       '_source': {
                           'name': "Lego Ninjago Cole's dragon",
                           'status': 0,
                       }
                   }
               ],
           }
       }
   )

   search_aggs_patch = patch.object(Elasticsearch, 'search',
       return_value={
           'hits': {
               'total': 1,
               'max_score': 1,
               'hits': [],
           },
           'aggregations': {
               'prices': {
                   'buckets': [
                       {'key': 0, 'doc_count': 4},
                       {'key': 20, 'doc_count': 35},
                       {'key': 40, 'doc_count': 7},
                   ]
               }
           }
       }
   )

First of all create Elasticsearch cluster and index objects:

.. testcode:: python

   from elasticsearch import Elasticsearch
   from elasticmagic import Cluster, Index

   es_cluster = Cluster(Elasticsearch())
   es_index = Index(es_cluster, 'test')

Let's describe elasticsearch document:

.. testcode:: python

   from elasticmagic import Document, Field
   from elasticmagic.types import String, Integer, Float

   class ProductDocument(Document):
       __doc_type__ = 'product'

       name = Field(String, fields={
           'sort': Field(
               String, index='no', doc_values=True, analyzer='keyword'
           ),
       })
       status = Field(Integer)
       price = Field(Float)

To create or update document mapping just run:

.. testcode:: python
   :hide:

   put_mapping_patch.__enter__()

.. testcode:: python

   es_index.put_mapping(ProductDocument)

.. testcode:: python
   :hide:

   put_mapping_patch.__exit__(None, None, None)

Try to reindex some documents:

.. testcode:: python
   :hide:

   index_patch.__enter__()

.. testcode:: python

   from decimal import Decimal

   doc1 = ProductDocument(
       name="Lego Ninjago Cole's dragon",
       status=0,
       price=Decimal('10.99'),
   )
   doc2 = ProductDocument()
   doc2.name = 'Lego minifigure'
   doc2.status = 1
   doc2.price = Decimal('2.50')
   result = es_index.add([doc1, doc2])
   assert result.errors == False

.. testcode:: python
   :hide:

   index_patch.__exit__(None, None, None)

Now we can build query:

.. testcode:: python

   search_query = (
       es_index.search_query(ProductDocument.name.match('lego'))
       .filter(ProductDocument.status == 0)
       .order_by(ProductDocument.name.sort)
       .limit(20)
   )

And finally make request and process result:

.. testcode:: python
   :hide:

   search_hits_patch.__enter__()

.. testcode:: python

   for doc in search_query:
       print('{}: {}'.format(doc._id, doc.name))

.. testoutput:: python
   :hide:

   1234: Lego Ninjago Cole's dragon

.. testcode:: python
   :hide:

   search_hits_patch.__exit__(None, None, None)

Let's build a histogram by price:

.. testcode:: python
   :hide:

   search_aggs_patch.__enter__()

.. testcode:: python

   from elasticmagic import agg

   search_query = (
       es_index.search_query()
       .filter(ProductDocument.status == 0)
       .aggs({
           'prices': agg.Histogram(ProductDocument.price, interval=20)
       })
       .limit(0)
   )

   for bucket in search_query.result.get_aggregation('prices').buckets:
       print('{} ({})'.format(bucket.key, bucket.doc_count))

.. testoutput:: python
   :hide:

   0 (4)
   20 (35)
   40 (7)

.. testcode:: python
   :hide:

   search_aggs_patch.__exit__(None, None, None)
