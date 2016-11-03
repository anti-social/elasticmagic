===========
Quick Start
===========

.. testsetup:: python

   from mock import patch

   from elasticsearch import Elasticsearch
               
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
       print(doc._id, doc.name)

.. testoutput:: python
   :hide:

   ('1234', u"Lego Ninjago Cole's dragon")

.. testcode:: python
   :hide:

   search_hits_patch.__exit__()
