.. _search_api:

================
Search Query API
================

.. testsetup:: python

   import datetime
   import pprint
   
   from elasticmagic import SearchQuery
   from elasticmagic import DynamicDocument

   PostDocument = DynamicDocument

   search_query = SearchQuery()

.. autoclass:: elasticmagic.search.SearchQuery
   :members:
