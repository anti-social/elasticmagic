We should make better api before release first version. There are some arguable decisions:

* "Private" meta field names: `_id`, `_type`, `_score` and others. Possibly it is worth to move them into special object, for example `Document.meta`.
Pros: rid of using private attributes
Cons: move away from raw elasticsearch dsl

* Expressions have `to_dict()` method that can return not only dictionary. Should be renamed. Possible variant: `to_elastic`.

* Replace `SearchQuery.result` property with `SearchQuery.get_result()` method. It was definitely a mistake.

* `QueryFilter.process_result()` method should not modify state but rather return new structure with processed results.
