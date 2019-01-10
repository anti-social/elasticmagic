# coding: utf-8
from __future__ import unicode_literals

from abc import ABCMeta
from math import ceil

from . import SearchQueryWrapper
from ...cluster import MAX_RESULT_WINDOW
from ...compat import with_metaclass


class BasePagination(with_metaclass(ABCMeta)):
    def _prev_page_params(self):
        return {
            'page': self.prev_num,
            'per_page': self.per_page,
            'max_items': self.max_items,
        }

    def _next_page_params(self):
        return {
            'page': self.next_num,
            'per_page': self.per_page,
            'max_items': self.max_items,
        }

    @property
    def pages(self):
        return int(
            ceil(min(self.total, self.max_items) / float(self.per_page))
        )

    @property
    def has_prev(self):
        return self.page > 1

    @property
    def prev_num(self):
        if not self.has_prev:
            return None
        return self.page - 1

    @property
    def has_next(self):
        return self.page < self.pages

    @property
    def next_num(self):
        if not self.has_next:
            return None
        return self.page + 1

    def iter_pages(self, left_edge=2, left_current=2,
                   right_current=5, right_edge=2):
        """Iterates over the page numbers in the pagination. The four
        parameters control the thresholds how many numbers should be produced
        from the sides. Skipped page numbers are represented as `None`.
        This is how you could render such a pagination in the templates:

        .. sourcecode:: html+jinja

        {% macro render_pagination(pagination, endpoint) %}
          <div class=pagination>
            {%- for page in pagination.iter_pages() %}
              {% if page %}
                {% if page != pagination.page %}
                  <a href="{{ url_for(endpoint, page=page) }}">{{ page }}</a>
                {% else %}
                  <strong>{{ page }}</strong>
                {% endif %}
              {% else %}
                <span class=ellipsis>…</span>
              {% endif %}
            {%- endfor %}
          </div>
        {% endmacro %}
"""
        last = 0
        for num in range(1, self.pages + 1):
            is_left = num <= left_edge
            is_right = num > self.pages - right_edge
            is_center = (
                self.page - left_current - 1 < num < self.page + right_current
            )
            if is_left or is_right or is_center:
                if last + 1 != num:
                    yield None
                yield num
                last = num


class Pagination(BasePagination):
    """Helper class to provide compatibility with Flask-SQLAlchemy paginator.
    """
    def __init__(
            self, query, page=1, per_page=10, max_items=MAX_RESULT_WINDOW
    ):
        self.original_query = query
        self.query = SearchQueryWrapper(query, max_items=max_items)
        self.page = page if page > 0 else 1
        self.per_page = per_page
        self.max_items = max_items
        self.offset = (self.page - 1) * self.per_page

        self.items = self.query[self.offset:self.offset + self.per_page]
        self.total = len(self.query)

    def prev(self):
        return type(self)(
            self.original_query, **self._prev_page_params()
        )

    def next(self):
        return type(self)(
            self.original_query, **self._next_page_params()
        )
