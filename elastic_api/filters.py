# Filters

import re

from typing import List
from datetime import datetime
from elastic_api.abstract_filters import Filter, DictFilter

class RegexFilter(Filter):
    def __init__(self, field_name):
        super().__init__()
        self.field_name = field_name

    def apply(self, items: List):
        filtered_items = []
        for item in items:
            for f in self.filters:
                if re.search(f, str(getattr(item, self.field_name)), flags=re.IGNORECASE):
                    filtered_items.append(item)
                    break
        return filtered_items
    
class BasicFilter(Filter):
    def __init__(self, field_name):
        super().__init__()
        self.field_name = field_name

    def apply(self, items: List):
        filtered_items = []
        for item in items:
            for f in self.filters:
                if f in str(getattr(item, self.field_name)):
                    filtered_items.append(item)
                    break
        return filtered_items

class RangeFilter(Filter):
    """A filter that returns items within a specified range based on a numeric field."""
    def __init__(self, field_name, start=None, end=None):
        super().__init__()
        self.field_name = field_name
        self.start = start
        self.end = end

    def apply(self, items: List):
        filtered_items = []
        for item in items:
            value = getattr(item, self.field_name)
            if self.start is not None and value < self.start:
                continue
            if self.end is not None and value > self.end:
                continue
            filtered_items.append(item)
        return filtered_items

class DateRangeFilter(Filter):
    """ filter that returns items within a specified date range based on a date field."""
    def __init__(self, field_name, start=None, end=None, date_format="%Y-%m-%d"):
        super().__init__()
        self.field_name = field_name
        self.start = datetime.strptime(start, date_format) if start else None
        self.end = datetime.strptime(end, date_format) if end else None

    def apply(self, items: List):
        filtered_items = []
        for item in items:
            value = datetime.strptime(getattr(item, self.field_name), "%Y-%m-%d")
            if self.start is not None and value < self.start:
                continue
            if self.end is not None and value > self.end:
                continue
            filtered_items.append(item)
        return filtered_items
    
class RegexDictFilter(DictFilter):
    def __init__(self, field_name, pattern):
        super().__init__()
        self.field_name = field_name
        self.pattern = re.compile(pattern)

    def apply(self, items: List[dict]):
        filtered_items = []
        for item in items:
            value = item.get(self.field_name)
            if value and self.pattern.search(value):
                filtered_items.append(item)
        return filtered_items
    
class NestedRegexDictFilter(DictFilter):
    def __init__(self, field_name, pattern):
        super().__init__()
        self.field_name = field_name.split('.')
        self.pattern = re.compile(pattern)

    def apply(self, items: List[dict]):
        filtered_items = []
        for item in items:
            value = self.get_nested_value(item, self.field_name)
            if value and self.pattern.search(value):
                filtered_items.append(item)
        return filtered_items

    def get_nested_value(self, item, field_name):
        for field in field_name:
            if isinstance(item, dict):
                item = item.get(field)
            else:
                return None
        return item
