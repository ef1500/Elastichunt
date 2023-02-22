from abc import ABC, abstractmethod
from typing import List
from dataclasses import dataclass

class Filter(ABC):
    """Abstract base class for filters that can be applied to lists of items.

    Subclasses of Filter must implement the apply method, which takes a list of
    items and returns a filtered list of items based on the specific filter logic.

    Filters can be added to a Filter object using the add_filter method, which
    takes a filter object as input.

    Example usage:
    ```
    class MyFilter(Filter):
        def apply(self, items: List):
            # apply filter logic to items
            return filtered_items

    my_filter = MyFilter()
    my_filter.add_filter(MyFilterObject())
    filtered_items = my_filter.apply(items)
    ```
    """
    def __init__(self):
        self.filters = []
    
    def add_filter(self, f):
        """Add a filter object to the filter list."""
        self.filters.append(f)
    
    @abstractmethod
    def apply(self, items: List):
        """Apply filter to a list of items and return filtered list."""
        pass

class DictFilter(ABC):
    """Abstract base class for filters that can be applied to lists of dictionaries.

    Subclasses of DictFilter must implement the apply method, which takes a list of
    dictionaries and returns a filtered list of dictionaries based on the specific filter logic.

    Filters can be added to a DictFilter object using the add_filter method, which
    takes a filter object as input.

    Example usage:
    ```
    class MyFilter(DictFilter):
        def apply(self, items: List[dict]):
            # apply filter logic to items
            return filtered_items

    my_filter = MyFilter()
    my_filter.add_filter(MyFilterObject())
    filtered_items = my_filter.apply(items)
    ```
    """
    def __init__(self):
        self.filters = []

    @abstractmethod
    def apply(self, items: List[dict]):
        """Apply filter to a list of dictionaries and return filtered list."""
        pass

    def add_filter(self, f):
        """Add a filter object to the filter list."""
        self.filters.append(f)
