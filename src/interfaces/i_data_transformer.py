from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class IDataTransformer(ABC):
    """
       Interface for data transformation operations to be implemented by any data transformer class.

       Adheres to:
           Interface Segregation Principle (ISP): Provides a specific interface for data transformation, ensuring classes only implement what they need.
       """
    @abstractmethod
    def timestamp_countby_dayofweek(self, df: DataFrame) -> DataFrame:
        """Transforms DataFrame to add timestamp and count by day of the week."""
        pass

    @abstractmethod
    def group_and_count_crimes_by_type(self, df: DataFrame) -> DataFrame:
        """Groups DataFrame by crime type and counts occurrences."""
        pass

    @abstractmethod
    def add_and_count_crimes_from_specific_day(self, df: DataFrame, date_str: str) -> DataFrame:
        """Adds data and counts crimes from a specific day."""
        pass
