from abc import ABC, abstractmethod
from pyspark.sql import SparkSession


class ISparkSessionManager(ABC):
    @abstractmethod
    def get_spark_session(self) -> SparkSession:
        """Returns a Spark session."""
        pass

    @abstractmethod
    def stop_spark(self):
        """Stops the Spark session."""
        pass
