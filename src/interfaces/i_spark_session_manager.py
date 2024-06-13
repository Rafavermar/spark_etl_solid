from abc import ABC, abstractmethod
from pyspark.sql import SparkSession


class ISparkSessionManager(ABC):
    """
        Interface for managing Spark sessions, including starting and stopping the session.

        Adheres to:
            Interface Segregation Principle (ISP): Provides a specific interface for Spark session management, ensuring classes only implement what they need.
        """
    @abstractmethod
    def get_spark_session(self) -> SparkSession:
        """Returns a Spark session."""
        pass

    @abstractmethod
    def stop_spark(self):
        """Stops the Spark session."""
        pass
