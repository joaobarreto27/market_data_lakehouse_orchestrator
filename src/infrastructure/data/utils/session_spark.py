"""Module for managing PySpark SparkSession."""

from typing import Any, Dict, Optional

from pyspark.sql import SparkSession


class SparkSessionManager:
    """Singleton that initializes and returns a SparkSession."""

    _instance: Optional["SparkSessionManager"] = None
    _spark: Optional[SparkSession] = None

    def __new__(cls, *args: Any, **kwargs: Any) -> "SparkSessionManager":
        """Ensure only one instance exists (singleton pattern).

        Returns:
            SparkSessionManager: The singleton instance.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        app_name: str = "MySparkApp",
        master: str = "local[*]",
        configs: Optional[Dict[str, str]] = None,
    ) -> None:
        """Initialize the SparkSession automatically on first instance.

        Args:
            app_name (str): Name of the Spark application.
                Defaults to 'MySparkApp'.
            master (str): Master URL (e.g., 'local[*]' or 'yarn').
                Defaults to 'local[*]'.
            configs (dict[str, str] | None): Additional Spark configurations.
                Defaults to None.
        """
        if self._spark is not None:
            return  # Session already initialized

        builder = SparkSession.builder.appName(app_name).master(master)
        if configs:
            for key, value in configs.items():
                builder = builder.config(key, value)

        self._spark = builder.getOrCreate()

    def __getattr__(self, item: str) -> Any:
        """Delegate attribute access to SparkSession.

        Allows calling any method or accessing any attribute of
        SparkSession directly on the manager instance.

        Args:
            item (str): Attribute or method name.

        Returns:
            Any: The requested attribute or method from SparkSession.

        Raises:
            AttributeError: If SparkSession is not yet initialized.

        Example:
            session.createDataFrame(...)
        """
        if self._spark is None:
            raise AttributeError("SparkSession not yet initialized.")
        return getattr(self._spark, item)

    def stop(self) -> None:
        """Stop the SparkSession and release resources.

        Raises:
            AttributeError: If SparkSession is not yet initialized.
        """
        if self._spark:
            self._spark.stop()
            self._spark = None
