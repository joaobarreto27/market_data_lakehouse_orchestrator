"""Module for managing PySpark SparkSession."""

import logging
import os
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class SparkSessionManager:
    """Singleton that initializes and returns a SparkSession."""

    _instance: Optional["SparkSessionManager"] = None
    _spark: Optional[SparkSession] = None

    JDBC_DRIVERS = {
        "postgresql": "org.postgresql:postgresql:42.7.3",
        "mysql": "mysql:mysql-connector-java:8.4.0",
    }

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
        sgbd_name: Optional[str] = None,
        configs: Optional[Dict[str, str]] = None,
    ) -> None:
        """Initialize the SparkSession automatically on first instance.

        Args:
            app_name (str): Name of the Spark application.
                Defaults to 'MySparkApp'.
            master (str): Master URL (e.g., 'local[*]' or 'yarn').
                Defaults to 'local[*]'.
            sgbd_name (str): Database management system name (postgresql or sqlite).
            configs (dict[str, str] | None): Additional Spark configurations.
                Defaults to None.
        """
        if self._spark is not None:
            return  # Session already initialized

        builder = SparkSession.builder.appName(app_name).master(master)  # pyright: ignore[reportAttributeAccessIssue]

        packages = []

        if sgbd_name:
            jdbc_pkg = self.JDBC_DRIVERS.get(sgbd_name)
            if jdbc_pkg:
                packages.append(jdbc_pkg)

        packages.append("org.apache.hadoop:hadoop-aws:3.3.4")
        builder = builder.config("spark.jars.packages", ",".join(packages))

        builder = builder.config("spark.driver.host", "localhost")

        if configs:
            for key, value in configs.items():
                builder = builder.config(key, value)

        self._spark = builder.getOrCreate()

        profile_name = os.getenv("AWS_PROFILE", "default")

        sc = self._spark.sparkContext  # type: ignore
        hadoop_conf = sc._jsc.hadoopConfiguration()  # type: ignore

        hadoop_conf.set(
            "fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.profile.ProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        hadoop_conf.set("fs.s3a.aws.profile", profile_name)

        hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

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
            msg = "SparkSession not yet initialized."
            logger.error(msg)
            raise AttributeError(msg)
        return getattr(self._spark, item)

    def stop(self) -> None:
        """Stop the SparkSession and release resources.

        Raises:
            AttributeError: If SparkSession is not yet initialized.
        """
        if self._spark:
            self._spark.stop()
            self._spark = None
