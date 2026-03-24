"""Module for resolving AWS Secrets Manager secret names based on configuration.

This module provides a resolver that reads a JSON configuration file to map
SGBD (database management system) and database names to their corresponding
AWS Secrets Manager secret paths. It uses caching for efficient lookups.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class SecretResolver:
    """Resolver for AWS Secrets Manager secret names.

    Maps SGBD and database identifiers to AWS Secrets Manager secret paths
    by parsing a configuration JSON file. Uses caching to avoid repeated
    file I/O operations.
    """

    def __init__(self, config_path: str = "secret_config.json") -> None:
        """Initialize the secret resolver.

        Args:
            config_path (str): Path to the configuration JSON file. Defaults
                to "secret_config.json" in the parent data/connection directory.
        """
        current_dir = Path(__file__).resolve().parent
        path = current_dir.parent.joinpath("connection", config_path)
        self.config_path = Path(path)

        self._config_cache: Optional[Dict[Any, Any]] = None

    def _load_config(self) -> dict:
        """Load and cache the configuration JSON file.

        Returns:
            dict: The parsed configuration dictionary.

        Raises:
            FileNotFoundError: If the configuration file does not exist.
            json.JSONDecodeError: If the file is not valid JSON.
        """
        if self._config_cache is None:
            if not self.config_path.is_file():
                raise FileNotFoundError(
                    f"Configuration JSON file not found: {self.config_path}"
                )
            with open(self.config_path, "r") as f:
                self._config_cache = json.load(f)
        return self._config_cache

    def resolve(self, sgbd: str, db: str) -> str:
        """Resolve the AWS Secrets Manager secret path for a database.

        Maps a database management system and database name to its corresponding
        AWS Secrets Manager secret path using the configuration JSON.

        Args:
            sgbd (str): Database management system name (e.g., 'postgresql').
            db (str): Database name (e.g., 'my_database').

        Returns:
            str: AWS Secrets Manager secret path in format:
                 {prefix}/{sgbd}/{db}/{environment}/{alias}

        Raises:
            FileNotFoundError: If the configuration file is not found.
            KeyError: If the SGBD/database combination is not configured.
        """
        config = self._load_config()

        try:
            data = config["connections"][sgbd][db]
            # Build the secret path: prefix/sgbd/db/env/alias
            return f"{data['prefix']}/{sgbd}/{db}/{data['environment']}/{data['alias']}"
        except KeyError as e:
            raise KeyError(
                f"Configuration mapping not found for SGBD '{sgbd}' and "
                f"database '{db}' in the configuration JSON. Missing key: {e}"
            ) from None
