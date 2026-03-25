"""Module for resolving AWS SSM Parameter Store names based on configuration."""

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class ParameterResolver:
    """Resolver for AWS Systems Manager Parameter Store paths.

    Maps system types and project identifiers to AWS SSM parameter paths
    by parsing a configuration JSON file.
    """

    def __init__(self, config_path: str = "parameter_config.json") -> None:
        """Initialize the parameter resolver.

        Args:
            config_path (str): Path to the configuration JSON file.
        """
        current_dir = Path(__file__).resolve().parent
        path = current_dir.parent.joinpath("connection", config_path)
        self.config_path = Path(path)

        self._config_cache: Optional[Dict[Any, Any]] = None

    def _load_config(self) -> dict:
        """Load and cache the configuration JSON file."""
        if self._config_cache is None:
            if not self.config_path.is_file():
                raise FileNotFoundError(
                    f"Configuration JSON file not found: {self.config_path}"
                )
            with open(self.config_path, "r") as f:
                self._config_cache = json.load(f)
        return self._config_cache  # type: ignore

    def resolve(self, system_type: str, project_name: str) -> str:
        """Resolve the AWS SSM Parameter Store path.

        Args:
            system_type (str): Type of system (e.g., 'api').
            project_name (str): Project or domain name.

        Returns:
            str: AWS SSM Parameter path in format:
                 {prefix}/{system_type}/{project_name}/{environment}/{alias}
        """
        config = self._load_config()

        try:
            data = config["parameters"][system_type][project_name]
            return (
                f"/{data['prefix']}/{system_type}/{project_name}/"
                f"{data['environment']}/{data['alias']}"
            )
        except KeyError as e:
            raise KeyError(f"Project '{project_name}'. Missing key: {e}") from None
