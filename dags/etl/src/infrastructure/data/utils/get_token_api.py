"""Module for managing environment variables and API authentication tokens.

Provides utilities for loading and retrieving configuration from .env files,
including API tokens and other sensitive configuration parameters.
"""

import logging
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from dotenv import dotenv_values

from .parameter_resolver import ParameterResolver

logger = logging.getLogger(__name__)


class EnvManager:
    """Manages environment variables and API tokens."""

    def __init__(
        self,
        environment: str = "dev",
        env_file: Optional[str] = None,
        aws_parameter_name: Optional[str] = None,
        system_type: str = "api",
        project_name: Optional[str] = None,
        config_path: str = "parameter_config.json",
    ) -> None:
        """Initialize the manager and load configurations."""
        self.environment = environment
        self.aws_parameter_name = aws_parameter_name
        self.env_vars: dict[str, str | None] = {}

        if self.environment == "prd" and not self.aws_parameter_name:
            try:
                resolver = ParameterResolver(config_path)
                self.aws_parameter_name = resolver.resolve(system_type, project_name)  # type: ignore
                logger.info(
                    f"Parameter name resolved dynamically: {self.aws_parameter_name}"
                )
            except (FileNotFoundError, KeyError) as e:
                logger.warning(
                    f"Failed to resolve parameter from JSON configuration: {e}. "
                    "System will attempt to use local .env file if available."
                )

        try:
            if env_file is not None:
                self.env_file = Path(env_file).resolve()
            else:
                current_path = Path(__file__).resolve()
                for parent in current_path.parents:
                    candidate = parent / ".env"
                    if candidate.is_file():
                        self.env_file = candidate
                        break
                else:
                    raise FileNotFoundError("Configuration file not found in parents.")

            if self.env_file.is_file():
                self.env_vars = dotenv_values(dotenv_path=self.env_file)
                logger.info(f".env file loaded from: {self.env_file}")

        except FileNotFoundError as e:
            if self.environment == "dev":
                logger.error(e)
                raise e
            else:
                logger.info("Running in PRD without .env file. Relying on AWS SSM.")

    def _get_ssm_parameter(self, parameter_name: str) -> Optional[str]:
        """Fetch a parameter from AWS Systems Manager Parameter Store."""
        try:
            region = self.env_vars.get("AWS_REGION") or "us-east-1"
            ssm = boto3.client("ssm", region_name=region)
            response = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
            return response["Parameter"]["Value"]
        except ClientError as e:
            logger.error(f"AWS SSM ClientError for {parameter_name}: {e}")
            return None

    def get_token(self, token_name: Optional[str] = None) -> Optional[str]:
        """Retrieve the API token dynamically from AWS SSM or .env."""
        if self.environment == "prd" and self.aws_parameter_name:
            logger.info(
                f"Fetching API token from AWS Parameter Store:{self.aws_parameter_name}"
            )
            token = self._get_ssm_parameter(self.aws_parameter_name)
            if token:
                return token
            logger.warning("Failed to get token from SSM. Falling back to .env...")

        token_name = token_name or "API_TOKEN"
        token = self.env_vars.get(token_name)

        if token is None:
            logger.warning(f"API token '{token_name}' not found in .env")
        else:
            logger.info(f"API token '{token_name}' retrieved from .env")

        return token
