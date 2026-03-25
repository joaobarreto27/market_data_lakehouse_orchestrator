"""Module for querying PETR4 stock quotes from external API.

Provides repository class for fetching daily closing price data
for PETR4 stock ticker from the BRAPI source system.
"""

import logging
from typing import Any, Optional

from .....utils import ConnectAPI, EnvManager

logger = logging.getLogger(__name__)


class QuotesPetr4BronzeQueryRepository:
    """Repository for querying PETR4 quotes from external API sources.

    Handles authentication and API communication to fetch daily stock
    price data for PETR4, including token management and error handling.
    """

    def __init__(self, base_url: str, db_name: str, environment: str) -> None:
        """Initialize API query with base URL.

        Args:
            base_url (str): Base URL of the API endpoint.
            db_name (stl): The db name
            environment (str): Environment type ('dev' or 'prd').
        """
        self.data_json: dict[str, Any] = {}
        self.data: dict[str, Any] = {}
        self.base_url: str = base_url
        self.db_name: str = db_name
        self.environment: str = environment

    def get_token(self) -> str:
        """Retrieve API authentication token from environment configuration.

        Loads the API token from environment variables (.env file).

        Returns:
            The API authentication token string.

        Raises:
            RuntimeError: If API token is not configured in environment.
        """
        try:
            api_token: Optional[str] = EnvManager(
                environment=self.environment, project_name=self.db_name
            ).get_token()
            if not api_token:
                msg = "API token not found in environment"
                logger.error(msg)
                raise RuntimeError(msg)
            logger.info("API token retrieved")
            return api_token
        except Exception as e:
            logger.exception("Failed to retrieve API token")
            raise e

    def get_daily_closing(self, quotes: str) -> dict[str, Any]:
        """Fetch daily closing price data for specified stock quotes.

        Makes API request to retrieve the latest closing price data for the
        specified quote symbol.

        Args:
            quotes: Stock ticker symbol (e.g., 'PETR4').

        Returns:
            Dictionary containing the API response with stock price data.

        Raises:
            ValueError: If API returns empty data or HTTP error occurs.
        """
        try:
            url = f"{self.base_url}/{quotes}"
            logger.info(f"Fetching daily closing data for {quotes} from: {url}")
            api_token = self.get_token()
            self.data_json = ConnectAPI(url=url).connect(bearer_token=api_token)
            if self.data_json:
                self.data = next(iter(self.data_json.values()))
                logger.info(f"Daily closing data retrieved for {quotes}")
            else:
                msg = f"Empty response from API for {quotes}"
                logger.error(msg)
                raise ValueError(msg)
            return self.data_json
        except Exception as e:
            logger.exception(f"Failed to fetch daily closing data for {quotes}")
            raise e
