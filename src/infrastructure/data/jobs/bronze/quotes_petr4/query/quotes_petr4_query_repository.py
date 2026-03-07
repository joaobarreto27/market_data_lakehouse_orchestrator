import logging  # noqa: D100
from typing import Any, Optional

from .....utils import ConnectAPI, EnvManager

logger = logging.getLogger(__name__)


class QuotesPetr4BronzeQueryRepository:  # noqa: D101
    def __init__(self, base_url: str) -> None:
        """Initialize API query with base URL."""
        self.data_json: dict[str, Any] = {}
        self.data: dict[str, Any] = {}
        self.base_url = base_url
        logger.debug(f"BronzeQueryRepository initialized with base URL: {base_url}")

    def get_token(self) -> str:  # noqa: D102
        try:
            api_token: Optional[str] = EnvManager().get_token()
            if not api_token:
                msg = "API token not found in environment"
                logger.error(msg)
                raise RuntimeError(msg)
            logger.info("API token retrieved")
            return api_token
        except Exception as e:
            logger.exception("Failed to retrieve API token")
            raise e

    def get_daily_closing(self, quotes):  # noqa: D102
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
