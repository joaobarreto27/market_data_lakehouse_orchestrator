"""Module for HTTP API connections with flexible authentication support.

Provides utilities for making authenticated HTTP requests with support for
multiple authentication methods including basic auth, bearer tokens, and OAuth.
"""

import logging
from typing import Any

import requests

from ..repository import ApiRepository

logger = logging.getLogger(__name__)


class ConnectAPI(ApiRepository):
    """Manages connection the API with support for multiple authentication methods."""

    def __init__(self, url: str) -> None:
        """Initializes the connection to the API.

        Args:
            url (str): Base URL of the API.

        """
        self.url: str = url
        self.data_json: dict[str, Any] = {}

    def connect(
        self,
        auth: tuple[str, str] | None = None,
        token: str | None = None,
        bearer_token: str | None = None,
        oauth_token: str | None = None,
        params_query: dict[str, str] | None = None,
        custom_headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Connects to an API with flexible authentication.

        Args:
            auth (tuple[str, str] | None): For Basic Auth (username, password).
            token (str | None): Authentication token for header (Token-based auth).
            bearer_token (str | None): Bearer token for header (OAuth 2.0 style).
            oauth_token (str | None): OAuth token for header.
            params_query (dict[str, str] | None): Optional query parameters,
            including token via query.
            custom_headers (dict[str, str] | None): Custom headers include in request.

        Returns:
            dict[str, Any]: JSON response from the API.

        Raises:
            ValueError: If authentication parameters are invalid
            or the request fails.
        """
        auth_methods = sum(
            x is not None
            for x in [auth, token, bearer_token, oauth_token, params_query]
        )
        if auth_methods > 1:
            msg = "Multiple authentication methods provided. Only one is allowed."
            logger.error(msg)
            raise ValueError(msg)

        request_auth = auth
        headers: dict[str, str] = custom_headers.copy() if custom_headers else {}
        params: dict[str, str] = params_query or {}

        if token is not None:
            headers["Authorization"] = f"Token {token}"

        if bearer_token is not None:
            headers["Authorization"] = f"Bearer {bearer_token}"

        if oauth_token is not None:
            headers["Authorization"] = f"OAuth {oauth_token}"

        response = requests.get(
            url=self.url,
            auth=request_auth,
            headers=headers if headers else None,
            params=params if params else None,
            timeout=10,
        )

        if response.status_code == 200:
            self.data_json = response.json()
            logger.info(f"API request to {self.url} successful")
        else:
            msg = f"API request failed with status {response.status_code}"
            logger.error(msg)
            raise ValueError(msg)
        return self.data_json
