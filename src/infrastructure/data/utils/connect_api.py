"""Module responsible for connecting to the API."""

from typing import Any

import requests


class ConnectAPI:
    """Manages connection the API with support for multiple authentication methods."""

    def __init__(self, url: str) -> None:
        """Initializes the connection to the API.

        Args:
            url (str): Base URL of the API.

        """
        self.url: str = url
        self.data_json: dict[str, Any] = {}

    def connect_api(
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
            raise ValueError(
                "Choose only one authentication method: auth, bearer_token, "
                "oauth_token, token, or params_query."
            )

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
        else:
            raise ValueError(
                f"""Connection error: Status code {response.status_code}.
                Please try again."""
            )
        return self.data_json
