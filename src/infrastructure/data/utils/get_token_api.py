"""Module for managing environment variables and API tokens."""

from pathlib import Path
from typing import Optional

from dotenv import dotenv_values


class EnvManager:
    """Manages environment variables and API tokens."""

    def __init__(self, env_file: Optional[str] = None) -> None:
        """Initialize the manager and load the .env file.

        Args:
            env_file (str | None): Path to .env file.
                If None, automatically searches parent directories.

        Raises:
            FileNotFoundError: If .env file not found.
        """
        if env_file is not None:
            # User provided a manual path
            self.env_file = Path(env_file).resolve()
        else:
            # Search for .env by traversing parent directories
            current_path = Path(__file__).resolve()
            for parent in current_path.parents:
                candidate = parent / ".env"
                if candidate.is_file():
                    self.env_file = candidate
                    break
            else:
                raise FileNotFoundError(".env file not found in any parent directory.")

        if not self.env_file.is_file():
            raise FileNotFoundError(f".env file not found at: {self.env_file}")

        # Load all variables from .env
        self.env_vars = dotenv_values(dotenv_path=self.env_file)

    def get_token(self, token_name: Optional[str] = None) -> Optional[str]:
        """Retrieve the API token from .env file.

        Args:
            token_name (str | None): Name of the token variable.
                Defaults to 'API_TOKEN'.

        Returns:
            str | None: Token value or None if not found.
        """
        token_name = token_name or "API_TOKEN"
        token = self.env_vars.get(token_name)
        if token is None:
            print(f"Warning: variable '{token_name}' not found in .env file")
        return token

    def get_variable(self, var_name: str) -> Optional[str]:
        """Retrieve any variable from .env file.

        Args:
            var_name (str): Name of the variable to retrieve.

        Returns:
            str | None: Variable value or None if not found.
        """
        value = self.env_vars.get(var_name)
        if value is None:
            print(f"Warning: variable '{var_name}' not found in .env file")
        return value
