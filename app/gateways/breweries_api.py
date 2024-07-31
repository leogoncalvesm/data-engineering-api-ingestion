from typing import Any
from time import sleep
from requests import get
from urllib.parse import urlencode


class BreweriesAPIGateway:
    API_URL = "https://api.openbrewerydb.org/v1/breweries"

    def __init__(self, retries: int = 3, delay_seconds: int = 3) -> None:
        self.retries = retries
        self.delay_seconds = delay_seconds

    # Usefull API methods should be implemented in this class

    def get_request(self, target_url: str) -> dict[str, Any]:
        attempt = 0
        while attempt < self.retries:
            output = get(url=target_url)
            if output.status_code == 200:
                return output.json()

            # Should verify other status codes, but I didn't get any other while testing
            # Not sure what to expect, so I just moved on

            attempt += 1
            sleep(self.delay_seconds)

        # This could be a custom exception or something like that
        raise Exception("Couldn't fetch from API")

    def list_breweries(self, **kwargs: dict[str, Any]) -> list[dict[str, str]]:
        target_url = f"{BreweriesAPIGateway.API_URL}?{urlencode(kwargs)}"
        return self.get_request(target_url)

    def get_metadata(self, **kwargs: dict[str, Any]) -> dict[str, str]:
        target_url = f"{BreweriesAPIGateway.API_URL}/meta?{urlencode(kwargs)}"
        return self.get_request(target_url)
