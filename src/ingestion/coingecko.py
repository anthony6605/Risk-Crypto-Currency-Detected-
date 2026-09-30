import requests

from config.configs import COINGECKO_API_KEY, COINGECKO_BASE_URL


class CoinGeckoClient:
    BASE_URL = COINGECKO_BASE_URL

    def __init__(self):
        auth_headers = {
            "https://api.coingecko.com/api/v3": "x-cg-demo-api-key",
            "https://pro-api.coingecko.com/api/v3": "x-cg-pro-api-key",
        }
        if self.BASE_URL not in auth_headers:
            raise ValueError(
                "COINGECKO_BASE_URL must be https://api.coingecko.com/api/v3 "
                "(Demo) or https://pro-api.coingecko.com/api/v3 (Pro)."
            )
        if not COINGECKO_API_KEY:
            raise ValueError("Set COINGECKO_API_KEY in the project .env file.")

        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            auth_headers[self.BASE_URL]: COINGECKO_API_KEY,
        })

    def get_market_data(
        self,
        currency: str = "usd",
        limit: int = 100,
    ):
        url = f"{self.BASE_URL}/coins/markets"

        params = {
            "vs_currency": currency,
            "order": "market_cap_desc",
            "per_page": limit,
            "page": 1,
            "sparkline": False,
        }

        response = self.session.get(
            url,
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def get_market_history(
        self,
        coin_id: str,
        currency: str = "usd",
        days: int = 30,
    ):
        url = (
            f"{self.BASE_URL}/coins/"
            f"{coin_id}/market_chart"
        )

        params = {
            "vs_currency": currency,
            "days": days,
        }

        response = self.session.get(
            url,
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()
