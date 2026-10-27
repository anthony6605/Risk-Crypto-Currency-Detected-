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
        page: int = 1,
    ):
        # Top coins by market cap. CoinGecko allows up to 250 per page.
        url = f"{self.BASE_URL}/coins/markets"

        params = {
            "vs_currency": currency,
            "order": "market_cap_desc",
            "per_page": limit,
            "page": page,
            "sparkline": False,
        }

        response = self.session.get(
            url,
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def get_market_history_range(
        self,
        coin_id: str,
        currency: str,
        start,
        end,
    ):
        # start / end are timezone-aware UTC datetimes
        url = (
            f"{self.BASE_URL}/coins/"
            f"{coin_id}/market_chart/range"
        )

        params = {
            "vs_currency": currency,
            "from": int(start.timestamp()),
            "to": int(end.timestamp()),
        }

        response = self.session.get(
            url,
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def get_coin_list(self):
        # Every coin CoinGecko knows: [{id, symbol, name}, ...]. Used for validation.
        url = f"{self.BASE_URL}/coins/list"

        response = self.session.get(
            url,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def get_simple_prices(
        self,
        coin_ids: list,
        currency: str = "usd",
        chunk_size: int = 100,
    ):
        # Live price and market cap for MANY coins in ONE request (validate_coins.py
        # uses it to rank look-alike ids). Longer lists are split into several requests.
        url = f"{self.BASE_URL}/simple/price"

        prices = {}

        for start in range(0, len(coin_ids), chunk_size):

            params = {
                "ids": ",".join(coin_ids[start:start + chunk_size]),
                "vs_currencies": currency,
                "include_market_cap": "true",
                "include_24hr_vol": "true",
                "include_24hr_change": "true",
                "include_last_updated_at": "true",
            }

            response = self.session.get(
                url,
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            prices.update(response.json())

        return prices
