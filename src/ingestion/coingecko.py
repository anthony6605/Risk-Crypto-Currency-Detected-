import requests 
class CoinGeckoAPI: 
    BASE_URL = "https://api.coingecko.com/api/v3"

    def __init__(self):
        self.session = requests.Session()
    
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
            timeout = 30,


        )
        response.raise_for_status()
        return response.json()
