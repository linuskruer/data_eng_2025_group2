import base64
import requests
import time

EBAY_OAUTH_URL = "https://api.ebay.com/identity/v1/oauth2/token"

class EbayAuth:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.expiry = 0  # epoch timestamp

    def _request_new_token(self):
        auth_header = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
        headers = {
            "Authorization": f"Basic {auth_header}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {
            "grant_type": "client_credentials",
            "scope": "https://api.ebay.com/oauth/api_scope"
        }

        resp = requests.post(EBAY_OAUTH_URL, headers=headers, data=data)
        resp.raise_for_status()
        token_data = resp.json()

        self.access_token = token_data["access_token"]
        self.expiry = int(time.time()) + int(token_data["expires_in"]) - 60  # refresh 60s before expiry
        return self.access_token

    def get_access_token(self):
        if not self.access_token or time.time() >= self.expiry:
            return self._request_new_token()
        return self.access_token


