"""Konzertmeister API utilities.

Usage in a Databricks notebook:

    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.abspath("/Workspace/Users/..../bmk-km-stats/utils")))

    from utils.km_api import init, get_km_auth_token, km_get_request, get_km_history
    init(dbutils)  # one-time setup so secrets are accessible
"""

import requests

_dbutils = None


def init(dbutils):
    """Initialise the module with a reference to dbutils (needed for secrets)."""
    global _dbutils
    _dbutils = dbutils


def get_km_auth_token():
    """
    Retrieves an authentication token from the Konzertmeister API.

    Returns
    -------
    str
        The authentication token retrieved from the API response headers.

    Raises
    ------
    RuntimeError
        If init() has not been called or the login request fails.
    """
    if _dbutils is None:
        raise RuntimeError("Call init(dbutils) before using this module.")

    login_url = "https://rest.konzertmeister.app/api/v2/login"
    password = _dbutils.secrets.get(scope="bmk-key-vault-scope", key="km-test-user-password-post")
    mail = _dbutils.secrets.get(scope="bmk-key-vault-scope", key="km-test-user-mail")

    headers = {"Content-Type": "application/json"}

    payload = {
        "mail": mail,
        "password": password,
        "locale": "en_US",
        "timezone": "Europe/Berlin",
    }

    login_response = requests.post(login_url, json=payload, headers=headers)

    if login_response.status_code == 200:
        auth_token_header = login_response.headers.get("X-AUTH-TOKEN")
        if auth_token_header:
            print("Auth token retrieved")
            return auth_token_header

    raise RuntimeError(
        f"KM login failed with status {login_response.status_code}: {login_response.text}"
    )


def km_get_request(url: str):
    """
    Send an authorised GET request to the Konzertmeister API.

    Parameters
    ----------
    url : str
        The URL to send the GET request to.

    Returns
    -------
    dict
        The JSON response from the GET request if successful.
    """
    token = get_km_auth_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        try:
            data = response.json()
            print("retrieved data")
            return data
        except ValueError:
            print("Failed to parse response as JSON")
            print("Response text:", response.text)


def get_km_history(start_date: str, end_date: str):
    """
    Retrieve attendance history from the Konzertmeister API.

    Parameters
    ----------
    start_date : str
        Start date in YYYY-MM-DD format.
    end_date : str
        End date in YYYY-MM-DD format.

    Returns
    -------
    dict
        The JSON response containing attendance history.
    """
    url = "https://rest.konzertmeister.app/api/v2/att/matrix/history"

    token = get_km_auth_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    payload = {
        "start": f"{start_date}T00:00:00+02:00",
        "end": f"{end_date}T23:59:59+01:00",
        "parentOrgId": 14981,
        "subOrgIds": None,
        "tagIds": None,
        "typIds": [1, 2, 3, 4, 5],
        "typesAndTagsWithAnd": True,
    }

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code == 200:
        try:
            data = response.json()
            print("retrieved data")
            return data
        except ValueError:
            print("Failed to parse response as JSON")
            print("Response text:", response.text)
