import requests


def fetch_get(url, params):
    # Construct the full query URL
    q = url + "?" + "&".join(params) if params else url
    print(q)

    # Make the HTTP GET request
    resp = requests.get(q)

    # Attempt to parse the JSON response, with fallback to empty dict on failure
    try:
        body = resp.json()
    except ValueError:
        body = {}

    return [resp, body]
