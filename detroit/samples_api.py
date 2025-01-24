import io
import json
import os
import requests

def env_variable(attr):
    if (attr not in os.environ or
        os.environ[attr] in ["", None]):
        raise RuntimeError(f"{attr} is not set")
    return os.environ[attr]

def fetch_json(url, token):
    headers = {"Authorization": f"Bearer {token}",
               "Accept": "application/json"}
    response = requests.get(url, headers = headers)
    if response.status_code == 400:
        raise RuntimeError(response.text)
    elif response.status_code != 200:
        raise RuntimeError(f"Server returned HTTP {response.status_code}")
    return json.loads(response.content)

def insert_endpoint(fn, attr = "DETROIT_SAMPLES_API_ENDPOINT"):
    def wrapped(token, *args, **kwargs):
        endpoint = env_variable(attr)
        return fn(token, endpoint, *args, **kwargs)
    return wrapped

@insert_endpoint
def list_tags(token, endpoint):
    url = f"{endpoint}/tags/list"
    return fetch_json(url, token)

@insert_endpoint
def list_sources(token, endpoint):
    url = f"{endpoint}/sources/list"
    return fetch_json(url, token)

@insert_endpoint
def list_samples(token, endpoint, tag, source = None):
    url = f"{endpoint}/samples/list?tag={tag}"
    if source != None:
        url += f"&source={source}"
    return fetch_json(url, token)

@insert_endpoint
def fetch_samples(token, endpoint, samples, cutoff):
    url = f"{endpoint}/samples/fetch"
    headers = {"Authorization": f"Bearer {token}",
               "Content-Type": "application/json",
               "Accept": "application/zip"}
    content = {"samples": samples,
               "cutoff": cutoff}
    response = requests.post(url,
                             headers = headers,
                             data = json.dumps(content))
    if response.status_code == 400:
        raise RuntimeError(response.text)
    elif response.status_code != 200:
        raise RuntimeError(f"Server returned HTTP {response.status_code}")
    return io.BytesIO(response.content)

if __name__ == "__main__":
    pass
