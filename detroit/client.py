from botocore.exceptions import ClientError

from detroit.auth import fetch_token
from detroit.api import list_tags, list_sources, list_samples
from detroit.api import fetch_samples as _fetch_samples

import os
import re
import sys
import time
import zipfile

def env_variable(attr):
    if (attr not in os.environ or
        os.environ[attr] in ["", None]):
        raise RuntimeError(f"{attr} is not set")
    return os.environ[attr]

def integer_env_variable(attr):
    value = env_variable(attr)
    if not re.search("^\\d+$", value):
            raise RuntimeError(f"{attr} is invalid")
    return int(value)

class Bank:
    
    def __init__(self, zip_buffer):
        self.zip_buffer = zip_buffer

    @property
    def zip_file(self):
        return zipfile.ZipFile(self.zip_buffer, 'r')

    def dump_wav(self, dir_path):
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        for file_name in self.zip_file.namelist():
            file_path = os.path.join(dir_path, file_name)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with self.zip_file.open(file_name, 'r') as file_entry:
                with open(file_path, 'wb') as f:
                    f.write(file_entry.read())

def fetch_samples_backoff(samples, cutoff, token, n = 4, wait = 2):
    for i in range(n):
        try:
            return _fetch_samples(samples = samples,
                                  cutoff = cutoff,
                                  token = token)
        except RuntimeError:
            print(f"WARNING: failed to fetch samples [{i+1}/{n}]")
            time.sleep(wait)
    return None

def fetch_samples(sources, batch_size, cutoff, token, wait = 1):
    print(f"INFO: fetching tags")
    tags = sorted(list_tags(token))
    print(f"INFO: tags => {', '.join(tags)}")
    for tag in tags:
        for source in sources:
            print(f"INFO: listing {source}/{tag} samples")
            samples = list_samples(tag = tag,
                                   source = source,
                                   token = token)
            print(f"INFO: {len(samples)} {source}/{tag} samples found")
            n_batches = int(len(samples) / batch_size)
            if 0 != len(samples) % batch_size:
                n_batches += 1
            for i in range(n_batches):
                print(f"INFO: fetching {source}/{tag} samples [{i+1}/{n_batches}]")
                batch = samples[i * batch_size : (i + 1) * batch_size]
                wav_data = fetch_samples_backoff(samples = batch,
                                                 cutoff = cutoff,
                                                 token = token)
                if not wav_data:
                    continue
                bank = Bank(wav_data)
                dir_name = f"samples/{tag}"
                bank.dump_wav(dir_name)
                time.sleep(wait)

if __name__ == "__main__":
    try:
        if len(sys.argv) < 4:
            raise RuntimeError("please enter email, password, sources")
        email, password, sources = sys.argv[1:4]
        sources = sorted(sources.split("|"))
        batch_size = integer_env_variable("DETROIT_SAMPLES_BATCH_SIZE")
        cutoff = integer_env_variable("DETROIT_SAMPLES_CUTOFF")
        stack_name = env_variable("DETROIT_SAMPLES_STACK_NAME")
        print(f"INFO: fetching token")                
        token = fetch_token(stack_name = stack_name,
                            email = email,
                            password = password)
        fetch_samples(sources = sources,
                      batch_size = batch_size,
                      cutoff = cutoff,
                      token = token)
    except RuntimeError as error:
        print(f"ERROR: {error}")
    except ClientError as error:
        print(f"ERROR: {error}")
