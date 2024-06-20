import requests
import time

import saeb.global_keys as global_keys

TOKEN = global_keys.get_api_token('PROD')

def get_request(url, header=None, param=None):
    attempts = 0
    while attempts < 5:  # Limita a 5 tentativas
        try:
            response = requests.get(url, headers=header, params=param)
            if response.status_code == 200:
                return response
            else:
                print(f'Error {response.status_code}. Retrying in 20 seconds...')
                time.sleep(20)
                attempts += 1
        except requests.exceptions.Timeout:
            print('Timeout error. Retrying...')
            attempts += 1
            time.sleep(10)
    return None  # Retorna None se falhar após todas as tentativas

def get_data(url):
    page = 1
    limit = 100
    is_more_data = True
    data = []
    while is_more_data:
        header = {'token': TOKEN}
        param = {'p': page, 'm': limit}
        response = get_request(url, header=header, param=param)
        if response is None:
            print("Failed to fetch data after several attempts.")
            break  # Sai do loop se não conseguir dados após várias tentativas
        content = response.json()
        data.extend(content['results']['data'])
        print(f"Fetched page {page} with {len(content['results']['data'])} records.")
        if content['results']['total_pages'] > page:
            page += 1
        else:
            is_more_data = False
    return data
