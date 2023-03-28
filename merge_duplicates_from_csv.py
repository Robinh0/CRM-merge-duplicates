import pandas as pd
import requests
from pprint import pprint

API_TOKEN = "pipedrive-api-token"

API_PARAMS = {
    'api_token': API_TOKEN
}

pipedrive_api_endpoint = "https://api.pipedrive.com/v1"

# Read the CSV file into a pandas DataFrame
df = pd.read_csv('people-14375401-85.csv', low_memory=False)

# Initialize an empty list to store duplicates
all_duplicates = []

# All emails
all_emails = df["Person - Email"].values

all_emails_list = pd.Series(all_emails).dropna().tolist()

# find duplicate values
duplicates = pd.Series(all_emails_list).duplicated()

# get a list of the duplicate values
duplicates_list = pd.Series(all_emails_list)[duplicates].tolist()

print(duplicates_list)
print(len(duplicates_list))


def get_person(email_address):
    """
    Returns the person_id if person found, else returns False.
    """
    search_persons_endpoint = f'{pipedrive_api_endpoint}/persons/search'

    params = {
        'api_token': API_TOKEN,
        'term': email_address,
        'fields': 'email'
    }

    response = requests.get(url=search_persons_endpoint, params=params)
    response.raise_for_status()
    data = response.json()['data']['items']
    return data


def create_id_list(data):
    """
    Returns a list of the id's of the person.
    """
    person_id_list = []
    for i in range(len(data)):
        id = data[i]['item']['id']
        person_id_list.append(id)
    return person_id_list


def person_in_email_list(person_id):
    get_persons_endpoint = f'{pipedrive_api_endpoint}/persons/{person_id}'
    response = requests.get(get_persons_endpoint, params=API_PARAMS)
    response.raise_for_status()
    data = response.json()
    # pprint(data)
    email_list_status_value = data['data']['12a142f628a4b760294be4aecee478dccc082627']
    in_email_list = False
    if email_list_status_value == '69':
        print('Added to email list')
        return True
    print('Not added to email list')
    return in_email_list


def add_to_email_list(person_id):
    person_url = f"https://api.pipedrive.com/v1/persons/{person_id}"
    field_hash = "12a142f628a4b760294be4aecee478dccc082627"
    payload = {
        field_hash: '69',
    }
    response = requests.put(person_url, params=API_PARAMS, json=payload)


def merge_persons(person_1, person_2):
    url = f"https://api.pipedrive.com/v1/persons/{person_1}/merge"
    payload = {
        'merge_with_id' : person_2,
    }
    requests.put(url, params=API_PARAMS, json=payload)


for person in duplicates_list:
    data = get_person(person)

    id_list = create_id_list(data)
    print(id_list)

    in_email_list = None
    if len(id_list) != 1:
        for person_id in id_list:
            if person_in_email_list(person_id):
                in_email_list = True
                print(in_email_list)

        if in_email_list:
            for person_id in id_list:
                add_to_email_list(person_id)

        if len(id_list) >= 2:
            merge_persons(id_list[0], id_list[1])
            print(f"Merged {id_list[0]} with {id_list[1]}.")

