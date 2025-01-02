#!/usr/bin/env python3
""" user_analytics.py
Functions that:
- Read users.json
- Run analytics on users.json
"""
import json

from ip2geotools.databases.noncommercial import DbIpCity
import pandas as pd


def parse_user_ip_addresses(file_path='../data/app_uploaded_files/user.json'):
    """
    Parses the IP addresses out of the user data and gets latitude and
    longitude coordinates from the IP addresses.

    This is only grabbing a subset of the IP addresses because the full set of
    data takes too long.

    :return: a DataFrame with latitude and longitude coordinates
    """
    json_file_path = file_path

    # opening json file
    import os
    print(os.getcwd())
    with open(json_file_path, 'r') as file:
        # raw data is a list of dictionaries "list of interactions with a person"
        raw_data = json.load(file)

    device_value = []
    # parse just the device records
    if 'devices' in raw_data:
        values = raw_data['devices']
        device_value = values

    # extract the IP addresses
    ip_addresses = [entry['ip_address'] for entry in device_value]

    # make a set based on ip_addresses to minimize api calls
    ip_address_set = set(ip_addresses)


    # lookup the latitude and longitude coordinates of each IP address
    try:
        ip_address_coords = map(
            lambda ip: DbIpCity.get(ip, api_key="free"),
            ip_address_set[:100]
        )
    except Exception as e:
        print(e)

    # create a lookup dictionary
    ip_dict = dict(zip(ip_address_set, ip_address_coords))

    # for each ip address in the original (with dupes)
    ip_address_coords = map(
        lambda ip: (ip, ip_dict[ip].latitude, ip_dict[ip].longitude),
        ip_addresses
    )

    # define column names and create a DataFrame
    coordinates = pd.DataFrame(
        ip_address_coords,
        columns=['ip', 'latitude', 'longitude']
    )

    return coordinates


def __validate_user_file_upload(file_path):
    if not file_path.endswith('.json'):
        raise ValueError("Invalid file type. Please upload a JSON file.")

    if 'user' not in file_path:
        raise ValueError("Invalid file. Please upload a user file.")
