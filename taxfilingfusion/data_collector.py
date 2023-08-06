import pandas as pd
import numpy as np
import requests

dict_income_group = None
df_fed_state_filings = None
df_tax_attribute = None
dict_state = None


def inc_dict():
    global dict_income_group
    if dict_income_group is None:
        url = f"https://us-central1-taxfiling-aggregation.cloudfunctions.net/fed-tax-data-get?p_01=inc"
        response = requests.get(url)
        responsedata = response.json()
        data = pd.DataFrame(responsedata)
        df = pd.DataFrame(data)
        income_dict = {}
        for index, row in df.iterrows():
            income_level = row["income_level"]
            description = row["income_level_description"]
            income_dict[income_level] = description
        dict_income_group = income_dict
    return dict_income_group


def ff_dataget():
    global df_fed_state_filings
    if df_fed_state_filings is None:
        df_fed_state_filings = pd.DataFrame()
        for year in range(2015, 2021):
            url = f"https://us-central1-taxfiling-aggregation.cloudfunctions.net/fed-tax-data-get?p_01={year}"
            response = requests.get(url)
            data = response.json()
            df = pd.DataFrame(data)
            df_fed_state_filings = pd.concat(
                [df_fed_state_filings, df], ignore_index=True)
        df_fed_state_filings.rename(columns=lambda x: x.strip(), inplace=True)
        df_fed_state_filings.reset_index(drop=True, inplace=True)
    return df_fed_state_filings


def tax_dict():
    global df_tax_attribute
    if df_tax_attribute is None:
        url = f"https://us-central1-taxfiling-aggregation.cloudfunctions.net/fed-tax-data-get?p_01=dict"
        response = requests.get(url)
        responsedata = response.json()
        data = pd.DataFrame(responsedata)
        df = pd.DataFrame(data)
        df_tax_attribute = df
    return df_tax_attribute


def st_dict():
    global dict_state
    if dict_state is None:
        url = f"https://us-central1-taxfiling-aggregation.cloudfunctions.net/fed-tax-data-get?p_01=st"
        response = requests.get(url)
        responsedata = response.json()
        data = pd.DataFrame(responsedata)
        state_dict = {}
        for index, row in data.iterrows():
            state_code = row["state_code"]
            state_name = row["state_name"]
            state_dict[state_code] = state_name
        dict_state = state_dict
    return dict_state


def init():
    tax_dict()
    st_dict()
    inc_dict()
    ff_dataget()
