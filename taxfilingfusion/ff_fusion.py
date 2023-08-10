import pandas as pd
import requests


from taxfilingfusion import ff_fetch
from taxfilingfusion import ff_called_func
ff_fetch.init()
df_tax_attribute = ff_fetch.df_tax_attribute
df_fed_state_filings = ff_fetch.df_fed_state_filings
dict_state = ff_fetch.dict_state
dict_income_group = ff_fetch.dict_income_group


def tax_attributes():
    """
      This function creates a data frame that serves as a mapping between column names present in the IRS dataset and user-friendly descriptive attribute names.

      :return: A data frame with the tax attribute descriptions, names, reference fields, and tax form fields for each attribute.
    """
    return df_tax_attribute


def ff_state_county_zip_incgrp(state_code: str, county_name: str) -> pd.DataFrame:
    """
      Given a state code and either the full name or a partial name of a county, this function retrieves tax filing statistics for each zipcode within the specified county.
      The data is organized by income level and year. To interpret the tax filing attributes in the resulting dataframe, refer to the function tax_attributes().

      :param state_code: The state code (state abbreviation) with the desired county.
      :param county_name: The full name or partial name of the desired county.

      :return: A dataframe containing key tax filing statistics, grouped by income level and year, for each zipcode of the specified county.
    """
    global dict_income_group
    global dict_state
    state_codes = dict_state
    if (state_code not in list(state_codes.keys())):
        return "Please enter a valid state code."

    url = f'https://us-central1-taxfiling-aggregation.cloudfunctions.net/fn-ff-fetch?p_statecode={state_code}&p_countyname={county_name}&p_type=cnt'
    response = requests.get(url, timeout=60)
    if response.status_code == 200:
        json_data = response.json()
        dfcnt = pd.DataFrame(json_data)
        if dfcnt.empty:
            return "Please enter a valid state code and county name."
    else:
        print("Failed to fetch data from the URL.")
        return None
    url = f'https://us-central1-taxfiling-aggregation.cloudfunctions.net/fn-ff-fetch?p_statecode={state_code}&p_countyname={county_name}&p_type=amt'
    response = requests.get(url, timeout=60)
    if response.status_code == 200:
        json_data = response.json()
        dfamt = pd.DataFrame(json_data)
        if dfamt.empty:
            return "Please enter a valid state code and county name."
    else:
        print("Failed to fetch data from the URL.")
        return None
    dfamt = dfamt.drop('n_ret', axis=1)
    dfamt.rename(columns=lambda x: x.strip(), inplace=True)
    dfcnt.rename(columns=lambda x: x.strip(), inplace=True)
    df = pd.merge(dfamt, dfcnt, on=['year', 'income_level', 'state_code',
                  'division', 'region', 'state', 'zip_code', 'city_name', 'county'])

    if 'year' in df.columns:
        df.rename(columns={'year': 'filing_year'}, inplace=True)
    df['income_level_desc'] = df['income_level'].replace(dict_income_group)
    selected_columns = ['filing_year', 'income_level', 'income_level_desc']
    remaining_columns = [
        col for col in df.columns if col not in selected_columns]
    df = df[selected_columns + remaining_columns]
    df_sorted = df.sort_values(by=['filing_year', 'income_level', 'state_code', 'division',
                               'region', 'state', 'zip_code', 'city_name', 'county'], ascending=True)
    df_sorted.reset_index(drop=True, inplace=True)
    return df_sorted


def ff_state_county_zip(state_code: str, county_name: str) -> pd.DataFrame:
    """
      Given a state code and either the full name or a partial name of a county, this function retrieves tax filing statistics for each zip code within the specified county.
      All income groups are aggregated into one row per year. To interpret the tax filing attributes in the resulting dataframe, refer to the function tax_attributes().

      :param state_code: The state code (state abbreviation) with the desired county.
      :param county_name: The full name or partial name of the desired county.

      :return: A dataframe containing key tax filing statistics, grouped by income level and year, for the specified county.
    """
    global dict_state
    state_codes = dict_state
    if (state_code not in list(state_codes.keys())):
        return "Please enter a valid state code."
    df = ff_state_county_zip_incgrp(state_code, county_name)
    if (isinstance(df, pd.DataFrame) and df.empty) or isinstance(df, str):
        return "Please enter a valid state code and county name."
    aggregation_functions = {

        'n_ret': 'sum',
        'amt_total_income': 'sum',
        'amt_taxable_income': 'sum',
        'amt_tax_liability': 'sum',
        'amt_total_tax_payments': 'sum',
        'amt_total_taxes_paid': 'sum',
        'agi': 'sum',
        'amt_alternative_minimum_tax': 'sum',
        'amt_excess_advance_premium_tax_credit_repayment': 'sum',
        'amt_self_employment_tax': 'sum',
        'amt_child_other_dependent_credit': 'sum',
        'amt_foreign_tax_credit': 'sum',
        'amt_child_dependent_care_credit': 'sum',
        'amt_nonrefundable_education_credit': 'sum',
        'amt_retirement_savings_contribution_credit': 'sum',
        'amt_residential_energy_tax_credit': 'sum',
        'amt_salaries_wages': 'sum',
        'amt_taxable_interest': 'sum',
        'amt_qualified_dividends': 'sum',
        'amt_ordinary_dividends': 'sum',
        'amt_unemployment_compensation': 'sum',
        'amt_taxable_ira_distributions': 'sum',
        'amt_taxable_pensions_annuities': 'sum',
        'amt_ira_pensions': 'sum',
        'amt_net_capital_gain': 'sum',
        'amt_taxable_social_security_benefits': 'sum',
        'amt_business_professional_net_income': 'sum',
        'amt_partnership_s_corp_net_income': 'sum',
        'nbr_individuals': 'sum',
        'nbr_dep': 'sum',
        'n_ret_electronic': 'sum',
        'n_ret_head_household': 'sum',
        'n_ret_joint': 'sum',
        'n_ret_itemized_deductions': 'sum',
        'n_ret_total_income': 'sum',
        'n_ret_taxable_income': 'sum',
        'n_ret_tax_liability': 'sum',
        'n_ret_total_tax_payments': 'sum',
        'n_ret_total_taxes_paid': 'sum',
        'n_ret_alternative_minimum_tax': 'sum',
        'n_ret_excess_advance_premium_tax_credit_repayment': 'sum',
        'n_ret_self_employment_tax': 'sum',
        'n_ret_child_other_dependent_credit': 'sum',
        'n_ret_foreign_tax_credit': 'sum',
        'n_ret_child_dependent_care_credit': 'sum',
        'n_ret_nonrefundable_education_credit': 'sum',
        'n_ret_retirement_savings_contribution_credit': 'sum',
        'n_ret_residential_energy_tax_credit': 'sum',
        'n_ret_salaries_wages': 'sum',
        'n_ret_taxable_interest': 'sum',
        'n_ret_qualified_dividends': 'sum',
        'n_ret_ordinary_dividends': 'sum',
        'n_ret_unemployment_compensation': 'sum',
        'n_ret_taxable_ira_distributions': 'sum',
        'n_ret_taxable_pensions_annuities': 'sum',
        'n_ret_ira_pensions': 'sum',
        'n_ret_net_capital_gain': 'sum',
        'n_ret_taxable_social_security_benefits': 'sum',
        'n_ret_business_professional_net_income': 'sum',
        'n_ret_partnership_s_corp_net_income': 'sum'

    }

    df_aggregated = df.groupby(['filing_year', 'state_code', 'division', 'region', 'state',
                               'zip_code', 'city_name', 'county']).agg(aggregation_functions).reset_index()
    if 'n_ret_x' in df_aggregated.columns:
        df_aggregated.rename(columns={'n_ret_x': 'n_ret'}, inplace=True)
    return df_aggregated


def ff_state_city_zip_incgrp(state_code: str, city_name: str) -> pd.DataFrame:
    """
      Given a state code and either the full name or a partial name of a city, this function retrieves tax filing statistics for each zipcode within the specified city.
      The data is organized by income level and year. To interpret the tax filing attributes in the resulting dataframe, refer to the function tax_attributes().

      :param state_code: The state code (state abbreviation) with the desired city.
      :param city_name: The full name or partial name of the desired city.

      :return: A dataframe containing key tax filing statistics, by income level and year, for each zipcode of the specified city.
    """
    global dict_income_group
    global dict_state
    income_level_mapping = dict_income_group
    state_codes = dict_state
    if (state_code not in list(state_codes.keys())):
        return "Please enter a valid state code."
    url = f'https://us-central1-taxfiling-aggregation.cloudfunctions.net/fn-ff-fetch?p_statecode={state_code}&p_cityname={city_name}&p_type=cnt'
    response = requests.get(url, timeout=60)
    if response.status_code == 200:
        json_data = response.json()
        dfcnt = pd.DataFrame(json_data)
        if dfcnt.empty:
            return "Please enter a valid state code and city name."
    else:
        print("Failed to fetch data from the URL.")
        return None
    url = f'https://us-central1-taxfiling-aggregation.cloudfunctions.net/fn-ff-fetch?p_statecode={state_code}&p_cityname={city_name}&p_type=amt'
    response = requests.get(url, timeout=60)
    if response.status_code == 200:
        json_data = response.json()
        dfamt = pd.DataFrame(json_data)
        if dfamt.empty:
            return "Please enter a valid state code and city name."
    else:
        print("Failed to fetch data from the URL.")
        return None

    dfamt = dfamt.drop('n_ret', axis=1)
    df = pd.merge(dfamt, dfcnt, on=['year', 'income_level', 'state_code',
                  'division', 'region', 'state', 'zip_code', 'city_name'])
    if 'year' in df.columns:
        df.rename(columns={'year': 'filing_year'}, inplace=True)
    if 'county_x' in df.columns:
        df.rename(columns={'county_x': 'county'}, inplace=True)
    df['income_level_desc'] = df['income_level'].replace(income_level_mapping)
    selected_columns = ['filing_year', 'income_level', 'income_level_desc']
    remaining_columns = [
        col for col in df.columns if col not in selected_columns]
    df = df[selected_columns + remaining_columns]
    df_sorted = df.sort_values(by=['filing_year', 'income_level', 'state_code',
                               'division', 'region', 'state', 'zip_code', 'city_name'], ascending=True)
    df_sorted.reset_index(drop=True, inplace=True)
    return df_sorted


def ff_state_city_zip(state_code: str, city_name: str) -> pd.DataFrame:
    """
      Given a state code and either the full name or a partial name of a city, this function retrieves tax filing statistics for each zipcode within the specified city.
      All income groups are aggregated into one row per year. To interpret the tax filing attributes in the resulting dataframe, refer to the function tax_attributes().

      :param state_code: The state code (state abbreviation) with the desired city.
      :param city_name: The full name or partial name of the desired city.

      :return: A dataframe containing key tax filing statistics, grouped by income level and year, for each zipcode of the specified city.
    """
    global dict_state
    state_codes = dict_state
    if (state_code not in list(state_codes.keys())):
        return "Please enter a valid state code."
    df = ff_state_city_zip_incgrp(state_code, city_name)
    if (isinstance(df, pd.DataFrame) and df.empty) or isinstance(df, str):
        return "Please enter a valid state code and county name."
    aggregation_functions = {
        'n_ret': 'sum',
        'amt_total_income': 'sum',
        'amt_taxable_income': 'sum',
        'amt_tax_liability': 'sum',
        'amt_total_tax_payments': 'sum',
        'amt_total_taxes_paid': 'sum',
        'agi': 'sum',
        'amt_alternative_minimum_tax': 'sum',
        'amt_excess_advance_premium_tax_credit_repayment': 'sum',
        'amt_self_employment_tax': 'sum',
        'amt_child_other_dependent_credit': 'sum',
        'amt_foreign_tax_credit': 'sum',
        'amt_child_dependent_care_credit': 'sum',
        'amt_nonrefundable_education_credit': 'sum',
        'amt_retirement_savings_contribution_credit': 'sum',
        'amt_residential_energy_tax_credit': 'sum',
        'amt_salaries_wages': 'sum',
        'amt_taxable_interest': 'sum',
        'amt_qualified_dividends': 'sum',
        'amt_ordinary_dividends': 'sum',
        'amt_unemployment_compensation': 'sum',
        'amt_taxable_ira_distributions': 'sum',
        'amt_taxable_pensions_annuities': 'sum',
        'amt_ira_pensions': 'sum',
        'amt_net_capital_gain': 'sum',
        'amt_taxable_social_security_benefits': 'sum',
        'amt_business_professional_net_income': 'sum',
        'amt_partnership_s_corp_net_income': 'sum',
        'nbr_individuals': 'sum',
        'nbr_dep': 'sum',
        'n_ret_electronic': 'sum',
        'n_ret_head_household': 'sum',
        'n_ret_joint': 'sum',
        'n_ret_itemized_deductions': 'sum',
        'n_ret_total_income': 'sum',
        'n_ret_taxable_income': 'sum',
        'n_ret_tax_liability': 'sum',
        'n_ret_total_tax_payments': 'sum',
        'n_ret_total_taxes_paid': 'sum',
        'n_ret_alternative_minimum_tax': 'sum',
        'n_ret_excess_advance_premium_tax_credit_repayment': 'sum',
        'n_ret_self_employment_tax': 'sum',
        'n_ret_child_other_dependent_credit': 'sum',
        'n_ret_foreign_tax_credit': 'sum',
        'n_ret_child_dependent_care_credit': 'sum',
        'n_ret_nonrefundable_education_credit': 'sum',
        'n_ret_retirement_savings_contribution_credit': 'sum',
        'n_ret_residential_energy_tax_credit': 'sum',
        'n_ret_salaries_wages': 'sum',
        'n_ret_taxable_interest': 'sum',
        'n_ret_qualified_dividends': 'sum',
        'n_ret_ordinary_dividends': 'sum',
        'n_ret_unemployment_compensation': 'sum',
        'n_ret_taxable_ira_distributions': 'sum',
        'n_ret_taxable_pensions_annuities': 'sum',
        'n_ret_ira_pensions': 'sum',
        'n_ret_net_capital_gain': 'sum',
        'n_ret_taxable_social_security_benefits': 'sum',
        'n_ret_business_professional_net_income': 'sum',
        'n_ret_partnership_s_corp_net_income': 'sum'

    }

    df_aggregated = df.groupby(['filing_year', 'state_code', 'division', 'region',
                               'state', 'zip_code', 'city_name']).agg(aggregation_functions).reset_index()

    return (df_aggregated)


def ff_zips_incgrp(zipcodes: list) -> pd.DataFrame:
    """
      Given a zip code list, this function retrieves tax filing statistics for each zipcode in the list. e.g. [60023, 60031].
      The data is organized by income level and year. To interpret the tax filing attributes in the resulting dataframe, refer to the function tax_attributes().

      :param zipcodes: A list of comma-separated zip code(s).

      :return: A dataframe containing key tax filing statistics, grouped by income level and year, for each specified zipcode.
    """
    if not isinstance(zipcodes, list):
        return 'Please enter a list of ZIP code(s).'
    global dict_income_group
    income_level_mapping = dict_income_group
    zip_list = ','.join(str(zipcode) for zipcode in zipcodes)
    url = f'https://us-central1-taxfiling-aggregation.cloudfunctions.net/fn-ff-fetch?p_zipcodes={zip_list}&p_type=cnt'
    response = requests.get(url, timeout=60)
    if response.status_code == 200:
        json_data = response.json()
        dfcnt = pd.DataFrame(json_data)
        if dfcnt.empty:
            return "Please enter valid ZIP code(s)."
        if 'zip_code' in dfcnt.columns:
            for zipcode in zipcodes:
                if zipcode not in dfcnt['zip_code'].unique():
                    return "Please enter valid ZIP code(s)."
    else:
        print("Failed to fetch data from the URL.")
        return None
    url = f'https://us-central1-taxfiling-aggregation.cloudfunctions.net/fn-ff-fetch?p_zipcodes={zip_list}&p_type=amt'
    response = requests.get(url, timeout=60)
    if response.status_code == 200:
        json_data = response.json()
        dfamt = pd.DataFrame(json_data)
        if dfamt.empty:
            return "Please enter valid ZIP codes."
        if 'zip_code' in dfamt.columns:
            for zipcode in zipcodes:
                if zipcode not in dfamt['zip_code'].unique():
                    return "Please enter valid ZIP codes."
    else:
        print("Failed to fetch data from the URL.")
        return None
    dfcnt = dfcnt.drop('n_ret', axis=1)
    df = pd.merge(dfamt, dfcnt, on=['filing_year', 'income_level', 'zip_code'])
    if 'year' in df.columns:
        df.rename(columns={'year': 'filing_year'}, inplace=True)
    df['income_level_desc'] = df['income_level'].replace(income_level_mapping)
    selected_columns = ['filing_year', 'income_level', 'income_level_desc']
    remaining_columns = [
        col for col in df.columns if col not in selected_columns]
    df = df[selected_columns + remaining_columns]
    df_sorted = df.sort_values(
        by=['filing_year', 'income_level',  'zip_code'], ascending=True)
    df_sorted.reset_index(drop=True, inplace=True)
    return df_sorted


def geo_state_county_city(state_code: str, county_name: str = '', city_name: str = '') -> pd.DataFrame:
    """
      Given a state code and an optional county and city name, this function retrieves ZIP code information for the specified county, city and state.

      :param state_code: The state code (state abbreviation).
      :param county_name: County within state (optional parameter).
      :param city_name: City within state (optional parameter).

      :return: A dataframe containing ZIP code information for the specified city, county, and state.
    """
    global dict_state
    state_codes = dict_state
    if (state_code not in list(state_codes.keys())):
        return "Please enter a valid state code."
    url = f'https://us-central1-taxfiling-aggregation.cloudfunctions.net/fn-ff-fetch?p_statecode={state_code}&p_countyname={county_name}&p_cityname={city_name}&p_geo=yes'
    response = requests.get(url, timeout=60)
    if response.status_code == 200:
        json_data = response.json()
        df = pd.DataFrame(json_data)
        if df.empty:
            return "Please enter a valid state, county name,city name."
    else:
        print("Failed to fetch data from the URL.")
        return None
    return (df)


def ff_state_incgrp(state_code: str, year: int, income_level: int, *categories: str) -> pd.DataFrame:
    """
      This function retrieves tax filing statistics given a state code, year, income group, and type of attribute(s).
      The data is organized by income level and year. To interpret the tax filing attributes in the resulting dataframe, refer to the function tax_attributes().

      :param state_code: The state code (state abbreviation). Pass in 'All' to retrieve data for all the states, one row per state.
      :param year: A tax year from 2015-2020, inclusive. Pass in 0 to retrieve data for all the filing years, one row per year.
      :param income_level:  [1 for '<$25K', 2 for '$25K - $50K',3 for '$50K - $75K', 4 for '$75K - $100K, 5 for '$100K - $200K',6 for '$200K+', 0 for all income levels, one row per level]
      :param *categories: Attributes to be retrieved. e.g. 'n_ret',    'nbr_individuals', 'n_ret_single'. Passing in no attributes will retrieve all of them.

      :return: A dataframe containing key tax filing statistics by income group and year, at a state level.
    """
    global df_tax_attribute
    global dict_state
    global df_fed_state_filings
    global dict_income_group
    state_codes = dict_state
    extraction_attributes = df_tax_attribute['variable'].tolist()
    df_federal_filing_statelevel = df_fed_state_filings.copy()
    income_level_mapping = dict_income_group
    if 'state' in df_federal_filing_statelevel.columns:
        df_federal_filing_statelevel.rename(
            columns={'state': 'state_code'}, inplace=True)
    if 'year' in df_federal_filing_statelevel.columns:
        df_federal_filing_statelevel.rename(
            columns={'year': 'filing_year'}, inplace=True)
    for category in categories:
        if list(categories).count(category) > 1:
            return "Please enter distinct categories."
    for category in categories:
        if category not in list(extraction_attributes):
            return "Please choose a valid category."
    for category in categories:
        if category in ['filing_year', 'zipcode', 'income_level', 'statefips', 'state_code']:
            return "Please choose a attribute  not in ['filing_year', 'state_code', 'zipcode', or 'income_level', or 'statefips'.]"
    if year not in [2015, 2016, 2017, 2018, 2019, 2020, 0]:
        return "Please enter a valid year, either 2015-2020 for a specific year, or 0 for all years."
    if income_level not in [0, 1, 2, 3, 4, 5, 6]:
        return "Please enter a valid income level: either 1-6 for a specific income level, or 0 for all income levels."
    if year != 0:
        df_filtered_year = df_federal_filing_statelevel[(
            df_federal_filing_statelevel['filing_year'] == year)]
    else:
        df_filtered_year = df_federal_filing_statelevel.copy()
    if (state_code not in list(state_codes.keys())) and state_code != 'ALL':
        return "Please enter a valid state code or 'ALL' to view all states."
    if (state_code != 'ALL'):
        df_filtered_yearstate = df_filtered_year.loc[df_filtered_year['state_code'] == state_code]
    else:
        df_filtered_yearstate = df_filtered_year.copy()
    if (income_level != 0):
        df_filtered_yearstateincome = df_filtered_yearstate[(
            df_filtered_yearstate['income_level'] == income_level)]
    else:
        df_filtered_yearstateincome = df_filtered_yearstate.copy()
    if len(categories) == 0:
        df_final = df_filtered_yearstateincome[[
            'filing_year',
            'income_level',
            'state_code',
            'n_ret',
            'nbr_individuals',
            'n_ret_single',
            'n_ret_head_household',
            'n_ret_joint',
            'n_ret_salaries_wages',
            'amt_salaries_wages',
            'n_ret_net_investment_income_tax',
            'amt_net_investment_income_tax',
            'n_ret_net_capital_gain',
            'amt_net_capital_gain',
            'n_ret_ordinary_dividends',
            'amt_ordinary_dividends',
            'n_ret_qualified_dividends',
            'amt_qualified_dividends',
            'n_ret_itemized_deductions',
            'amt_agi_itemized_returns',
            'n_ret_alternative_minimum_tax',
            'amt_alternative_minimum_tax',
            'n_ret_home_mortgage_interest_paid',
            'amt_home_mortgage_interest_paid',
            'n_ret_real_estate_taxes',
            'amt_real_estate_taxes',
            'n_ret_state_local_income_taxes',
            'amt_state_local_income_taxes',
            'amt_total_income',
            'n_ret_total_income',
            'n_ret_taxable_income',
            'amt_taxable_income',
            'n_ret_total_tax_payments',
            'amt_total_tax_payments',
            'n_ret_taxable_interest',
            'amt_taxable_interest',
            'n_ret_tax_liability',
            'amt_tax_liability']].copy()
    else:
        extraction_list = ['filing_year', 'income_level',  'state_code']
        extraction_list += list(categories)
        df_final = df_filtered_yearstateincome[list(extraction_list)].copy()
    if 'year' in df_final.columns:
        df_final.rename(columns={'year': 'filing_year'}, inplace=True)
    df_final['income_level_desc'] = df_final['income_level'].replace(
        income_level_mapping)
    selected_columns = ['filing_year', 'state_code',
                        'income_level_desc', 'income_level']
    remaining_columns = [
        col for col in df_final.columns if col not in selected_columns]
    df_final = df_final[selected_columns + remaining_columns]
    df_final = df_final.reset_index(drop=True)
    if income_level == 0 and state_code != 'ALL' and year != 0:
        df_final = df_final.sort_values(by='income_level', ascending=True)
        df_final.reset_index(drop=True, inplace=True)
    return (df_final)


def ff_state(state_code: str, year: int, *categories: str) -> pd.DataFrame:
    """
      This function retrieves tax filing statistics given a state code, year, and type of attribute(s).
      The data is organized by income level and year. To interpret the tax filing attributes in the resulting dataframe, refer to the function tax_attributes().

      :param state_code: The state code (state abbreviation). Pass in 'All' to retrieve data for all the states, one row per state.
      :param year: A tax year from 2015-2020, inclusive. Pass in 0 to retrieve data for all the filing years, one row per year.
      :param *categories: Attributes to be retrieved. e.g. 'n_ret', 'nbr_individuals', 'n_ret_single'. Passing in no attributes will retrieve all of them.

      :return: A dataframe containing key tax filing statistics aggregated for all income groups by year, at a state level.
    """
    global df_tax_attribute
    global dict_state
    global df_fed_state_filings
    global dict_income_group
    state_codes = dict_state
    extraction_attributes = df_tax_attribute['variable'].tolist()

    for category in categories:
        if list(categories).count(category) > 1:
            return "Please enter distinct categories."
    for category in categories:
        if category not in list(extraction_attributes):
            return "Please choose a valid category."
    for category in categories:
        if category in ['filing_year', 'zipcode', 'income_level', 'statefips', 'state_code']:
            return "Please choose a attribute  not in ['filing_year', 'state_code', 'zipcode', or 'income_level', or 'statefips'.]"
    if year not in [2015, 2016, 2017, 2018, 2019, 2020, 0]:
        return "Please enter a valid year, either 2015-2020 for a specific year, or 0 for all years."
    if (state_code not in list(state_codes.keys())) and state_code != 'ALL':
        return "Please enter a valid state code or 'ALL' to view all states."

    df = ff_state_incgrp(state_code, year, 0, *categories)

    df_st_agg = df.drop(columns=['income_level'])

    df_st_agg = df_st_agg.groupby(['filing_year', 'state_code']).sum(
        numeric_only=True).reset_index()

    return (df_st_agg)


def ff_state_incgrp_filingtype_pct(state_code: str, year: int, income_level: int) -> pd.DataFrame:
    """
      This function retrieves the number of returns by filing status distribution as percentages (single, joint, head of household, other) given a state code, year, and income group.

      :param state_code: The state code (state abbreviation). Pass in 'All' to retrieve data for all the states, one row per state.
      :param year: A tax year from 2015-2020, inclusive. Pass in 0 to retrieve data for all the filing years, one row per year.
      :param income_level:  [1 for '<$25K', 2 for '$25K - $50K',3 for '$50K - $75K', 4 for '$75K - $100K, 5 for '$100K - $200K',6 for '$200K+', 0 for all income levels, one row per level]

      :return: A dataframe containing filing status distribution percentages, for each income group, by year and at a state level.
    """
    global df_tax_attribute
    global dict_state
    global df_fed_state_filings
    global dict_income_group

    state_codes = dict_state

    if year not in [0, 2015, 2016, 2017, 2018, 2019, 2020]:
        return "Please enter a valid year: either 2015-2020 for a specific year, or 0 for all years."
    if income_level not in [0, 1, 2, 3, 4, 5, 6]:
        return "Please enter a valid income level: either 1-6 for a specific income level, or 0 for all income levels."
    if (state_code not in list(state_codes.keys())) and state_code != 'ALL':
        return "Please enter a valid state code or 'ALL' to view all states."
    df_all = pd.DataFrame()
    if (income_level == 0):
        for income_level in range(1, 7):
            df = ff_called_func.ff_state_incgrp_returntype_distribution(
                state_code, year, income_level)
            if isinstance(df, str):
                return "Please enter valid parameters."
            df_all = pd.concat([df_all, df], ignore_index=True)
        return (df_all)
    elif (income_level >= 1 and income_level < 7):
        df = ff_called_func.ff_state_incgrp_returntype_distribution(
            state_code, year, income_level)
        if isinstance(df, str):
            return "Please enter valid parameters."
        df_all = pd.concat([df_all, df], ignore_index=True)
        return (df_all)
    else:
        return "Please enter valid parameters."


def ff_state_filingtype_pct(state_code: str, year: int):
    """
      This function retrieves the number of returns by filing status distribution as percentages (single, joint, head of household, other) given a state code and year, aggregating across all income groups.

      :param state_code: The state code (state abbreviation). Pass in 'ALL' to retrieve data for all the states, one row per state.
      :param year: A tax year from 2015-2020, inclusive. Pass in 0 to retrieve data for all the filing years, one row per year.

      :return: A dataframe containing filing status distribution percentages when aggregating all income groups, by year, and at a state level.

    """
    global df_tax_attribute
    global dict_state
    global df_fed_state_filings
    global dict_income_group
    income_level_mapping = dict_income_group
    state_codes = dict_state
    df = df_fed_state_filings
    if year not in [0, 2015, 2016, 2017, 2018, 2019, 2020]:
        return "Please enter a valid year: either 2015-2020 for a specific year, or 0 for all years."
    if (state_code not in list(state_codes.keys())) and state_code != 'ALL':
        return "Please enter a valid state code or 'ALL' to view all states."
    if 'year' in df.columns:
        df.rename(columns={'year': 'filing_year'}, inplace=True)
    if 'state' in df.columns:
        df.rename(columns={'state': 'state_code'}, inplace=True)

    df_federal_filing_statelevel = df[['filing_year', 'income_level', 'state_code', 'n_ret',
                                       'nbr_individuals', 'n_ret_single', 'n_ret_head_household', 'n_ret_joint']].copy()

    df_filtered_income_federal_filing_statelevel = df_federal_filing_statelevel.copy()

    if (year != 0):
        df_filtered_income_federal_filing_statelevel_year = df_filtered_income_federal_filing_statelevel[(
            df_filtered_income_federal_filing_statelevel['filing_year'] == year)]
    else:
        df_filtered_income_federal_filing_statelevel_year = df_filtered_income_federal_filing_statelevel.copy()

    if (state_code != 'ALL'):
        df_filtered_incomestate_federal_filing_statelevel = df_filtered_income_federal_filing_statelevel_year.loc[
            df_filtered_income_federal_filing_statelevel_year['state_code'] == state_code]
    else:
        df_filtered_incomestate_federal_filing_statelevel = df_filtered_income_federal_filing_statelevel_year.copy()

    df_final = df_filtered_incomestate_federal_filing_statelevel.groupby(
        ['filing_year', 'state_code', 'income_level']).apply(ff_called_func.transform_group)
    df_final.reset_index(drop=True, inplace=True)
    df_final['income_level_desc'] = df_final['income_level'].replace(
        income_level_mapping)
    selected_columns = ['filing_year', 'state_code',
                        'income_level_desc', 'income_level']
    remaining_columns = [
        col for col in df_final.columns if col not in selected_columns]
    df_final = df_final[selected_columns + remaining_columns]
    df_final = df_final.reset_index(drop=True)

    df_aggregated = df_filtered_incomestate_federal_filing_statelevel.groupby(
        ['filing_year', 'state_code']).apply(ff_called_func.transform_group)
    df_aggregated.reset_index(drop=True, inplace=True)
    df_aggregated['income_level'] = 0
    df_aggregated['income_level_desc'] = 'All'
    df_final = df_aggregated.copy()
    df_final.reset_index(drop=True, inplace=True)

    df_aggregated = df_filtered_incomestate_federal_filing_statelevel.groupby(
        ['filing_year', 'state_code']).apply(ff_called_func.transform_group)
    df_aggregated.reset_index(drop=True, inplace=True)
    df_aggregated['income_level'] = 0
    df_aggregated['income_level_desc'] = 'All'
    df_final = df_aggregated.copy()
    df_final.reset_index(drop=True, inplace=True)

    if 'income_level' in df_final.columns:
        df_final.drop(columns=['income_level'], inplace=True)
    if 'income_level_desc' in df_final.columns:
        df_final.drop(columns=['income_level_desc'], inplace=True)
    if 'number' in df_final.columns:
        df_final.rename(columns={'number': 'n_ret'}, inplace=True)
    df_final['state_level_pct'] = df_final['state_level_pct'].apply(
        lambda x: f"{x}%")

    return df_final


def ff_state_incgrp_distribution(state_code: str, year: int, category: str) -> pd.DataFrame:
    """
      This function retrieves tax filing statistics distributed across the six income levels given a state code, year, and attribute.
      To find the possible tax filing attributes, refer to the function tax_attributes().

      :param state_code: The state code (state abbreviation). Pass in 'All' to retrieve data for all the states, one row per state.
      :param year: A tax year from 2015-2020, inclusive. Pass in 0 to retrieve data for all the filing years, one row per year.
      :param category: Attribute to be retrieved.

      :return: A data frame containing the distribution of the number/amount of returns of an attribute by income level.
    """
    global df_tax_attribute
    global dict_state
    global df_fed_state_filings
    global dict_income_group
    state_codes = dict_state
    df = df_fed_state_filings
    extraction_attributes = df_tax_attribute['variable'].tolist()
    df_federal_filing_statelevel = pd.DataFrame()
    if category not in list(extraction_attributes):
        return "Please choose a valid category."
    if category in ['filing_year', 'zipcode', 'income_level', 'statefips', 'state_code']:
        return "Please choose a category that is not 'filing_year', 'state_code', 'zipcode', or 'income_level', or 'statefips'."
    if year not in [2015, 2016, 2017, 2018, 2019, 2020]:
        return "Please enter a valid year (2015-2020)."
    if (state_code not in list(state_codes.keys())) and state_code != 'ALL':
        return "Please enter a valid state code or 'ALL' to view all states."

    if 'year' in df.columns:
        df.rename(columns={'year': 'filing_year'}, inplace=True)
    if 'state' in df.columns:
        df.rename(columns={'state': 'state_code'}, inplace=True)
    df_temp = df[(df['filing_year'] == year)]
    df_federal_filing_statelevel = df_temp[[
        'filing_year', 'income_level', 'state_code', category]].copy()

    if (state_code != 'ALL'):
        df_filtered_federal_filing_statelevel = df_federal_filing_statelevel.loc[
            df_federal_filing_statelevel['state_code'] == state_code]
    else:
        df_filtered_federal_filing_statelevel = df_federal_filing_statelevel.copy()

    income_dummies = pd.get_dummies(
        df_filtered_federal_filing_statelevel['income_level'], prefix='income_level')

    df_filtered_federal_filing_statelevel = pd.concat(
        [df_filtered_federal_filing_statelevel, income_dummies], axis=1)

    df_filtered_federal_filing_statelevel.drop(
        'income_level', axis=1, inplace=True)

    for income_level in range(1, 7):
        df_filtered_federal_filing_statelevel[f'income_level{income_level}'] = df_filtered_federal_filing_statelevel[
            category] * df_filtered_federal_filing_statelevel[f'income_level_{income_level}']
        df_filtered_federal_filing_statelevel[f'income_level{income_level}'].fillna(
            0, inplace=True)
        df_filtered_federal_filing_statelevel[f'income_level{income_level}'] = df_filtered_federal_filing_statelevel[f'income_level{income_level}'].astype(
            int)

    df_filtered_federal_filing_statelevel.drop(
        income_dummies.columns, axis=1, inplace=True)

    df_filtered_federal_filing_statelevel_agg = df_filtered_federal_filing_statelevel.groupby(
        ['filing_year', 'state_code'], as_index=False).sum()
    column_mapping = {
        'income_level1': f'{category}_income_<_25K',
        'income_level2': f'{category}_income_btwn_25K_50K',
        'income_level3': f'{category}_income_btwn_50K_75K',
        'income_level4': f'{category}_income_btwn_75K_100K',
        'income_level5': f'{category}_income_btwn_100K_200K',
        'income_level6': f'{category}_income_>_200K'
    }
    df_filtered_federal_filing_statelevel_agg.rename(
        columns=column_mapping, inplace=True)
    return (df_filtered_federal_filing_statelevel_agg)


def ff_state_incgrp_pct_change(state_code: str, year1: int, year2: int, income_level: int, *categories: str) -> pd.DataFrame:
    """
      This function retrieves the percent change from the older year to the newer year for selected tax filing attributes given a state code, two distinct years, and an income group.
      To interpret selected tax filing attributes in the resulting dataframe, refer to the function tax_attributes().

      :param state_code: The state code (state abbreviation). Pass in 'All' to retrieve data for all the states, one row per state.
      :param year1: A tax year from 2015-2020, inclusive.
      :param year2: A tax year from 2015-2020, inclusive, different from year1.
      :param income_level:  [1 for '<$25K', 2 for '$25K - $50K',3 for '$50K - $75K', 4 for '$75K - $100K, 5 for '$100K - $200K',6 for '$200K+', 0 for all income levels, one row per level]
      :param *categories: Attributes to be retrieved. e.g. 'n_ret',    'nbr_individuals', 'n_ret_single'. Passing in no attributes will retrieve all of them.

      :return: A dataframe containing the percent change for selected tax filing items by income group and year, at a state level.
    """
    global df_tax_attribute
    global dict_state
    global df_fed_state_filings
    global dict_income_group
    state_codes = dict_state
    df = df_fed_state_filings
    extraction_attributes = df_tax_attribute['variable'].tolist()

    for category in categories:
        if category not in list(extraction_attributes):
            return "Please choose a valid category."
    if year1 not in [2015, 2016, 2017, 2018, 2019, 2020] or year2 not in [2015, 2016, 2017, 2018, 2019, 2020]:
        return "Please enter a valid year, between 2015-2020."
    if year1 == year2:
        return "Years entered are the same. Please enter different years."
    if (state_code not in list(state_codes.keys())) and state_code != 'ALL':
        return "Please enter a valid state code."
    for category in categories:
        if category in ['filing_year', 'zipcode', 'income_level', 'statefips', 'state_code']:
            return "Please choose a category that is not 'filing_year', 'state_code', 'zipcode', or 'income_level', or 'statefips'."
    for category in categories:
        if list(categories).count(category) > 1:
            return "Please enter distinct categories."
    if income_level not in [0, 1, 2, 3, 4, 5, 6]:
        return "Please enter a valid income level: either 1-6 for a specific income level, or 0 for all income levels (aggregated)."

    df_all = pd.DataFrame()
    if (income_level == 0):
        for income_level in range(1, 7):
            df = ff_called_func.ff_each_inc_distr_pct_change(
                state_code, year1, year2, income_level, *categories)
            if isinstance(df, str):
                return "Please enter valid parameters."
            df_all = pd.concat([df_all, df], ignore_index=True)
        return df_all
    elif (income_level >= 1 and income_level < 7):
        df = ff_called_func.ff_each_inc_distr_pct_change(
            state_code, year1, year2, income_level, *categories)
        if isinstance(df, str):
            return "Please enter valid parameters."
        df_all = pd.concat([df_all, df], ignore_index=True)
        return df_all
    else:
        return df_all


def ff_state_pct_change(state_code: str, year1: int, year2: int, *categories: str) -> pd.DataFrame:
    """
      This function retrieves the percent change from the older year to the newer year for selected tax filing attributes given a state code and two distinct years, aggregating across all income levels.
      To interpret selected tax filing attributes in the resulting dataframe, refer to the function tax_attributes().

      :param state_code: The state code (state abbreviation). Pass in 'All' to retrieve data for all the states, one row per state.
      :param year1: A tax year from 2015-2020, inclusive.
      :param year2: A tax year from 2015-2020, inclusive, different from year1.
      :param *categories: Attributes to be retrieved. e.g. 'n_ret',    'nbr_individuals', 'n_ret_single'. Passing in no attributes will retrieve all of them.

      :return: A dataframe containing the percent change for selected tax filing items when aggregating all income groups, by year, and at a state level.
    """
    global df_tax_attribute
    global dict_state
    global df_fed_state_filings
    global dict_income_group
    income_level_mapping = dict_income_group
    state_codes = dict_state
    df_irs_totals = df_fed_state_filings
    extraction_attributes = df_tax_attribute['variable'].tolist()

    for category in categories:
        if category not in list(extraction_attributes):
            return "Please choose a valid category."
    if year1 not in [2015, 2016, 2017, 2018, 2019, 2020] or year2 not in [2015, 2016, 2017, 2018, 2019, 2020]:
        return "Please enter a valid year, between 2015-2020."
    if (state_code not in list(state_codes.keys())) and state_code != 'ALL':
        return "Please enter a valid state code."
    for category in categories:
        if category in ['filing_year', 'zipcode', 'income_level', 'statefips', 'state_code']:
            return "Please choose a category that is not 'filing_year', 'state_code', 'zipcode', or 'income_level', or 'statefips'."
    for category in categories:
        if list(categories).count(category) > 1:
            return "Please enter distinct categories."
    income_level = 0

    df_filtered_1 = df_irs_totals[df_irs_totals['filing_year'] == year1].copy()
    df_filtered_1 = df_filtered_1.reset_index(drop=True)
    df_filtered_2 = df_irs_totals[df_irs_totals['filing_year'] == year2].copy()
    df_filtered_2 = df_filtered_2.reset_index(drop=True)

    df_filtered_1

    for category in categories:
        if df_filtered_1[category].isna().all() or df_filtered_2[category].isna().all():
            return "Please choose a valid category that is in both of the years you entered."

    for category in categories:
        if category not in df_filtered_1.columns:
            return "Please choose categories that are in both of the years you entered."
        if category not in df_filtered_2.columns:
            return "Please choose categories that are in both of the years you entered."

    for category in categories:
        if (category in df_filtered_1.columns and category not in df_filtered_2.columns) or (category in df_filtered_2.columns and category not in df_filtered_1.columns):
            return "Please choose categories that are in both of the years you entered."

    if len(categories) == 0:
        categories = ['state_code',
                      'n_ret',
                      'nbr_individuals',
                      'n_ret_single',
                      'n_ret_head_household',
                      'n_ret_joint',
                      'n_ret_salaries_wages',
                      'amt_salaries_wages',
                      'n_ret_net_investment_income_tax',
                      'amt_net_investment_income_tax',
                      'n_ret_net_capital_gain',
                      'amt_net_capital_gain',
                      'n_ret_ordinary_dividends',
                      'amt_ordinary_dividends',
                      'n_ret_qualified_dividends',
                      'amt_qualified_dividends',
                      'n_ret_itemized_deductions',
                      'amt_agi_itemized_returns',
                      'n_ret_alternative_minimum_tax',
                      'amt_alternative_minimum_tax',
                      'n_ret_home_mortgage_interest_paid',
                      'amt_home_mortgage_interest_paid',
                      'n_ret_real_estate_taxes',
                      'amt_real_estate_taxes',
                      'n_ret_state_local_income_taxes',
                      'amt_state_local_income_taxes',
                      'amt_total_income',
                      'n_ret_total_income',
                      'n_ret_taxable_income',
                      'amt_taxable_income',
                      'n_ret_total_tax_payments',
                      'amt_total_tax_payments',
                      'n_ret_taxable_interest',
                      'amt_taxable_interest',
                      'n_ret_tax_liability',
                      'amt_tax_liability']
        df_last = pd.DataFrame(columns=categories)
    else:
        categories = ('state_code',) + categories
        df_last = pd.DataFrame(columns=categories)

    states = df_irs_totals['state_code'].unique()
    for index, state in enumerate(states):
        df_last.loc[index, 'state_code'] = state
        if income_level == 0:
            df_filtered_1_more = df_filtered_1[df_filtered_1['state_code'] == state]
            df_filtered_2_more = df_filtered_2[df_filtered_2['state_code'] == state]
        if income_level != 0:
            df_filtered_1_more = df_filtered_1[(df_filtered_1['state_code'] == state) & (
                df_filtered_1['income_level'] == income_level)]
            df_filtered_2_more = df_filtered_2[(df_filtered_2['state_code'] == state) & (
                df_filtered_1['income_level'] == income_level)]

        for category in categories[1:]:
            sum_1 = df_filtered_1_more[category].sum()
            sum_2 = df_filtered_2_more[category].sum()
            if year1 < year2:
                if sum_1 != 0:
                    percent_change = (((sum_2 - sum_1) / sum_1) * 100).round(2)
                    df_last.loc[index, category] = f'{percent_change}%'
                elif sum_1 == 0:
                    df_last.loc[index, category] = 'Inf.'
            elif year1 > year2:
                if sum_2 != 0:
                    percent_change = (((sum_1 - sum_2) / sum_2) * 100).round(2)
                    df_last.loc[index, category] = f'{percent_change}%'
                elif sum_2 == 0:
                    df_last.loc[index, category] = 'Inf.'
            else:
                return "Years entered are the same. Please enter different years."

    if year1 < year2:
        df_last['start_yr'] = year1
        df_last['end_yr'] = year2
    else:
        df_last['start_yr'] = year2
        df_last['end_yr'] = year1

    if income_level == 0:
        df_last['income_level'] = 'All'
    if income_level != 0:
        df_last['income_level'] = income_level

    df_last['income_level_desc'] = df_last['income_level'].replace(
        income_level_mapping)
    selected_columns = ['state_code', 'start_yr',
                        'end_yr', 'income_level', 'income_level_desc']
    remaining_columns = [
        col for col in df_last.columns if col not in selected_columns]
    df_last = df_last[selected_columns + remaining_columns]
    df_last.rename(columns={'start_yr': 'filing_start_yr',
                   'end_yr': 'filing_end_yr'}, inplace=True)
    if 'income_level' in df_last.columns:
        df_last.drop(columns=['income_level'], inplace=True)
    if 'income_level_desc' in df_last.columns:
        df_last.drop(columns=['income_level_desc'], inplace=True)
    if state_code == "ALL":
        return df_last
    else:
        df_last_state = df_last[df_last['state_code'] == state_code]
        df_last_state.reset_index(drop=True, inplace=True)
        return df_last_state


def ff_state_county_incgrp(state_code: str, year: int) -> pd.DataFrame:
    """
      This function retrieves tax filing statistics for each county of the specified state code, given the filing year.
      The data is organized by income. To interpret the tax filing attributes in the resulting dataframe, refer to the function tax_attributes().

      :param state_code: The state code (state abbreviation).
      :param year: A tax year from 2015-2020, inclusive.

      :return: A dataframe containing key tax filing statistics, grouped by income level, for each county of the specified state.
    """
    global dict_income_group
    global dict_state
    state_codes = dict_state
    if (state_code not in list(state_codes.keys())):
        return "Please enter a valid state code."
    income_level_mapping = dict_income_group
    url = f'https://us-central1-taxfiling-aggregation.cloudfunctions.net/fn-ff-fetch?p_01={year}&p_statecode={state_code}&p_type=cnt&p_countyall=1'
    response = requests.get(url, timeout=60)
    if response.status_code == 200:
        json_data = response.json()
        dfcnt = pd.DataFrame(json_data)
        if dfcnt.empty:
            return "Please enter a valid state code and year."
    else:
        print("Failed to fetch data from the URL.")
        return None

    url = f'https://us-central1-taxfiling-aggregation.cloudfunctions.net/fn-ff-fetch?p_01={year}&p_statecode={state_code}&p_type=amt&p_countyall=1'
    response = requests.get(url, timeout=60)
    responsedata = response.json()
    data = pd.DataFrame(responsedata)
    dfamt = pd.DataFrame(data)
    if response.status_code == 200:
        json_data = response.json()
        dfamt = pd.DataFrame(json_data)
        if dfamt.empty:
            return "Please enter a valid state code and year."
    else:
        print("Failed to fetch data from the URL.")
        return None
    dfamt = dfamt.drop('n_ret', axis=1)
    dfamt.rename(columns=lambda x: x.strip(), inplace=True)
    dfcnt.rename(columns=lambda x: x.strip(), inplace=True)
    df = pd.merge(dfamt, dfcnt, on=[
                  'state_code', 'division', 'region', 'year', 'income_level', 'state', 'county'])

    if 'year' in df.columns:
        df.rename(columns={'year': 'filing_year'}, inplace=True)
    df['income_level_desc'] = df['income_level'].replace(income_level_mapping)
    selected_columns = ['filing_year', 'income_level', 'income_level_desc']
    remaining_columns = [
        col for col in df.columns if col not in selected_columns]
    df = df[selected_columns + remaining_columns]
    df_sorted = df.sort_values(by=['filing_year', 'income_level', 'state_code',
                               'division', 'region', 'state', 'county'], ascending=True)
    df_sorted.reset_index(drop=True, inplace=True)
    return df_sorted


def ff_state_city_incgrp(state_code: str, filing_year: int) -> pd.DataFrame:
    """
      This function retrieves tax filing statistics for each city of the specified state code, given the filing year.
      The data is organized by income. To interpret the tax filing attributes in the resulting dataframe, refer to the function tax_attributes().

      :param state_code: The state code (state abbreviation).
      :param year: A tax year from 2015-2020, inclusive.

      :return: A dataframe containing key tax filing statistics, grouped by income level, for each city of the specified state.
    """
    global dict_income_group
    global dict_state
    state_codes = dict_state
    if (state_code not in list(state_codes.keys())):
        return "Please enter a valid state code."
    income_level_mapping = dict_income_group
    url = f'https://us-central1-taxfiling-aggregation.cloudfunctions.net/fn-ff-fetch?p_01={filing_year}&p_statecode={state_code}&p_type=cnt&p_cityall=1'
    response = requests.get(url, timeout=60)
    if response.status_code == 200:
        json_data = response.json()
        dfcnt = pd.DataFrame(json_data)
        if dfcnt.empty:
            return "Please enter a valid state code and year."
    else:
        print("Failed to fetch data from the URL.")
        return None

    url = f'https://us-central1-taxfiling-aggregation.cloudfunctions.net/fn-ff-fetch?p_01={filing_year}&p_statecode={state_code}&p_type=amt&p_cityall=1'
    response = requests.get(url, timeout=60)
    responsedata = response.json()
    data = pd.DataFrame(responsedata)
    dfamt = pd.DataFrame(data)
    if response.status_code == 200:
        json_data = response.json()
        dfamt = pd.DataFrame(json_data)
        if dfamt.empty:
            return "Please enter a valid state code and year."
    else:
        print("Failed to fetch data from the URL.")
        return None
    dfamt = dfamt.drop('n_ret', axis=1)
    dfamt.rename(columns=lambda x: x.strip(), inplace=True)
    dfcnt.rename(columns=lambda x: x.strip(), inplace=True)
    df = pd.merge(dfamt, dfcnt, on=[
                  'state_code', 'division', 'region', 'year', 'income_level', 'state', 'city_name'])

    if 'year' in df.columns:
        df.rename(columns={'year': 'filing_year'}, inplace=True)
    df['income_level_desc'] = df['income_level'].replace(income_level_mapping)
    selected_columns = ['filing_year', 'income_level', 'income_level_desc']
    remaining_columns = [
        col for col in df.columns if col not in selected_columns]
    df = df[selected_columns + remaining_columns]
    df_sorted = df.sort_values(by=['filing_year', 'income_level', 'state_code',
                               'division', 'region', 'state', 'city_name'], ascending=True)
    df_sorted.reset_index(drop=True, inplace=True)
    return df_sorted
