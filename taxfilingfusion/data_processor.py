import pandas as pd
import requests


from taxfilingfusion import data_collector
data_collector.init()
df_tax_attribute = data_collector.df_tax_attribute
df_fed_state_filings = data_collector.df_fed_state_filings
dict_state = data_collector.dict_state
dict_income_group = data_collector.dict_income_group


def ff_st_co_ziplevel(state_code: str, county_name: str) -> pd.DataFrame:
    """Takes two parameters: a state code and the name of a county or partial name of a county. It will return a dataframe of the key tax filing statistics for each zipcode of that county, grouped by income level and year. Use tax_dict() method to understand the tax filing attributes in this dataframe."""
    global dict_income_group
    url = f"https://us-central1-taxfiling-aggregation.cloudfunctions.net/fed-tax-data-get?p_statecode={state_code}&p_countyname={county_name}&p_type=cnt"
    response = requests.get(url, timeout=60)
    if response.status_code == 200:
        json_data = response.json()
        dfcnt = pd.DataFrame(json_data)
        if dfcnt.empty:
            return "Please enter a valid state code and county name."
    else:
        print("Failed to fetch data from the URL.")
        return None
    url = f"https://us-central1-taxfiling-aggregation.cloudfunctions.net/fed-tax-data-get?p_statecode={state_code}&p_countyname={county_name}&p_type=amt"
    response = requests.get(url, timeout=60)
    if response.status_code == 200:
        json_data = response.json()
        dfamt = pd.DataFrame(json_data)
        if dfamt.empty:
            return "Please enter a valid state code and county name."
    else:
        print("Failed to fetch data from the URL.")
        return None
    dfamt = dfamt.drop("n_ret", axis=1)
    dfamt.rename(columns=lambda x: x.strip(), inplace=True)
    dfcnt.rename(columns=lambda x: x.strip(), inplace=True)
    df = pd.merge(
        dfamt,
        dfcnt,
        on=[
            "year",
            "income_level",
            "state_code",
            "division",
            "region",
            "state",
            "zip_code",
            "city_name",
            "county",
        ],
    )

    if "year" in df.columns:
        df.rename(columns={"year": "filing_year"}, inplace=True)
    df["income_level_desc"] = df["income_level"].replace(dict_income_group)
    selected_columns = ["filing_year", "income_level", "income_level_desc"]
    remaining_columns = [
        col for col in df.columns if col not in selected_columns]
    df = df[selected_columns + remaining_columns]
    df_sorted = df.sort_values(
        by=[
            "filing_year",
            "income_level",
            "state_code",
            "division",
            "region",
            "state",
            "zip_code",
            "city_name",
            "county",
        ],
        ascending=True,
    )
    df_sorted.reset_index(drop=True, inplace=True)
    return df_sorted


def ff_st_co_agg(state_code: str, county_name: str) -> pd.DataFrame:
    """Takes two parameters: a state code and the name of a county or partial name of a county. It will return a dataframe of the key tax filing statistics for each zipcode of that county, grouped by year. Use tax_dict() method to understand the tax filing attributes in this dataframe."""
    df = ff_st_co_ziplevel(state_code, county_name)

    aggregation_functions = {
        "n_ret": "sum",
        "amt_total_income": "sum",
        "amt_taxable_income": "sum",
        "amt_tax_liability": "sum",
        "amt_total_tax_payments": "sum",
        "amt_total_taxes_paid": "sum",
        "agi": "sum",
        "amt_alternative_minimum_tax": "sum",
        "amt_excess_advance_premium_tax_credit_repayment": "sum",
        "amt_self_employment_tax": "sum",
        "amt_child_other_dependent_credit": "sum",
        "amt_foreign_tax_credit": "sum",
        "amt_child_dependent_care_credit": "sum",
        "amt_nonrefundable_education_credit": "sum",
        "amt_retirement_savings_contribution_credit": "sum",
        "amt_residential_energy_tax_credit": "sum",
        "amt_salaries_wages": "sum",
        "amt_taxable_interest": "sum",
        "amt_qualified_dividends": "sum",
        "amt_ordinary_dividends": "sum",
        "amt_unemployment_compensation": "sum",
        "amt_taxable_ira_distributions": "sum",
        "amt_taxable_pensions_annuities": "sum",
        "amt_ira_pensions": "sum",
        "amt_net_capital_gain": "sum",
        "amt_taxable_social_security_benefits": "sum",
        "amt_business_professional_net_income": "sum",
        "amt_partnership_s_corp_net_income": "sum",
        "nbr_individuals": "sum",
        "nbr_dep": "sum",
        "n_ret_electronic": "sum",
        "n_ret_head_household": "sum",
        "n_ret_joint": "sum",
        "n_ret_itemized_deductions": "sum",
        "n_ret_total_income": "sum",
        "n_ret_taxable_income": "sum",
        "n_ret_tax_liability": "sum",
        "n_ret_total_tax_payments": "sum",
        "n_ret_total_taxes_paid": "sum",
        "n_ret_alternative_minimum_tax": "sum",
        "n_ret_excess_advance_premium_tax_credit_repayment": "sum",
        "n_ret_self_employment_tax": "sum",
        "n_ret_child_other_dependent_credit": "sum",
        "n_ret_foreign_tax_credit": "sum",
        "n_ret_child_dependent_care_credit": "sum",
        "n_ret_nonrefundable_education_credit": "sum",
        "n_ret_retirement_savings_contribution_credit": "sum",
        "n_ret_residential_energy_tax_credit": "sum",
        "n_ret_salaries_wages": "sum",
        "n_ret_taxable_interest": "sum",
        "n_ret_qualified_dividends": "sum",
        "n_ret_ordinary_dividends": "sum",
        "n_ret_unemployment_compensation": "sum",
        "n_ret_taxable_ira_distributions": "sum",
        "n_ret_taxable_pensions_annuities": "sum",
        "n_ret_ira_pensions": "sum",
        "n_ret_net_capital_gain": "sum",
        "n_ret_taxable_social_security_benefits": "sum",
        "n_ret_business_professional_net_income": "sum",
        "n_ret_partnership_s_corp_net_income": "sum",
    }

    df_aggregated = (
        df.groupby(
            [
                "filing_year",
                "state_code",
                "division",
                "region",
                "state",
                "zip_code",
                "city_name",
                "county",
            ]
        )
        .agg(aggregation_functions)
        .reset_index()
    )
    if "n_ret_x" in df_aggregated.columns:
        df_aggregated.rename(columns={"n_ret_x": "n_ret"}, inplace=True)
    return df_aggregated


def ff_st_city_ziplevel(state_code: str, city_name: str) -> pd.DataFrame:
    """Takes two parameters: a state code and the name of a city or partial name of a city. It will return a dataframe of the key tax filing statistics for each zipcode of that county, grouped by income level and year. Use tax_dict() method to understand the tax filing attributes in this dataframe."""
    global dict_income_group
    income_level_mapping = dict_income_group
    url = f"https://us-central1-taxfiling-aggregation.cloudfunctions.net/fed-tax-data-get?p_statecode={state_code}&p_cityname={city_name}&p_type=cnt"
    response = requests.get(url, timeout=60)
    if response.status_code == 200:
        json_data = response.json()
        dfcnt = pd.DataFrame(json_data)
        if dfcnt.empty:
            return "Please enter a valid state code and city name."
    else:
        print("Failed to fetch data from the URL.")
        return None
    url = f"https://us-central1-taxfiling-aggregation.cloudfunctions.net/fed-tax-data-get?p_statecode={state_code}&p_cityname={city_name}&p_type=amt"
    response = requests.get(url, timeout=60)
    if response.status_code == 200:
        json_data = response.json()
        dfamt = pd.DataFrame(json_data)
        if dfamt.empty:
            return "Please enter a valid state code and city name."
    else:
        print("Failed to fetch data from the URL.")
        return None

    dfamt = dfamt.drop("n_ret", axis=1)
    df = pd.merge(
        dfamt,
        dfcnt,
        on=[
            "year",
            "income_level",
            "state_code",
            "division",
            "region",
            "state",
            "zip_code",
            "city_name",
            "county",
        ],
    )

    if "year" in df.columns:
        df.rename(columns={"year": "filing_year"}, inplace=True)
    df["income_level_desc"] = df["income_level"].replace(income_level_mapping)
    selected_columns = ["filing_year", "income_level", "income_level_desc"]
    remaining_columns = [
        col for col in df.columns if col not in selected_columns]
    df = df[selected_columns + remaining_columns]
    df_sorted = df.sort_values(
        by=[
            "filing_year",
            "income_level",
            "state_code",
            "division",
            "region",
            "state",
            "zip_code",
            "city_name",
            "county",
        ],
        ascending=True,
    )
    df_sorted.reset_index(drop=True, inplace=True)
    return df_sorted


def ff_st_city_agg(state_code: str, city_name: str) -> pd.DataFrame:
    """Takes two parameters: a state code and the name of a city or partial name of a city. It will return a dataframe of the key tax filing statistics for each zipcode of that county, grouped by year. Use tax_dict() method to understand the tax filing attributes in this dataframe."""
    df = ff_st_city_ziplevel(state_code, city_name)

    aggregation_functions = {
        "n_ret": "sum",
        "amt_total_income": "sum",
        "amt_taxable_income": "sum",
        "amt_tax_liability": "sum",
        "amt_total_tax_payments": "sum",
        "amt_total_taxes_paid": "sum",
        "agi": "sum",
        "amt_alternative_minimum_tax": "sum",
        "amt_excess_advance_premium_tax_credit_repayment": "sum",
        "amt_self_employment_tax": "sum",
        "amt_child_other_dependent_credit": "sum",
        "amt_foreign_tax_credit": "sum",
        "amt_child_dependent_care_credit": "sum",
        "amt_nonrefundable_education_credit": "sum",
        "amt_retirement_savings_contribution_credit": "sum",
        "amt_residential_energy_tax_credit": "sum",
        "amt_salaries_wages": "sum",
        "amt_taxable_interest": "sum",
        "amt_qualified_dividends": "sum",
        "amt_ordinary_dividends": "sum",
        "amt_unemployment_compensation": "sum",
        "amt_taxable_ira_distributions": "sum",
        "amt_taxable_pensions_annuities": "sum",
        "amt_ira_pensions": "sum",
        "amt_net_capital_gain": "sum",
        "amt_taxable_social_security_benefits": "sum",
        "amt_business_professional_net_income": "sum",
        "amt_partnership_s_corp_net_income": "sum",
        "nbr_individuals": "sum",
        "nbr_dep": "sum",
        "n_ret_electronic": "sum",
        "n_ret_head_household": "sum",
        "n_ret_joint": "sum",
        "n_ret_itemized_deductions": "sum",
        "n_ret_total_income": "sum",
        "n_ret_taxable_income": "sum",
        "n_ret_tax_liability": "sum",
        "n_ret_total_tax_payments": "sum",
        "n_ret_total_taxes_paid": "sum",
        "n_ret_alternative_minimum_tax": "sum",
        "n_ret_excess_advance_premium_tax_credit_repayment": "sum",
        "n_ret_self_employment_tax": "sum",
        "n_ret_child_other_dependent_credit": "sum",
        "n_ret_foreign_tax_credit": "sum",
        "n_ret_child_dependent_care_credit": "sum",
        "n_ret_nonrefundable_education_credit": "sum",
        "n_ret_retirement_savings_contribution_credit": "sum",
        "n_ret_residential_energy_tax_credit": "sum",
        "n_ret_salaries_wages": "sum",
        "n_ret_taxable_interest": "sum",
        "n_ret_qualified_dividends": "sum",
        "n_ret_ordinary_dividends": "sum",
        "n_ret_unemployment_compensation": "sum",
        "n_ret_taxable_ira_distributions": "sum",
        "n_ret_taxable_pensions_annuities": "sum",
        "n_ret_ira_pensions": "sum",
        "n_ret_net_capital_gain": "sum",
        "n_ret_taxable_social_security_benefits": "sum",
        "n_ret_business_professional_net_income": "sum",
        "n_ret_partnership_s_corp_net_income": "sum",
    }

    df_aggregated = (
        df.groupby(
            [
                "filing_year",
                "state_code",
                "division",
                "region",
                "state",
                "zip_code",
                "city_name",
                "county",
            ]
        )
        .agg(aggregation_functions)
        .reset_index()
    )

    return df_aggregated


def ff_zips(zipcodes: list) -> pd.DataFrame:
    """Takes in one parameter: a list of zipcodes. It will return a dataframe of key tax filing statistics for each zipcode listed, grouped by income level and year. Use tax_dict() method to understand the tax filing attributes in this dataframe."""
    if not isinstance(zipcodes, list):
        return "Please enter a list of ZIP code(s)."
    global dict_income_group
    income_level_mapping = dict_income_group
    zip_list = ",".join(str(zipcode) for zipcode in zipcodes)
    url = f"https://us-central1-taxfiling-aggregation.cloudfunctions.net/fed-tax-data-get?p_zipcodes={zip_list}&p_type=cnt"
    response = requests.get(url, timeout=60)
    if response.status_code == 200:
        json_data = response.json()
        dfcnt = pd.DataFrame(json_data)
        if dfcnt.empty:
            return "Please enter valid ZIP code(s)."
        if "zip_code" in dfcnt.columns:
            for zipcode in zipcodes:
                if zipcode not in dfcnt["zip_code"].unique():
                    return "Please enter valid ZIP code(s)."
    else:
        print("Failed to fetch data from the URL.")
        return None
    url = f"https://us-central1-taxfiling-aggregation.cloudfunctions.net/fed-tax-data-get?p_zipcodes={zip_list}&p_type=amt"
    response = requests.get(url, timeout=60)
    if response.status_code == 200:
        json_data = response.json()
        dfamt = pd.DataFrame(json_data)
        if dfamt.empty:
            return "Please enter valid ZIP codes."
        if "zip_code" in dfamt.columns:
            for zipcode in zipcodes:
                if zipcode not in dfamt["zip_code"].unique():
                    return "Please enter valid ZIP codes."
    else:
        print("Failed to fetch data from the URL.")
        return None
    dfcnt = dfcnt.drop("n_ret", axis=1)
    df = pd.merge(dfamt, dfcnt, on=["filing_year", "income_level", "zip_code"])
    if "year" in df.columns:
        df.rename(columns={"year": "filing_year"}, inplace=True)
    df["income_level_desc"] = df["income_level"].replace(income_level_mapping)
    selected_columns = ["filing_year", "income_level", "income_level_desc"]
    remaining_columns = [
        col for col in df.columns if col not in selected_columns]
    df = df[selected_columns + remaining_columns]
    df_sorted = df.sort_values(
        by=["filing_year", "income_level", "zip_code"], ascending=True
    )
    df_sorted.reset_index(drop=True, inplace=True)
    return df_sorted


def geo_st_co_city(state_code: str, county_name: str, city_name: str) -> pd.DataFrame:
    """Takes two parameters: a state code, the name of a county or partial name of a county, and the name of a city or partial name of a city. It will return a dataframe of the key tax filing statistics for each zipcode corresponding to the given city of the given county in the given state."""
    url = f"https://us-central1-taxfiling-aggregation.cloudfunctions.net/fed-tax-data-get?p_statecode={state_code}&p_countyname={county_name}&p_cityname={city_name}&p_geo=yes"
    response = requests.get(url, timeout=60)
    if response.status_code == 200:
        json_data = response.json()
        df = pd.DataFrame(json_data)
        if df.empty:
            return "Please enter a valid state, county name,city name."
    else:
        print("Failed to fetch data from the URL.")
        return None
    return df


def ff_st(
    state_code: str, year: int, income_level: int, *categories: str
) -> pd.DataFrame:
    """Takes in at least three parameters: a state code, the year, the income level, and any number of categories. It will return a dataframe of the sums for each of those categories for the desired income levels (0 for all income levels), states ('ALL' for all states), and years (0 for all years)."""
    global df_tax_attribute
    global dict_state
    global df_fed_state_filings
    global dict_income_group
    state_codes = dict_state
    extraction_attributes = df_tax_attribute["variable"].tolist()
    df_federal_filing_statelevel = df_fed_state_filings.copy()
    income_level_mapping = dict_income_group
    if "state" in df_federal_filing_statelevel.columns:
        df_federal_filing_statelevel.rename(
            columns={"state": "state_code"}, inplace=True
        )
    if "year" in df_federal_filing_statelevel.columns:
        df_federal_filing_statelevel.rename(
            columns={"year": "filing_year"}, inplace=True
        )
    for category in categories:
        if list(categories).count(category) > 1:
            return "Please enter distinct categories."
    for category in categories:
        if category not in list(extraction_attributes):
            return "Please choose a valid category."
    for category in categories:
        if category in [
            "filing_year",
            "zipcode",
            "income_level",
            "statefips",
            "state_code",
        ]:
            return "Please choose a attribute  not in ['filing_year', 'state_code', 'zipcode', or 'income_level', or 'statefips'.]"
    if year not in [2015, 2016, 2017, 2018, 2019, 2020, 0]:
        return "Please enter a valid year, either 2015-2020 for a specific year, or 0 for all years."
    if income_level not in [0, 1, 2, 3, 4, 5, 6]:
        return "Please enter a valid income level: either 1-6 for a specific income level, or 0 for all income levels."
    if year != 0:
        df_filtered_year = df_federal_filing_statelevel[
            (df_federal_filing_statelevel["filing_year"] == year)
        ]
    else:
        df_filtered_year = df_federal_filing_statelevel.copy()
    if (state_code not in list(state_codes.keys())) and state_code != "ALL":
        return "Please enter a valid state code or 'ALL' to view all states."
    if state_code != "ALL":
        df_filtered_yearstate = df_filtered_year.loc[
            df_filtered_year["state_code"] == state_code
        ]
    else:
        df_filtered_yearstate = df_filtered_year.copy()
    if income_level != 0:
        df_filtered_yearstateincome = df_filtered_yearstate[
            (df_filtered_yearstate["income_level"] == income_level)
        ]
    else:
        df_filtered_yearstateincome = df_filtered_yearstate.copy()
    if len(categories) == 0:
        df_final = df_filtered_yearstateincome[
            [
                "filing_year",
                "income_level",
                "state_code",
                "n_ret",
                "nbr_individuals",
                "n_ret_single",
                "n_ret_head_household",
                "n_ret_joint",
                "n_ret_salaries_wages",
                "amt_salaries_wages",
                "n_ret_net_investment_income_tax",
                "amt_net_investment_income_tax",
                "n_ret_net_capital_gain",
                "amt_net_capital_gain",
                "n_ret_ordinary_dividends",
                "amt_ordinary_dividends",
                "n_ret_qualified_dividends",
                "amt_qualified_dividends",
                "n_ret_itemized_deductions",
                "amt_agi_itemized_returns",
                "n_ret_alternative_minimum_tax",
                "amt_alternative_minimum_tax",
                "n_ret_home_mortgage_interest_paid",
                "amt_home_mortgage_interest_paid",
                "n_ret_real_estate_taxes",
                "amt_real_estate_taxes",
                "n_ret_state_local_income_taxes",
                "amt_state_local_income_taxes",
                "amt_total_income",
                "n_ret_total_income",
                "n_ret_taxable_income",
                "amt_taxable_income",
                "n_ret_total_tax_payments",
                "amt_total_tax_payments",
                "n_ret_taxable_interest",
                "amt_taxable_interest",
                "n_ret_tax_liability",
                "amt_tax_liability",
            ]
        ].copy()
    else:
        extraction_list = ["filing_year", "income_level", "state_code"]
        extraction_list += list(categories)
        df_final = df_filtered_yearstateincome[list(extraction_list)].copy()
    if "year" in df_final.columns:
        df_final.rename(columns={"year": "filing_year"}, inplace=True)
    df_final["income_level_desc"] = df_final["income_level"].replace(
        income_level_mapping
    )
    selected_columns = [
        "filing_year",
        "state_code",
        "income_level_desc",
        "income_level",
    ]
    remaining_columns = [
        col for col in df_final.columns if col not in selected_columns]
    df_final = df_final[selected_columns + remaining_columns]
    df_final = df_final.reset_index(drop=True)
    if income_level == 0 and state_code != "ALL" and year != 0:
        df_final = df_final.sort_values(by="income_level", ascending=True)
        df_final.reset_index(drop=True, inplace=True)
    return df_final


def ff_st_agg(state_code: str, year: int, *categories: str) -> pd.DataFrame:
    """Takes in at least two parameters: a state code, the year, and any number of categories. It will return a dataframe of the sums for each of those categories for the desire states ('ALL' for all states) and years (0 for all years), aggregating all income levels."""
    global df_tax_attribute
    global dict_state
    global df_fed_state_filings
    global dict_income_group
    state_codes = dict_state
    extraction_attributes = df_tax_attribute["variable"].tolist()
    df_federal_filing_statelevel = df_fed_state_filings.copy()
    income_level_mapping = dict_income_group

    for category in categories:
        if list(categories).count(category) > 1:
            return "Please enter distinct categories."
    for category in categories:
        if category not in list(extraction_attributes):
            return "Please choose a valid category."
    for category in categories:
        if category in [
            "filing_year",
            "zipcode",
            "income_level",
            "statefips",
            "state_code",
        ]:
            return "Please choose a attribute  not in ['filing_year', 'state_code', 'zipcode', or 'income_level', or 'statefips'.]"
    if year not in [2015, 2016, 2017, 2018, 2019, 2020, 0]:
        return "Please enter a valid year, either 2015-2020 for a specific year, or 0 for all years."
    if (state_code not in list(state_codes.keys())) and state_code != "ALL":
        return "Please enter a valid state code or 'ALL' to view all states."

    df = ff_st(state_code, year, 0, *categories)

    # Drop the 'income_level' column
    df_st_agg = df.drop(columns=["income_level"])

    # Group by 'filing_year' and 'state_code' and aggregate the data
    df_st_agg = (
        df_st_agg.groupby(["filing_year", "state_code"])
        .sum(numeric_only=True)
        .reset_index()
    )

    return df_st_agg


def ff_st_each_inc_distr(state_code: str, year: int, income_level: int) -> pd.DataFrame:
    """Takes in three parameters: the state code, the year, and the income level. It will return a dataframe of the distribution of the types of returns by filing status out of the total number of returns for the specified income level (0 to aggregate all income levels), given the state ('ALL' for all states) and year (0 for all years)."""
    global df_tax_attribute
    global dict_state
    global df_fed_state_filings
    global dict_income_group
    income_level_mapping = dict_income_group
    state_codes = dict_state
    df = df_fed_state_filings
    if year not in [0, 2015, 2016, 2017, 2018, 2019, 2020]:
        return "Please enter a valid year: either 2015-2020 for a specific year, or 0 for all years."
    if income_level not in [0, 1, 2, 3, 4, 5, 6]:
        return "Please enter a valid income level: either 1-6 for a specific income level, or 0 for all income levels."
    if (state_code not in list(state_codes.keys())) and state_code != "ALL":
        return "Please enter a valid state code or 'ALL' to view all states."
    if "year" in df.columns:
        df.rename(columns={"year": "filing_year"}, inplace=True)
    if "state" in df.columns:
        df.rename(columns={"state": "state_code"}, inplace=True)

    df_federal_filing_statelevel = df[
        [
            "filing_year",
            "income_level",
            "state_code",
            "n_ret",
            "nbr_individuals",
            "n_ret_single",
            "n_ret_head_household",
            "n_ret_joint",
        ]
    ].copy()

    if income_level != 0:
        df_filtered_income_federal_filing_statelevel = df_federal_filing_statelevel[
            (df_federal_filing_statelevel["income_level"] == income_level)
        ]
    else:
        df_filtered_income_federal_filing_statelevel = (
            df_federal_filing_statelevel.copy()
        )

    if year != 0:
        df_filtered_income_federal_filing_statelevel_year = (
            df_filtered_income_federal_filing_statelevel[
                (df_filtered_income_federal_filing_statelevel["filing_year"] == year)
            ]
        )
    else:
        df_filtered_income_federal_filing_statelevel_year = (
            df_filtered_income_federal_filing_statelevel.copy()
        )

    if state_code != "ALL":
        df_filtered_incomestate_federal_filing_statelevel = (
            df_filtered_income_federal_filing_statelevel_year.loc[
                df_filtered_income_federal_filing_statelevel_year["state_code"]
                == state_code
            ]
        )
    else:
        df_filtered_incomestate_federal_filing_statelevel = (
            df_filtered_income_federal_filing_statelevel_year.copy()
        )

    # Apply the transformation function to each group
    df_final = df_filtered_incomestate_federal_filing_statelevel.groupby(
        ["filing_year", "state_code", "income_level"]
    ).apply(transform_group)
    df_final.reset_index(drop=True, inplace=True)
    df_final["income_level_desc"] = df_final["income_level"].replace(
        income_level_mapping
    )
    selected_columns = [
        "filing_year",
        "state_code",
        "income_level_desc",
        "income_level",
    ]
    remaining_columns = [
        col for col in df_final.columns if col not in selected_columns]
    df_final = df_final[selected_columns + remaining_columns]
    df_final = df_final.reset_index(drop=True)

    if income_level == 0:
        # Aggregate across all income levels
        df_aggregated = df_filtered_incomestate_federal_filing_statelevel.groupby(
            ["filing_year", "state_code"]
        ).apply(transform_group)
        df_aggregated.reset_index(drop=True, inplace=True)
        df_aggregated["income_level"] = 0
        df_aggregated["income_level_desc"] = "All"
        df_final = df_aggregated.copy()
        df_final.reset_index(drop=True, inplace=True)

    if "number" in df_final.columns:
        df_final.rename(columns={"number": "n_ret"}, inplace=True)

    df_final["state_level_pct"] = df_final["state_level_pct"].apply(
        lambda x: f"{x}%")

    return df_final


def transform_group(group):
    group.fillna(0, inplace=True)
    n1 = group["n_ret"].sum()
    mars1 = group["n_ret_single"].sum()
    mars2 = group["n_ret_joint"].sum()
    mars4 = group["n_ret_head_household"].sum()
    other = n1 - (mars1 + mars2 + mars4)
    mars1_pct = (mars1 / n1) * 100
    mars2_pct = (mars2 / n1) * 100
    mars4_pct = (mars4 / n1) * 100
    other_pct = 100 - (mars1_pct + mars2_pct + mars4_pct)

    data = [
        {
            "filing_year": group["filing_year"].iloc[0],
            "state_code": group["state_code"].iloc[0],
            "income_level": group["income_level"].iloc[0],
            "type": "Single",
            "number": mars1,
            "state_level_pct": f"{mars1_pct:.2f}",
        },
        {
            "filing_year": group["filing_year"].iloc[0],
            "state_code": group["state_code"].iloc[0],
            "income_level": group["income_level"].iloc[0],
            "type": "Joint",
            "number": mars2,
            "state_level_pct": f"{mars2_pct:.2f}",
        },
        {
            "filing_year": group["filing_year"].iloc[0],
            "state_code": group["state_code"].iloc[0],
            "income_level": group["income_level"].iloc[0],
            "type": "Head of Household",
            "number": mars4,
            "state_level_pct": f"{mars4_pct:.2f}",
        },
        {
            "filing_year": group["filing_year"].iloc[0],
            "state_code": group["state_code"].iloc[0],
            "income_level": group["income_level"].iloc[0],
            "type": "Other",
            "number": other,
            "state_level_pct": f"{other_pct:.2f}",
        },
    ]
    return pd.DataFrame(data)


def ff_st_all_inc_distr(state_code: str, year: int) -> pd.DataFrame:
    """Takes in three parameters: the state code, the year, and the income level. It will return a dataframe of the distribution of the types of returns by filing status out of the total number of returns for each and every income level, given the state ('ALL' for all states) and year (0 for all years)."""
    global df_tax_attribute
    global dict_state
    global df_fed_state_filings
    global dict_income_group
    state_codes = dict_state
    extraction_attributes = df_tax_attribute["variable"].tolist()
    df_federal_filing_statelevel = df_fed_state_filings.copy()
    income_level_mapping = dict_income_group

    if year not in [2015, 2016, 2017, 2018, 2019, 2020, 0]:
        return "Please enter a valid year, either 2015-2020 for a specific year, or 0 for all years."
    if (state_code not in list(state_codes.keys())) and state_code != "ALL":
        return "Please enter a valid state code or 'ALL' to view all states."
    df_all = pd.DataFrame()
    for income_level in range(1, 7):
        df = ff_st_each_inc_distr(state_code, year, income_level)
        df_all = pd.concat([df_all, df], ignore_index=True)
    return df_all


def ff_st_inc_range(state_code: str, year: int, category: str) -> pd.DataFrame:
    """Takes in three parameters: the state code, the year, and a category. It will return a dataframe of the distribution of the given category for each income level, given the state ('ALL' for all states) and year."""
    global df_tax_attribute
    global dict_state
    global df_fed_state_filings
    global dict_income_group
    income_level_mapping = dict_income_group
    state_codes = dict_state
    df = df_fed_state_filings
    extraction_attributes = df_tax_attribute["variable"].tolist()
    df_federal_filing_statelevel = pd.DataFrame()
    if category not in list(extraction_attributes):
        return "Please choose a valid category."
    if category in [
        "filing_year",
        "zipcode",
        "income_level",
        "statefips",
        "state_code",
    ]:
        return "Please choose a category that is not 'filing_year', 'state_code', 'zipcode', or 'income_level', or 'statefips'."
    if year not in [2015, 2016, 2017, 2018, 2019, 2020]:
        return "Please enter a valid year (2015-2020)."
    if (state_code not in list(state_codes.keys())) and state_code != "ALL":
        return "Please enter a valid state code or 'ALL' to view all states."

    if "year" in df.columns:
        df.rename(columns={"year": "filing_year"}, inplace=True)
    if "state" in df.columns:
        df.rename(columns={"state": "state_code"}, inplace=True)
    df_temp = df[(df["filing_year"] == year)]
    df_federal_filing_statelevel = df_temp[
        ["filing_year", "income_level", "state_code", category]
    ].copy()

    if state_code != "ALL":
        df_filtered_federal_filing_statelevel = df_federal_filing_statelevel.loc[
            df_federal_filing_statelevel["state_code"] == state_code
        ]
    else:
        df_filtered_federal_filing_statelevel = df_federal_filing_statelevel.copy()

    # Convert income_level to dummies
    income_dummies = pd.get_dummies(
        df_filtered_federal_filing_statelevel["income_level"], prefix="income_level"
    )

    # Concatenate the dummy columns with the original DataFrame
    df_filtered_federal_filing_statelevel = pd.concat(
        [df_filtered_federal_filing_statelevel, income_dummies], axis=1
    )

    # Drop the original income_level column
    df_filtered_federal_filing_statelevel.drop(
        "income_level", axis=1, inplace=True)

    # Create new columns for each income level with n_ret values or 0
    for income_level in range(1, 7):
        df_filtered_federal_filing_statelevel[f"income_level{income_level}"] = (
            df_filtered_federal_filing_statelevel[category]
            * df_filtered_federal_filing_statelevel[f"income_level_{income_level}"]
        )
        df_filtered_federal_filing_statelevel[f"income_level{income_level}"].fillna(
            0, inplace=True
        )
        df_filtered_federal_filing_statelevel[
            f"income_level{income_level}"
        ] = df_filtered_federal_filing_statelevel[f"income_level{income_level}"].astype(
            int
        )

    # Drop the original n_ret column and income level dummy columns
    df_filtered_federal_filing_statelevel.drop(
        income_dummies.columns, axis=1, inplace=True
    )

    # Aggregate the DataFrame by summing values for each filing_year and state_code
    df_filtered_federal_filing_statelevel_agg = (
        df_filtered_federal_filing_statelevel.groupby(
            ["filing_year", "state_code"], as_index=False
        ).sum()
    )
    # Replace column names with more descriptive names
    column_mapping = {
        "income_level1": f"{category}_income_<_25K",
        "income_level2": f"{category}_income_btwn_25K_50K",
        "income_level3": f"{category}_income_btwn_50K_75K",
        "income_level4": f"{category}_income_btwn_75K_100K",
        "income_level5": f"{category}_income_btwn_100K_200K",
        "income_level6": f"{category}_income_>_200K",
    }
    df_filtered_federal_filing_statelevel_agg.rename(
        columns=column_mapping, inplace=True
    )
    return df_filtered_federal_filing_statelevel_agg


def ff_each_inc_distr_pct_change(
    state_code: str, year1: int, year2: int, income_level: int, *categories: str
) -> pd.DataFrame:
    """Takes in at least four parameters: a state code, two years, an income level, and any number of categories. It will return a dataframe with the percent change for the value of that category from the older year to the newer year, for the state ('ALL' for all states) and income level (0 to aggregate all income levels)."""
    global df_tax_attribute
    global dict_state
    global df_fed_state_filings
    global dict_income_group
    income_level_mapping = dict_income_group
    state_codes = dict_state
    df_irs_totals = df_fed_state_filings
    extraction_attributes = df_tax_attribute["variable"].tolist()

    for category in categories:
        if category not in list(extraction_attributes):
            return "Please choose a valid category."
    if year1 not in [2015, 2016, 2017, 2018, 2019, 2020] or year2 not in [
        2015,
        2016,
        2017,
        2018,
        2019,
        2020,
    ]:
        return "Please enter a valid year, between 2015-2020."
    if state_code not in (list(state_codes.keys()) + ["ALL"]):
        return "Please enter a valid state code."
    for category in categories:
        if category in [
            "filing_year",
            "zipcode",
            "income_level",
            "statefips",
            "state_code",
        ]:
            return "Please choose a category that is not 'filing_year', 'state_code', 'zipcode', or 'income_level', or 'statefips'."
    for category in categories:
        if list(categories).count(category) > 1:
            return "Please enter distinct categories."
    if income_level not in [0, 1, 2, 3, 4, 5, 6]:
        return "Please enter a valid income level: either 1-6 for a specific income level, or 0 for all income levels (aggregated)."

    df_filtered_1 = df_irs_totals[df_irs_totals["filing_year"] == year1].copy()
    df_filtered_1 = df_filtered_1.reset_index(drop=True)
    df_filtered_2 = df_irs_totals[df_irs_totals["filing_year"] == year2].copy()
    df_filtered_2 = df_filtered_2.reset_index(drop=True)

    for category in categories:
        if df_filtered_1[category].isna().all() or df_filtered_2[category].isna().all():
            return "Please choose a valid category that is in both of the years you entered."

    for category in categories:
        if category not in df_filtered_1.columns:
            return "Please choose categories that are in both of the years you entered."
        if category not in df_filtered_2.columns:
            return "Please choose categories that are in both of the years you entered."

    for category in categories:
        if (
            category in df_filtered_1.columns and category not in df_filtered_2.columns
        ) or (
            category in df_filtered_2.columns and category not in df_filtered_1.columns
        ):
            return "Please choose categories that are in both of the years you entered."

    if len(categories) == 0:
        categories = [
            "state_code",
            "n_ret",
            "nbr_individuals",
            "n_ret_single",
            "n_ret_head_household",
            "n_ret_joint",
            "n_ret_salaries_wages",
            "amt_salaries_wages",
            "n_ret_net_investment_income_tax",
            "amt_net_investment_income_tax",
            "n_ret_net_capital_gain",
            "amt_net_capital_gain",
            "n_ret_ordinary_dividends",
            "amt_ordinary_dividends",
            "n_ret_qualified_dividends",
            "amt_qualified_dividends",
            "n_ret_itemized_deductions",
            "amt_agi_itemized_returns",
            "n_ret_alternative_minimum_tax",
            "amt_alternative_minimum_tax",
            "n_ret_home_mortgage_interest_paid",
            "amt_home_mortgage_interest_paid",
            "n_ret_real_estate_taxes",
            "amt_real_estate_taxes",
            "n_ret_state_local_income_taxes",
            "amt_state_local_income_taxes",
            "amt_total_income",
            "n_ret_total_income",
            "n_ret_taxable_income",
            "amt_taxable_income",
            "n_ret_total_tax_payments",
            "amt_total_tax_payments",
            "n_ret_taxable_interest",
            "amt_taxable_interest",
            "n_ret_tax_liability",
            "amt_tax_liability",
        ]
        df_last = pd.DataFrame(columns=categories)
    else:
        categories = ("state_code",) + categories
        df_final_1 = df_filtered_1[list(categories)].copy()
        df_final_2 = df_filtered_2[list(categories)].copy()
        df_last = pd.DataFrame(columns=categories)

    states = df_irs_totals["state_code"].unique()
    for index, state in enumerate(states):
        df_last.loc[index, "state_code"] = state
        if income_level == 0:
            df_filtered_1_more = df_filtered_1[df_filtered_1["state_code"] == state]
            df_filtered_2_more = df_filtered_2[df_filtered_2["state_code"] == state]
        if income_level != 0:
            df_filtered_1_more = df_filtered_1[
                (df_filtered_1["state_code"] == state)
                & (df_filtered_1["income_level"] == income_level)
            ]
            df_filtered_2_more = df_filtered_2[
                (df_filtered_2["state_code"] == state)
                & (df_filtered_1["income_level"] == income_level)
            ]

        for category in categories[1:]:
            sum_1 = df_filtered_1_more[category].sum()
            sum_2 = df_filtered_2_more[category].sum()
            if year1 < year2:
                if sum_1 != 0:
                    percent_change = (((sum_2 - sum_1) / sum_1) * 100).round(2)
                    df_last.loc[index, category] = f"{percent_change}%"
                elif sum_1 == 0:
                    df_last.loc[index, category] = "Inf."
            elif year1 > year2:
                if sum_2 != 0:
                    percent_change = (((sum_1 - sum_2) / sum_2) * 100).round(2)
                    df_last.loc[index, category] = f"{percent_change}%"
                elif sum_2 == 0:
                    df_last.loc[index, category] = "Inf."
            else:
                return "Years entered are the same. Please enter different years."

    if year1 < year2:
        df_last["start_yr"] = year1
        df_last["end_yr"] = year2
    else:
        df_last["start_yr"] = year2
        df_last["end_yr"] = year1

    if income_level == 0:
        df_last["income_level"] = "All"
    if income_level != 0:
        df_last["income_level"] = income_level

    df_last["income_level_desc"] = df_last["income_level"].replace(
        income_level_mapping)
    selected_columns = [
        "state_code",
        "start_yr",
        "end_yr",
        "income_level",
        "income_level_desc",
    ]
    remaining_columns = [
        col for col in df_last.columns if col not in selected_columns]
    df_last = df_last[selected_columns + remaining_columns]
    df_last.rename(
        columns={"start_yr": "filing_start_yr", "end_yr": "filing_end_yr"}, inplace=True
    )

    if state_code == "ALL":
        return df_last
    else:
        return df_last[df_last["state_code"] == state_code]


def ff_all_inc_distr_pct_change(
    state_code: str, year1: int, year2: int, *categories: str
) -> pd.DataFrame:
    """Takes in at least three parameters: a state code, two years, and any number of categories. It will return a dataframe with the percent change for the value of that category from the older year to the newer year for the state ('ALL' for all states), for each and every income level."""
    global df_tax_attribute
    global dict_state
    global df_fed_state_filings
    global dict_income_group
    income_level_mapping = dict_income_group
    state_codes = dict_state
    df = df_fed_state_filings
    extraction_attributes = df_tax_attribute["variable"].tolist()
    df_federal_filing_statelevel = pd.DataFrame()
    for category in categories:
        if category not in list(extraction_attributes):
            return "Please choose a valid category."
    for category in categories:
        if category in [
            "filing_year",
            "zipcode",
            "income_level",
            "statefips",
            "state_code",
        ]:
            return "Please choose a category that is not 'filing_year', 'state_code', 'zipcode', or 'income_level', or 'statefips'."
    if (year1 not in [2015, 2016, 2017, 2018, 2019, 2020]) or (
        year2 not in [2015, 2016, 2017, 2018, 2019, 2020]
    ):
        return "Please enter a valid year (2015-2020)."
    if year1 == year2:
        return "Years entered are the same. Please enter different years."
    if (state_code not in list(state_codes.keys())) and state_code != "ALL":
        return "Please enter a valid state code or 'ALL' to view all states."
    df_all = pd.DataFrame()
    for income_level in range(1, 7):
        df = ff_each_inc_distr_pct_change(
            state_code, year1, year2, income_level, *categories
        )
        df_all = pd.concat([df_all, df], ignore_index=True)
    return df_all
