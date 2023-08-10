import pandas as pd

from taxfilingfusion import ff_fetch
ff_fetch.init()
df_tax_attribute = ff_fetch.df_tax_attribute
df_fed_state_filings = ff_fetch.df_fed_state_filings
dict_state = ff_fetch.dict_state
dict_income_group = ff_fetch.dict_income_group


def transform_group(group):
    group.fillna(0, inplace=True)
    n1 = group['n_ret'].sum()
    mars1 = group['n_ret_single'].sum()
    mars2 = group['n_ret_joint'].sum()
    mars4 = group['n_ret_head_household'].sum()
    other = n1 - (mars1 + mars2 + mars4)
    mars1_pct = ((mars1 / n1) * 100)
    mars2_pct = ((mars2 / n1) * 100)
    mars4_pct = ((mars4 / n1) * 100)
    other_pct = (100 - (mars1_pct + mars2_pct + mars4_pct))

    data = [
        {'filing_year': group['filing_year'].iloc[0], 'state_code': group['state_code'].iloc[0],
            'income_level': group['income_level'].iloc[0], 'type': 'Single', 'number': mars1, 'state_level_pct': f'{mars1_pct:.2f}'},
        {'filing_year': group['filing_year'].iloc[0], 'state_code': group['state_code'].iloc[0],
            'income_level': group['income_level'].iloc[0], 'type': 'Joint', 'number': mars2, 'state_level_pct': f'{mars2_pct:.2f}'},
        {'filing_year': group['filing_year'].iloc[0], 'state_code': group['state_code'].iloc[0],
            'income_level': group['income_level'].iloc[0], 'type': 'Head of Household', 'number': mars4, 'state_level_pct': f'{mars4_pct:.2f}'},
        {'filing_year': group['filing_year'].iloc[0], 'state_code': group['state_code'].iloc[0],
            'income_level': group['income_level'].iloc[0], 'type': 'Other', 'number': other, 'state_level_pct': f'{other_pct:.2f}'}
    ]
    return pd.DataFrame(data)


def ff_state_incgrp_returntype_distribution(state_code: str, year: int, income_level: int) -> pd.DataFrame:
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
    if (state_code not in list(state_codes.keys())) and state_code != 'ALL':
        return "Please enter a valid state code or 'ALL' to view all states."
    if 'year' in df.columns:
        df.rename(columns={'year': 'filing_year'}, inplace=True)
    if 'state' in df.columns:
        df.rename(columns={'state': 'state_code'}, inplace=True)

    df_federal_filing_statelevel = df[['filing_year', 'income_level', 'state_code', 'n_ret', 'nbr_individuals', 'n_ret_single', 'n_ret_head_household', 'n_ret_joint']].copy()

    if (income_level != 0):
        df_filtered_income_federal_filing_statelevel = df_federal_filing_statelevel[(
            df_federal_filing_statelevel['income_level'] == income_level)]
    else:
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
        ['filing_year', 'state_code', 'income_level']).apply(transform_group)
    df_final.reset_index(drop=True, inplace=True)
    df_final['income_level_desc'] = df_final['income_level'].replace(
        income_level_mapping)
    selected_columns = ['filing_year', 'state_code',
                        'income_level_desc', 'income_level']
    remaining_columns = [
        col for col in df_final.columns if col not in selected_columns]
    df_final = df_final[selected_columns + remaining_columns]
    df_final = df_final.reset_index(drop=True)

    if income_level == 0:
        df_aggregated = df_filtered_incomestate_federal_filing_statelevel.groupby(
            ['filing_year', 'state_code']).apply(transform_group)
        df_aggregated.reset_index(drop=True, inplace=True)
        df_aggregated['income_level'] = 0
        df_aggregated['income_level_desc'] = 'All'
        df_final = df_aggregated.copy()
        df_final.reset_index(drop=True, inplace=True)

    if 'number' in df_final.columns:
        df_final.rename(columns={'number': 'n_ret'}, inplace=True)

    df_final['state_level_pct'] = df_final['state_level_pct'].apply(
        lambda x: f"{x}%")

    return (df_final)


def ff_each_inc_distr_pct_change(state_code: str, year1: int, year2: int,  income_level: int, *categories: str) -> pd.DataFrame:
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
    if income_level not in [0, 1, 2, 3, 4, 5, 6]:
        return "Please enter a valid income level: either 1-6 for a specific income level, or 0 for all income levels (aggregated)."

    df_filtered_1 = df_irs_totals[df_irs_totals['filing_year'] == year1].copy(
    )
    df_filtered_1 = df_filtered_1.reset_index(drop=True)
    df_filtered_2 = df_irs_totals[df_irs_totals['filing_year'] == year2].copy(
    )
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
                    percent_change = (
                        ((sum_2 - sum_1) / sum_1) * 100).round(2)
                    df_last.loc[index, category] = f'{percent_change}%'
                elif sum_1 == 0:
                    df_last.loc[index, category] = 'Inf.'
            elif year1 > year2:
                if sum_2 != 0:
                    percent_change = (
                        ((sum_1 - sum_2) / sum_2) * 100).round(2)
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

    if state_code == "ALL":
        return df_last
    else:
        return df_last[df_last['state_code'] == state_code]
