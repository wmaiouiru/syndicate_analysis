import sys
from loguru import logger
import pandas as pd
import fire
from typing import Optional
import json
LAST_INVESTMENT_WITH_ANGELLIST = 'Last Investment With AngelList'
LAST_INVESTMENT_WITH_YOUR_SYNDICATE = 'Last Investment with Your Syndicate'
DATE_JOINED_YOUR_SYNDICATE = 'Date Joined Your Syndicate'

LAST_WITH_ANGELLIST_YEAR = 'Last With AngelList Year'
LAST_WITH_YOUR_SYNDICATE_YEAR = 'Last with Your Syndicate Year'
JOINED_YOUR_SYNDICATE_YEAR = 'Year Joined Your Syndicate'
from enum import Enum

# class syntax
class InvestmentStatus(Enum):
    INVESTED_IN_SYNDICATE = "Invested in Syndicate"
    INVESTED_IN_ANGELLIST = "Invested in AngelList"
    NO_INVESTMENT = "No Investment"
    TOTAL = 'total'

def generate_sankey_diagram(df):
    # Group the DataFrame by 'investment_cohort' and count the occurrences
    cohort_counts = df['investment_cohort'].value_counts().reset_index()
    cohort_counts.columns = ['Source', 'Target']
    cohort_dict = dict(zip(cohort_counts['Source'], cohort_counts['Target']))
    cohort_dict[InvestmentStatus.TOTAL.value] = \
        cohort_dict[InvestmentStatus.INVESTED_IN_SYNDICATE.value] + \
        cohort_dict[InvestmentStatus.INVESTED_IN_ANGELLIST.value] + \
            cohort_dict[InvestmentStatus.NO_INVESTMENT.value]
    logger.info(json.dumps(cohort_dict, indent=2))
    # TODO convert to https://sankeymatic.com/ text with percentage

def investment_status(row):
    if not pd.isna(row[LAST_INVESTMENT_WITH_YOUR_SYNDICATE]):
        return InvestmentStatus.INVESTED_IN_SYNDICATE.value
    if not pd.isna(row[LAST_INVESTMENT_WITH_ANGELLIST]):
        return InvestmentStatus.INVESTED_IN_ANGELLIST.value
    return InvestmentStatus.NO_INVESTMENT.value
def analyze_by_group_by(df, key, custom_order):
    # Calculate the count of each group
    groupby_df = df.groupby(key).size().reset_index(name='Count')
    
    # Calculate the total count of all groups
    total_count = groupby_df['Count'].sum()
    
    # Calculate the percentage column and add it to the DataFrame
    groupby_df['Percentage'] = (groupby_df['Count'] / total_count) * 100
    groupby_df['Percentage'] = groupby_df['Percentage'].round(2)
    # Sort the DataFrame by the custom order of the 'key' column
    df_sorted = groupby_df[groupby_df[key].isin(custom_order)].sort_values(
        by=[key],
        key=lambda x: x.map({k: i for i, k in enumerate(custom_order)})
    )

    return df_sorted
def convert_to_year_col(df, input_col, output_col) -> pd.DataFrame:
    # Assuming MM/DD/YY format (September 11, 2023)
    input_format = "%m/%d/%y %I:%M %p"

    # Convert the "date" column to a datetime format
    df[output_col] = pd.to_datetime(df[input_col], format=input_format)

    # Format the "date" column in YYYY-MM format
    df[output_col] = df[output_col].dt.strftime('%Y')
    return df

class Main:
    def __init__(self) -> None:
        logger.remove()
        logger.add(sys.stdout, format="{time} {level} {file}:{line} {message}", level="INFO")

    def analyze_syndicate(self, input_csv: str, out_dir: Optional[str]=None) -> None:
        df = pd.read_csv(input_csv)
        logger.debug(df)
        df['investment_cohort'] = df.apply(investment_status, axis=1)
        
        # generate_sankey_diagram(df)

        dollar_custom_order = [
            '≥ $500k',
            '≥ $250k',
            '≥ $100k',
            '≥ $50k',
            '≥ $10k',
            '≥ $1k',
            '$0',
        ]
        invested_last_12_m_df = analyze_by_group_by(df, 'Total Amount Invested With AngelList (Last 12m)', dollar_custom_order)        

        # Calculate the count of non-NA/null entries in the column "Total Amount Invested With AngelList (Last 12m)"

        invested_last_12_m_df = invested_last_12_m_df.rename(columns={'Total Amount Invested With AngelList (Last 12m)': 'Last 12m'})
        invested_last_12_m_df = invested_last_12_m_df.rename(columns={'Count': 'LPs'})
        print('Total Amount Invested With AngelList')
        print(invested_last_12_m_df.to_markdown(index=False))  



        df = convert_to_year_col(df, LAST_INVESTMENT_WITH_ANGELLIST, LAST_WITH_ANGELLIST_YEAR)
        df = convert_to_year_col(df, LAST_INVESTMENT_WITH_YOUR_SYNDICATE, LAST_WITH_YOUR_SYNDICATE_YEAR)

        # Extract year and month from the date columns
        df[LAST_WITH_ANGELLIST_YEAR] = pd.to_datetime(df[LAST_WITH_ANGELLIST_YEAR]).dt.to_period('Y')
        df[LAST_WITH_YOUR_SYNDICATE_YEAR] = pd.to_datetime(df[LAST_WITH_YOUR_SYNDICATE_YEAR]).dt.to_period('Y')
        df[JOINED_YOUR_SYNDICATE_YEAR] = pd.to_datetime(df[DATE_JOINED_YOUR_SYNDICATE]).dt.to_period('Y')
        df[LAST_WITH_YOUR_SYNDICATE_YEAR].fillna(pd.Period('1900', freq='Y'), inplace=True) 
        df[LAST_WITH_ANGELLIST_YEAR].fillna(pd.Period('1900', freq='Y'), inplace=True) 
        df[LAST_INVESTMENT_WITH_ANGELLIST].fillna(-1, inplace=True)
        df[LAST_INVESTMENT_WITH_YOUR_SYNDICATE].fillna(-1, inplace=True)
        # Create a pivot table for cohort analysis
        cohort_pivot = df.pivot_table(index=JOINED_YOUR_SYNDICATE_YEAR, 
                                      columns=LAST_WITH_ANGELLIST_YEAR, 
                                      values=LAST_INVESTMENT_WITH_ANGELLIST, 
                                      aggfunc='count',
                                        margins=True,  # Add row and column totals
                                        margins_name='Total'  # Customize the name of the total row and column
                                      )
        # Fill NaN values with zeros
        cohort_pivot = cohort_pivot.fillna(0)
        # cohort_pivot.index = cohort_pivot.index.where(cohort_pivot.index != pd.Period('1900', freq='Y'), -1)
        cohort_pivot.columns = cohort_pivot.columns.where(cohort_pivot.columns != pd.Period('1900', freq='Y'), 'No Investment')

        # Print the cohort analysis pivot table
        print('Cohort Joined Your Syndicate Year to Last Invested with AngelList Year')
        print(cohort_pivot.to_markdown())

        # Convert the pivot table to percentages
        # percentage_cohort_pivot = ((cohort_pivot / total_sum) * 100).round(2)
        cohort_pivot_total_removed = cohort_pivot.loc[cohort_pivot.index != 'Total', cohort_pivot.columns != 'Total']
        percentage_cohort_pivot = cohort_pivot_total_removed.div(cohort_pivot_total_removed.sum(axis=1), axis=0) * 100
        # Remove row and column totals after the fact
        percentage_cohort_pivot = percentage_cohort_pivot.round(1)
        # Print the pivot table with percentages
        print('Cohort Joined Your Syndicate Year to Last Invested with AngelList Year Percent')
        print(percentage_cohort_pivot.to_markdown())


if __name__ == "__main__":
    fire.Fire(Main)    