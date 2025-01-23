from bs4 import BeautifulSoup
from datetime import datetime
import requests
import sqlite3
import pandas as pd

url = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"

db = "World_Economies.db"
db_table_name = "Countries_by_GDP"
attribute_list = ["Country", "GDP_USD_billion"]
target_file = "Countries_by_GDP.json"
file_path = "./"
log_file = "./log_file.txt"


# Find the GDP table
def find_table(soup, key):
    tables = soup.find_all("table")
    for table in tables:
        if key in str(table):
            return table
    return None


def extract_from_url(url):
    try:
        data = requests.get(url)
        html_content = data.text
        soup = BeautifulSoup(html_content, "html.parser")
        gdp_table_key = "GDP (USD million) by country"
        return find_table(soup, gdp_table_key)
    except Exception as e:
        raise e


def extract_data(gdp_table):
    try:
        df = pd.DataFrame(columns=["Country", "GDP_USD_million"])

        table_bs = BeautifulSoup(str(gdp_table), "html5lib")
        table_rows = table_bs.find_all("tr")

        for i, row in enumerate(table_rows):
            row_data = row.find_all("td")
            country_name = ""
            country_gdp = 0
            bs = BeautifulSoup(str(row_data), "html5lib")
            country_links = bs.find_all("a")

            if len(country_links) > 0:
                country_name = country_links[0].text
            if len(row_data) >= 2:
                formatted_string = row_data[2].text.replace(",", "")
                if formatted_string.isnumeric():
                    country_gdp = float(formatted_string)

            # Adding data to new dataframe if valid
            if country_name:
                df.loc[len(df)] = [country_name, country_gdp]

        return df

    except Exception as e:
        raise e


def extract():
    """
    Extract the list of all countries in order of their GDPs in million USDs.
    """

    gdp_table = extract_from_url(url)
    extracted_data = extract_data(gdp_table)
    return extracted_data


def transform(data):
    """
    Converts the GPD of countries in billion USDs rounded to 2 decimal places. 1 billion is equal to 1000 millions.
    """
    data["GDP_USD_million"] = round(data.GDP_USD_million / 1000, 2)
    data.rename(columns={"GDP_USD_million": "GDP_USD_billion"}, inplace=True)
    return data


def load_in_json(target_file, transformed_data):
    transformed_data.to_json(target_file)


def load_in_db(target_db, table_name, attribute_list, file_path, data):
    # Setting up database connection
    conn = sqlite3.connect(target_db)

    # Create and load the table
    data.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()


def load(target_file, target_db, db_table_name, attribute_list, file_path, data):
    """
    Load the extracted data into a JSON file as well as in the database.
    """
    load_in_json(target_file, data)
    load_in_db(target_db, db_table_name, attribute_list, file_path, data)


def log_progress(message):
    timestamp_format = "%Y-%m-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(f"{timestamp}, {message}\n")


if __name__ == '__main__':
    # ETL process
    log_progress("ETL Job started")

    log_progress("Extract phase Started")
    extracted_data = extract()
    log_progress("Extract phase Ended")

    log_progress("Transform phase Started")
    transformed_data = transform(extracted_data)
    log_progress("Transform phase Ended")

    log_progress("Load phase Started")
    load(target_file, db, db_table_name, attribute_list, file_path, transformed_data)
    log_progress("Load phase Ended\n-------------------------------------------")
    print("ETL Job completed")

    # Query database to display only the entries with more than 100 billion USD
    conn = sqlite3.connect(db)
    query_statement = f"SELECT * FROM Countries_by_GDP WHERE GDP_USD_billion > 100"
    query_output = pd.read_sql(query_statement, conn)
    print(query_statement)
    print(query_output)
