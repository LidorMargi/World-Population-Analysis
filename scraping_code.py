import bs4
from bs4 import BeautifulSoup  
import pandas as pd
import scipy as sc
import numpy as np
import requests
import csv
import os
import re

def get_city_area(city):
    try:
        url = f'https://en.wikipedia.org/wiki/{city.replace(" ", "_")}'
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        infobox = soup.find('table', class_='infobox')
        rows = infobox.find_all('tr')
        
        density_section = False
        for row in rows:
            header = row.find('th')
            if header:
                header_text = header.text.strip().lower()
                if 'density' in header_text:
                    density_section = True
                else:
                    density_section = False
            
            if not density_section:
                data = row.find('td')
                if data and 'km' in data.text:
                    area_text = data.text.strip()
                    area_match = re.search(r'(\d+(?:,\d+)?(?:\.\d+)?)', area_text)
                    if area_match:
                        area = area_match.group(1).replace(',', '')
                        return float(area)
    except Exception:
        pass
    
    return None

def compile_city_population_data(urls):
    city_data = []
    
    for url in urls:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        tables = soup.find_all('table', class_='wikitable')
        
        for table in tables:
            heading = table.find_previous(['h2', 'h3'])
            country_name = heading.text.strip() if heading else 'Unknown'
            country_name = country_name.replace('[edit]', '')
            rows = table.find_all('tr')[1:]  # Exclude the header row
            
            for row in rows:
                columns = row.find_all('td')
                
                if len(columns) >= 3:
                    city = columns[0].text.strip()
                    population_raw = columns[2].text.strip()
                    population_match = re.search(r'(\d{1,3}(?:,\d{3})*\b)', population_raw)
                    if population_match:
                        population = population_match.group(1).replace(',', '')
                        area = get_city_area(city)
                        city_data.append([city, country_name, int(population), area])
    
    df = pd.DataFrame(city_data, columns=['City', 'Country', 'Population', 'Area'])
    df['Country'] = df['Country'].replace({
    "China, Republic of (Taiwan)": "Taiwan",
    "China, People's Republic of": "China"})
    return df

urls = [
    'https://en.wikipedia.org/wiki/List_of_towns_and_cities_with_100,000_or_more_inhabitants/country:_A-B',
    'https://en.wikipedia.org/wiki/List_of_towns_and_cities_with_100,000_or_more_inhabitants/country:_C-D-E-F',
    'https://en.wikipedia.org/wiki/List_of_towns_and_cities_with_100,000_or_more_inhabitants/country:_G-H-I-J-K',
    'https://en.wikipedia.org/wiki/List_of_towns_and_cities_with_100,000_or_more_inhabitants/country:_L-M-N-O',
    'https://en.wikipedia.org/wiki/List_of_towns_and_cities_with_100,000_or_more_inhabitants/country:_P-Q-R-S',
    'https://en.wikipedia.org/wiki/List_of_towns_and_cities_with_100,000_or_more_inhabitants/country:_T-U-V-W-Y-Z'
]


df = compile_city_population_data(urls)
df = pd.read_csv('city_population.csv')
df.to_csv('city_population.csv', index=False)
print(df)


def scrape_wikipedia_table(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table', class_='wikitable')
    df = pd.read_html(str(table))[0]
    return df

# URL 1: List of countries by population growth rate
url1 = 'https://en.wikipedia.org/wiki/List_of_countries_by_population_growth_rate'
df1 = scrape_wikipedia_table(url1)
df1.to_csv('countries_by_population_growth.csv', index=False)


# URL 2: List of countries and dependencies by population density
url2 = 'https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population_density'
df2 = scrape_wikipedia_table(url2)
df2.to_csv('countries_by_population_density.csv', index=False)
df3 = pd.read_csv('countries_by_population_density.csv', skiprows=1)
df3.to_csv('countries_by_population_density.csv', index=False)


#modify countries growth rate csv file
# Read the original CSV file
df = pd.read_csv('countries_by_population_growth.csv')

# Create a new DataFrame to store the modified data
new_df1 = df.iloc[:, :2].copy()

# Rename the columns
new_df1.columns = ['country', 'growth rate']
new_df1['2009 growth rate'] = df['WB[4] 2009']
new_df1['2005-2010 growth rate'] = df['UN[5] 2005–10']
new_df1['2010-2015 growth rate'] = df['UN[5] 2010–15']
new_df1['2015-2020 growth rate'] = df['UN[5] 2015–20']

# Remove any suffix from the country name in the 'country' column
new_df1['country'] = new_df1['country'].str.split(' ').str[0].str.strip('*')

# Save the new DataFrame as a CSV file
new_df1.to_csv('modified_countries_by_population_growth.csv', index=False)



#modify country density csv file
# Read the original CSV file
df = pd.read_csv('countries_by_population_density.csv')

# Create a new DataFrame to store the modified data
new_df = pd.DataFrame()

# Extract the country names and population
new_df['country'] = df['Country or dependency']
new_df['country population'] = df['Population']
new_df['country area'] = df['km2']
new_df['country density'] = df['/km2']

# Remove any suffix from the country names in the 'country' column
new_df['country'] = new_df['country'].str.replace(r'\([^()]*\)', '').str.replace(r'\[[^\[\]]*\]', '').str.strip()


# Save the new DataFrame as a CSV file
new_df.to_csv('modified_countries_by_population_density.csv', index=False)


# Read the city population data
city_df = pd.read_csv('city_population.csv')

# Read the modified country population density data
density_df = pd.read_csv('modified_countries_by_population_density.csv')

# Read the modified country population growth data
growth_df = pd.read_csv('modified_countries_by_population_growth.csv')

# Perform a join operation on the city DataFrame with the density DataFrame
city_df = city_df.merge(density_df, left_on='Country', right_on='country', how='left')

# Perform a join operation on the city DataFrame with the growth DataFrame
city_df = city_df.merge(growth_df, left_on='Country', right_on='country', how='left')

# Drop the redundant 'country' column from the joins
city_df.drop(['country_x', 'country_y'], axis=1, inplace=True)

# Save the final DataFrame as a CSV file
city_df.to_csv('final_dataframe.csv', index=False)
nan_counts = city_df.isnull().sum()
print(city_df)
print(nan_counts)
