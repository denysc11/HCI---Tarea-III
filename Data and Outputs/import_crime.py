#!/usr/bin/env python

import os
os.chdir("C:/Users/Hp Support/Videos/03 - Cursos/05 - Herramientas computacionales/Clase 3 - Scraping Python/Tarea")

# make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import pandas as pd
from sodapy import Socrata

# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
# client = Socrata("odn.data.socrata.com", None)

# Example authenticated client (needed for non-public datasets):
client = Socrata("odn.data.socrata.com",
                  "NqMZDY9gtRg6fczW6AW93gXQn",
                  username="casianoinga@gmail.com",
                  password="trabajo3_udesa")

# First 2000 results, returned as JSON from API / converted to Python list of
# dictionaries by sodapy.
results = client.get("tt5s-y5fc", limit=100000)

# Convert to pandas DataFrame
results_df = pd.DataFrame.from_records(results)

# Exportar los datos  a formato CSV:

results_df.to_csv('crime.csv', header=True, index=False)
  


