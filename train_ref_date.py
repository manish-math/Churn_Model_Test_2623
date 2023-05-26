import digdag
import os
import sys

import numpy as np
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

from datetime import date
import pytd.pandas_td as td
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.filterwarnings("ignore")

#declare API Key and db/target tables  #Maanvi APIkey
# td_api_key = '10729/559e1a5566782d174ad1fe9ded708239355c82bd'
# os.environ['TD_API_KEY'] = td_api_key
# apikey = os.environ['TD_API_KEY']
database = os.environ['database']
engine = td.create_engine('presto:{}'.format(database))
con = td.connect()


def train_ref_date_set():
  # ref_date_script = ("select cast('2022-11-08' as VARCHAR) as ref_date")
  ref_date_script = ("select cast(CURRENT_DATE - INTERVAL '1'  year as VARCHAR) as ref_date")
  train_ref_date = str(td.read_td(ref_date_script,engine)['ref_date'][0])
  digdag.env.store({'train_ref_date' : train_ref_date})

# def train_table_ref_date_set():
#   table_ref_date_script = ( "select SUBSTR(cast(CURRENT_DATE - INTERVAL '1'  year as VARCHAR),1,4)||'_'||SUBSTR(cast(CURRENT_DATE - INTERVAL '1'  year as VARCHAR),6,2)||'_'||SUBSTR(cast(CURRENT_DATE - INTERVAL '1'  year as VARCHAR),9,2) as table_ref_date" )
#   train_table_ref_date = str(td.read_td(table_ref_date_script,engine)['table_ref_date'][0])
#   digdag.env.store({'train_table_ref_date' : train_table_ref_date})

def train_table_ref_date_set():
  table_ref_date_script = ( "select SUBSTR(cast('2022-08-31' as VARCHAR),1,4)||'_'||SUBSTR(cast('2022-08-31' as VARCHAR),6,2)||'_'||SUBSTR(cast('2022-08-31' as VARCHAR),9,2) as table_ref_date" )
  train_table_ref_date = str(td.read_td(table_ref_date_script,engine)['table_ref_date'][0])
  digdag.env.store({'train_table_ref_date' : train_table_ref_date})