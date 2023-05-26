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

#declare API Key and db/target tables #Maanvi APIkey
# td_api_key = '10729/559e1a5566782d174ad1fe9ded708239355c82bd'
# os.environ['TD_API_KEY'] = td_api_key
# apikey = os.environ['TD_API_KEY']
database = os.environ['database']
engine = td.create_engine('presto:{}'.format(database))
con = td.connect()


def score_ref_date_set():
  ref_date_script = ( "select cast(CURRENT_DATE as VARCHAR) as ref_date" )
  train_ref_date = str(td.read_td(ref_date_script,engine)['ref_date'][0])
  digdag.env.store({'train_ref_date' : train_ref_date})

# def score_table_ref_date_set():
#   table_ref_date_script = ( "select SUBSTR(cast(CURRENT_DATE as VARCHAR),1,4)||'_'||SUBSTR(cast(CURRENT_DATE as VARCHAR),6,2)||'_'||SUBSTR(cast(CURRENT_DATE as VARCHAR),9,2) as table_ref_date" )
#   score_table_ref_date = str(td.read_td(table_ref_date_script,engine)['table_ref_date'][0])
#   digdag.env.store({'score_table_ref_date' : score_table_ref_date})