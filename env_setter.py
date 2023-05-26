import os 

import sys 

os.system(f"{sys.executable} -m pip install --upgrade pip") 

os.system(f"{sys.executable} -m pip install pytd==1.4.3")

import pytd 
import pytd.pandas_td as td
import digdag 


    
 


def global_setup(): 

    env_query = """ 

    SELECT env_name AS env from fca_bcm_stage.tbl_env 

    """ 

    conn = pytd.Client(database='fca_bcm_stage') 

    result_set = conn.query(env_query) 
    
    print(result_set,"rrrrrrrrrrrrrrrrreeeeeeeeeeeeeeeettttttttt")
    env = result_set["data"][0][0] 
    
    if(env == 'qa') :
        env_repsntn = 'UAT'
    else :
        env_repsntn = 'PROD'

    # Store val_to_globalize as environment variable 

    digdag.env.store({'unification_db': 'cdp_unification_'+env+'_v2',
                  'stg_db':'fca_stg_'+env, 

                'sds_db': 'sandbox_data_science',  

                'env': env})

    digdag.env.store({'env_repsntn' : env_repsntn})