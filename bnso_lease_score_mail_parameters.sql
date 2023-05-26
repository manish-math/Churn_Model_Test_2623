with base as (
SELECT count(DISTINCT fca_id) fcaid_counts,
scoring_date as scoring_date_disp,'BNSO Lease' as population
from 
churn_bnso_lease_scoring_ads
group by 2,3 
),

base2 as (
select case when trim(env_name)='qa' then 'UAT' 
when trim(env_name)='prd' then 'Production' end as env
from fca_bcm_stage.tbl_env
)

select fcaid_counts,scoring_date_disp,population,env 
from base,base2 