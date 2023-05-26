with base as(
select scoring_date,

count(distinct fca_id) as fca_id_cnt_BUSO_retail
from ${output_table_with_ppo_churn_scores}
group by 1
),

base2 as (
select case when trim(env_name)='qa' then 'UAT' 
when trim(env_name)='prd' then 'Production' end as env
from fca_bcm_stage.tbl_env
)

SELECT scoring_date,fca_id_cnt_BUSO_retail,env
from base,base2 