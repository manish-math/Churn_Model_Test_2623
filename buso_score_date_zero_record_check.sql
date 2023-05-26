with date_check_retail as (
  SELECT DISTINCT qc_score_date_retail from (
SELECT case when date(processed_date)<>CURRENT_DATE then true else false end as qc_score_date_retail
from ${bnso_retail_score}
)
),

base as (  
select max(test) zero_record_qc from (
  SELECT  if(cnt=0,true,false) as test from ( 
    select count(*) cnt  
    from ${bnso_retail_score}
  )
)
)
,
base2 as (
SELECT DISTINCT qc_score_date from (
SELECT case when date(scoring_date)<>CURRENT_DATE then true else false end as qc_score_date
-- from ${dummy_delete}
from ${output_table_with_ppo_churn_scores}
)
)
SELECT zero_record_qc,qc_score_date,qc_score_date_retail,qc_score_date_lease,
case when (zero_record_qc or qc_score_date)=false then true else false end as qc_check,
case when (qc_score_date_retail)=false then true else false end as qc_check_date
 from base,base2,date_check_retail 