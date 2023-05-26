with ppo as (
select distinct cast(fca_id as VARCHAR) fca_id,Round(in_market_prob,3) in_market_prob,Round(defection_prob,3) defection_prob,SUBSTR(scoring_date,1,4)||'-'||SUBSTR(scoring_date,5,2)||'-'||SUBSTR(scoring_date,7,2) as scoring_date from tmc_ppo_result_20201220_final
where fca_id in (select distinct fca_id from ${bnso_lease_train_base})
and lower(trim(flag))='is_lease'
union
select distinct cast(fca_id as VARCHAR) fca_id,Round(in_market_prob,3) in_market_prob,Round(defection_prob,3) defection_prob,SUBSTR(scoring_date,1,4)||'-'||SUBSTR(scoring_date,5,2)||'-'||SUBSTR(scoring_date,7,2) as scoring_date from tmc_ppo_result_20210118_final
where fca_id in (select distinct fca_id from ${bnso_lease_train_base})
and lower(trim(flag))='is_lease'
union
select distinct cast(fca_id as VARCHAR) fca_id,Round(in_market_prob,3) in_market_prob,Round(defection_prob,3) defection_prob,SUBSTR(scoring_date,1,4)||'-'||SUBSTR(scoring_date,5,2)||'-'||SUBSTR(scoring_date,7,2) as scoring_date from tmc_ppo_result_20210224_final
where fca_id in (select distinct fca_id from ${bnso_lease_train_base})
and lower(trim(flag))='is_lease'
union
select distinct cast(fca_id as VARCHAR) fca_id,Round(in_market_prob,3) in_market_prob,Round(defection_prob,3) defection_prob,'2021-03-29' as scoring_date from tmc_ppo_result_20210329_adhoc
where fca_id in (select distinct fca_id from ${bnso_lease_train_base})
and lower(trim(flag))='is_lease'
union
select distinct cast(fca_id as VARCHAR) fca_id,Round(in_market_prob,3) in_market_prob,Round(defection_prob,3) defection_prob,SUBSTR(scoring_date,1,4)||'-'||SUBSTR(scoring_date,5,2)||'-'||SUBSTR(scoring_date,7,2) as scoring_date from mathco_owner_bnso_buso_complete_scores_20210614_v3
where UPPER(TRIM(population_flag))='BNSO'
and lower(trim(flag))='is_lease'
and fca_id in (select distinct fca_id from ${bnso_lease_train_base})
union 
select distinct cast(fca_id as VARCHAR) fca_id,Round(in_market_prob,3) in_market_prob,Round(defection_prob,3) defection_prob,scoring_date from mathco_ppo_scores_july_2021
where UPPER(TRIM(population_flag))='BNSO'
and fca_id in (select distinct fca_id from ${bnso_lease_train_base})
and lower(trim(flag))='is_lease'
union 
select distinct cast(fca_id as VARCHAR) fca_id,Round(in_market_prob,3) in_market_prob,Round(defection_prob,3) defection_prob,scoring_date
from 
mathco_ppo_scores_historical
where UPPER(TRIM(population_flag))='BNSO'
and fca_id in (select distinct fca_id from ${bnso_lease_train_base})
and lower(trim(flag))='is_lease'
),

scores as (
select * from (
select *,
ROW_NUMBER()over(PARTITION by fca_id ORDER by scoring_date desc) as p_rank
from ppo
where date(scoring_date) between (date('${train_ref_date}') - interval '90' day) and date('${train_ref_date}')
)
where p_rank=1 
)

select distinct fca_id,in_market_prob from  scores 