with max_churn as (
select fca_id,max(churn_30_prob) churn_30_prob
from churn_buso_retail_score_current_date
group by 1
),

scores_from_churn as(
select distinct a.fca_id,c.i_consmr,churn_30_decile,b.churn_30_prob,population_flag,retail_lease_flag as flag,processed_date as scoring_date
from churn_buso_retail_score_current_date a 
inner join max_churn b
on a.fca_id=b.fca_id 
and a.churn_30_prob=b.churn_30_prob
-- mapping with ownership to fetch COIN_ID
inner join  CDP_UNIFICATION_QA_V2.enriched_cms_consmvhclownshp c
on a.fca_id=c.fca_id
),
scores_from_ppo as
(
select
distinct a.fca_id ,a.hh_id,a.population_flag ,a.flag,in_market_prob,in_market_decile, current_brand ,current_nameplate ,current_trim, first_preferred_nameplate, first_preferred_brand, first_preferred_score ,second_preferred_nameplate ,second_preferred_brand, second_preferred_score, third_preferred_nameplate, third_preferred_brand ,third_preferred_score, b.scoring_date,churn_30_prob,churn_30_decile
from mathco_ppo_enr_nonenr_scores_current_date a
inner join scores_from_churn b
-- mapping based on coin_id 
on a.i_consmr=b.i_consmr
-- on a.fca_id=b.fca_id
and upper(trim(a.population_flag))=upper(trim(b.population_flag))
-- and a.flag=b.flag
)


select fca_id ,hh_id,
population_flag ,flag,churn_30_prob,churn_30_decile,in_market_prob,in_market_decile, current_brand ,current_nameplate ,current_trim, first_preferred_nameplate, first_preferred_brand, first_preferred_score ,second_preferred_nameplate ,second_preferred_brand, second_preferred_score, third_preferred_nameplate, third_preferred_brand ,third_preferred_score,scoring_date from (
SELECT *,
ROW_NUMBER()over(PARTITION by fca_id ORDER by in_market_prob desc) as p_rank
from scores_from_ppo
)
where p_rank=1