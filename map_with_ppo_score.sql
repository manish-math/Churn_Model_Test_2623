with max_churn as (
select fca_id,max(churn_90_probability) churn_90_probability
from ${churn_scores_single_table}
group by 1
),
scores_from_churn as(
select distinct a.fca_id,churn_30_probability,churn_30_decile,b.churn_90_probability,churn_90_decile,population_flag,flag,scoring_date
from ${churn_scores_single_table} a inner join max_churn b
on a.fca_id=b.fca_id and a.churn_90_probability=b.churn_90_probability
),
scores_from_ppo as
(
select
distinct a.fca_id ,a.hh_id,a.population_flag ,a.flag,in_market_prob,in_market_decile, current_brand ,current_nameplate ,current_trim, first_preferred_nameplate, first_preferred_brand, first_preferred_score ,second_preferred_nameplate ,second_preferred_brand, second_preferred_score, third_preferred_nameplate, third_preferred_brand ,third_preferred_score, b.scoring_date,churn_30_probability,churn_30_decile,churn_90_probability,churn_90_decile
from ${ppo_enr_nonenr_current_date_score} a
inner join scores_from_churn b
on a.fca_id=b.fca_id
and a.population_flag=b.population_flag
and a.flag=b.flag
)

select fca_id ,hh_id,
population_flag ,flag,churn_30_probability,churn_30_decile,churn_90_probability,churn_90_decile,in_market_prob,in_market_decile, current_brand ,current_nameplate ,current_trim, first_preferred_nameplate, first_preferred_brand, first_preferred_score ,second_preferred_nameplate ,second_preferred_brand, second_preferred_score, third_preferred_nameplate, third_preferred_brand ,third_preferred_score, scoring_date from (
SELECT *,
ROW_NUMBER()over(PARTITION by fca_id ORDER by in_market_prob desc) as p_rank
from scores_from_ppo
)
where p_rank=1