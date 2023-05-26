with singlescoretable as(
with scores as(
select  distinct fca_id, (churn_30_prob) churn_30_probability,(cast(churn_30_decile as VARCHAR))as churn_30_decile ,(churn_90_prob)churn_90_probability,(cast(churn_90_decile as VARCHAR))as churn_90_decile, (processed_date)scoring_date  ,'bnso' as population_flag,'is_retail' as flag
from ${bnso_retail_score}
UNION
select distinct fca_id,(churn_30_prob)churn_30_probability,(cast(churn_30_decile as VARCHAR))as churn_30_decile  ,(churn_90_prob)churn_90_probability  ,(cast(churn_90_decile as VARCHAR))as churn_90_decile , scoring_date,'bnso' as population_flag,'is_lease' as flag
from ${bnso_lease_score}
)
select  distinct fca_id ,churn_30_probability,churn_30_decile,churn_90_probability,churn_90_decile,population_flag,flag,scoring_date
from scores
)

select distinct fca_id,churn_30_probability,churn_30_decile,churn_90_probability,churn_90_decile,population_flag,flag,scoring_date
from singlescoretable
-- )
