-- select count(distinct fca_id) as fca_id_cnt_BNSO_lease
-- from ${bnso_lease_score}

select scoring_date,

count(distinct case when flag = 'is_retail' then fca_id end) as fca_id_cnt_BNSO_retail,

count(distinct case when flag = 'is_lease' then fca_id end) as fca_id_cnt_BNSO_lease

from ${output_table_with_ppo_churn_scores}

group by 1