select distinct fca_id from churn_bnso_retail_score_base
union 
select distinct fca_id from churn_bnso_lease_score_base
union 
select distinct fca_id from churn_buso_retail_score_base