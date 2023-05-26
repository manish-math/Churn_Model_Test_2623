select distinct fca_id from churn_bnso_retail_train_base
union 
select distinct fca_id from churn_bnso_lease_train_base
union 
select distinct fca_id from churn_buso_train_base