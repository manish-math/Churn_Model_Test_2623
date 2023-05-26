with churn_base as (
SELECT fca_id,vin
from ${bnso_retail_train_base}
),

recall_data as (
SELECT trim(i_vin,' ') as i_vin, trim(d_recal_lnch, ' ') as d_recal_lnch
,a.c_vhcl_recal,l_vin, c_vhcl_recal_typ
FROM ${stg_recal_sseo_vin} as a
INNER JOIN ${stg_recal_code} as b
on a.c_vhcl_recal=b.c_vhcl_recal
where trim(i_vin) in (select vin from churn_base)
),

recall_cust as (
SELECT i_vin,d_recal_lnch, c_vhcl_recal_typ
,CASE WHEN c_vhcl_recal_typ='W' THEN 1 else 0
END as recall_type_customer_satisfaction
from recall_data
),

get_consmr as (
SELECT distinct fca_id,b.vin, recall_type_customer_satisfaction
from recall_cust as a
INNER JOIN churn_base as b
on a.i_vin =b.vin
)

select distinct fca_id,max(recall_type_customer_satisfaction) recall_type_customer_satisfaction
from get_consmr
group by 1
