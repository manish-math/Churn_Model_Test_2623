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
,CASE WHEN c_vhcl_recal_typ='S' THEN 1 else 0 END as recall_type_safety
from recall_data
),

get_consmr as (
SELECT distinct fca_id,a.i_vin,d_recal_lnch, recall_type_safety
from recall_cust as a 
INNER JOIN churn_base as b
on a.i_vin =b.vin
),

recall_consmr_frm_base as ( 
SELECT distinct a.fca_id,a.vin
,COALESCE(recall_type_safety,0) as recall_type_safety,type_vehicle, d_recal_lnch
from ${bnso_retail_train_base} as a
LEFT JOIN get_consmr as b
on a.fca_id = b.fca_id
WHERE
CAST
((SUBSTRING(CAST(date_parse(d_recal_lnch,'%Y%m%d') as VARCHAR),1,10)) as date) BETWEEN date('${train_ref_date}') - INTERVAL '2' year AND date('${train_ref_date}')
),

get_fca_lvl as (
select a.fca_id,vin,type_vehicle,recall_type_safety,d_recal_lnch
FROM recall_consmr_frm_base as a
INNER JOIN ${owner} b
on a.fca_id=b.fca_id
)

select distinct fca_id,max(recall_type_safety) recall_type_safety
from get_fca_lvl
group by 1
