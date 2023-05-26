with churn_base as (
SELECT fca_id,vin
from ${business_lease_base}
),

recall_data as (
SELECT trim(i_vin,' ') as i_vin,trim(d_recal_lnch, ' ') as d_recal_lnch
,a.c_vhcl_recal,l_vin
FROM ${stg_recal_sseo_vin} as a
INNER JOIN ${stg_recal_code} as b
on a.c_vhcl_recal=b.c_vhcl_recal
where trim(i_vin) in (select vin from churn_base)
),
recall_cust as (
SELECT i_vin
,CASE WHEN l_vin='N' THEN 1 else 0
END as retail_recall_open_sseo_std,d_recal_lnch
from recall_data
),

get_consmr as (
SELECT distinct fca_id,a.i_vin,retail_recall_open_sseo_std,d_recal_lnch
from recall_cust as a 
INNER JOIN churn_base as b
on a.i_vin =b.vin
),

recall_consmr_frm_base as ( 
SELECT distinct a.fca_id,a.vin
,COALESCE(retail_recall_open_sseo_std,0) as retail_recall_open_sseo_std,d_recal_lnch
from ${business_lease_base} as a
LEFT JOIN get_consmr as b
on a.fca_id = b.fca_id
and a.vin=b.i_vin
WHERE
c_sales_typ_orig='E' 
AND CAST
((SUBSTRING(CAST(date_parse(d_recal_lnch,'%Y%m%d') as VARCHAR),1,10)) as date) BETWEEN date('${train_ref_date}') - INTERVAL '2' year AND date('${train_ref_date}')
),

get_fca_lvl as (
select fca_id,vin,retail_recall_open_sseo_std,d_recal_lnch
FROM recall_consmr_frm_base
)


select distinct fca_id,vin,max(retail_recall_open_sseo_std) as lease_recall_open_sseo_std
from get_fca_lvl
group by 1,2