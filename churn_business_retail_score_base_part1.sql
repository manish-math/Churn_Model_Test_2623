with common_base as (

select * from (
select distinct fca_id ,i_consmr ,i_vin_first_9||i_vin_last_8 as vin ,c_sales_typ_orig,d_vhcl_dspsl,d_leas_end,l_new_vhcl, d_purchase,l_cert_usd_vhcl,i_hshld_id_curr , x_vhcl_make, x_nmplt,
LEAST(COALESCE(d_vhcl_dspsl,cast(CURRENT_DATE as VARCHAR)),
COALESCE(d_leas_end,cast(CURRENT_DATE as VARCHAR))
) as true_disp , 
ROW_NUMBER()over(PARTITION by fca_id, CONCAT(i_vin_first_9,i_vin_last_8), d_purchase ORDER by t_stmp_upd desc) as p_rank
from ${owner}
-- i_vin_first_9||i_vin_last_8
where c_sales_typ_orig in ('E','B','C')
and (date(d_purchase) BETWEEN (date('${train_ref_date}') - INTERVAL '10' year) and date('${train_ref_date}'))
and c_cntry_srce = '1'
and upper(trim(c_mkt)) = 'U')
where p_rank = 1)
, 

active_base as (
select distinct fca_id,i_consmr,vin,l_new_vhcl,c_sales_typ_orig,l_cert_usd_vhcl,true_disp,d_purchase
from common_base
where fca_id in (select fca_id from common_base group by fca_id
having max(true_disp) >= '${train_ref_date}'
)
and true_disp >= '${train_ref_date}'
)
,

-- new_base as (
-- select *,'BNSO' as status
-- from active_base
-- where lower(l_new_vhcl) in ('new')
-- ),


-- cpov_base as (
-- select *
-- from(select *,case when (lower(trim(l_new_vhcl))='used' and lower(trim(l_cert_usd_vhcl))='true') then 'CPOV' else 'BUSO' end as status
-- from active_base
-- where i_consmr not in (select i_consmr from new_base)
-- )
-- where status in ('CPOV')
-- ),

-- buso_base as (
-- select *,'BUSO' as status
-- from active_base
-- where i_consmr not in (select i_consmr from new_base)
-- and i_consmr not in (select i_consmr from cpov_base)
-- and lower(l_new_vhcl) in ('used')
-- ),
category as (
select array_join((ARRAY_SORT(ARRAY_DISTINCT(array_agg(trim(c_sales_typ_orig))))),'+') as category ,fca_id from active_base
group by 2 )
,

retail as (
select *,'Retail' as retail_lease_flag from category
where category in ('B', 'B+E', 'C') 
),

lease as (
select *,'Lease' as retail_lease_flag from category
where category ='E'
and fca_id not in (select distinct fca_id from retail)
)
,


base as (
select fca_id,retail_lease_flag from retail 
-- UNION 
-- select fca_id,retail_lease_flag from lease 
)


SELECT fca_id,i_consmr,'Business_Retail' as status,retail_lease_flag,i_hshld_id_curr,vin,l_new_vhcl,c_sales_typ_orig,d_purchase,true_disp, x_vhcl_make, x_nmplt
from (
SELECT b.fca_id,a.i_consmr,retail_lease_flag,l_new_vhcl,i_hshld_id_curr , x_vhcl_make, x_nmplt,i_vin_first_9||i_vin_last_8 as vin,
LEAST(COALESCE(d_vhcl_dspsl,cast(CURRENT_DATE as VARCHAR)),
COALESCE(d_leas_end,cast(CURRENT_DATE as VARCHAR))
) as true_disp,l_cert_usd_vhcl,d_purchase,c_sales_typ_orig,
ROW_NUMBER()over(PARTITION by a.i_consmr, CONCAT(i_vin_first_9,i_vin_last_8), d_purchase ORDER by t_stmp_upd desc) as p_rank
from ${owner} a 
inner join base b 
on a.fca_id=b.fca_id
where retail_lease_flag='Retail'
and 
(date(d_purchase) BETWEEN (date('${train_ref_date}') - INTERVAL '10' year) and date('${train_ref_date}'))
and
c_cntry_srce = '1'
and c_sales_typ_orig in ('E','B','C')
and upper(trim(c_mkt)) = 'U'
and upper(trim(c_co_car_catgy)) = 'FALSE'
and upper(trim(l_new_vhcl)) in ('NEW','USED')
)
where p_rank=1
and true_disp >= '${train_ref_date}'
-- and fca_id not in (select fca_id from churn_bnso_buso_retail_lease_score)