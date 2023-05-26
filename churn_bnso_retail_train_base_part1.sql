with common_base as (
SELECT fca_id,i_hshld_id_curr,vin,l_new_vhcl,c_sales_typ_orig,true_disp, x_vhcl_make, x_nmplt,l_cert_usd_vhcl,d_purchase
from (
SELECT fca_id,l_new_vhcl,i_hshld_id_curr , x_vhcl_make, x_nmplt,i_vin_first_9||i_vin_last_8 as vin,
LEAST(COALESCE(d_vhcl_dspsl,cast(CURRENT_DATE as VARCHAR)),
COALESCE(d_leas_end,cast(CURRENT_DATE as VARCHAR))
) as true_disp,l_cert_usd_vhcl,d_purchase,c_sales_typ_orig,
ROW_NUMBER()over(PARTITION by fca_id, CONCAT(i_vin_first_9,i_vin_last_8), d_purchase ORDER by t_stmp_upd desc) as p_rank
from ${owner}
where
(date(d_purchase) BETWEEN (date('${train_ref_date}') - INTERVAL '10' year) and date('${train_ref_date}'))
and c_cntry_srce = '1'
and c_sales_typ_orig in ('1','L')
and upper(trim(c_mkt)) = 'U'
and upper(trim(c_co_car_catgy)) = 'FALSE'
and upper(trim(l_new_vhcl)) in ('NEW','USED')
)
where p_rank=1
),

bad_customers as
(
  SELECT fca_id, count(distinct vin)
  FROM common_base
  group by 1
  having count(distinct vin) > 15
),

active_base as (
select distinct fca_id,i_hshld_id_curr,vin,l_new_vhcl,c_sales_typ_orig,l_cert_usd_vhcl,true_disp,d_purchase
from common_base
where fca_id in (select fca_id from common_base group by fca_id
having max(true_disp) >= '${train_ref_date}'
)
and fca_id not in (select fca_id from bad_customers)
and true_disp >= '${train_ref_date}'
),

reatil_lease_new_base as (
select *,'BNSO' as status
from active_base
where lower(l_new_vhcl) in ('new')
),

retail as (
select *,'Retail' as retail_lease_flag from reatil_lease_new_base
where c_sales_typ_orig='1' 
),

lease as (
select *,'Lease' as retail_lease_flag from reatil_lease_new_base
where c_sales_typ_orig='L'
and fca_id not in (select distinct fca_id from retail)
),

-- cpov_base as (
-- select *
-- from(select *,case when (lower(trim(l_new_vhcl))='used' and lower(trim(l_cert_usd_vhcl))='true') then 'CPOV' else 'BUSO' end as status
-- from active_base
-- where fca_id not in (select fca_id from new_base)
-- )
-- where status in ('CPOV')
-- ),

-- buso_base as (
-- select *,'BUSO' as status
-- from active_base
-- where fca_id not in (select fca_id from new_base)
-- and fca_id not in (select fca_id from cpov_base)
-- and lower(l_new_vhcl) in ('used')
-- ),
base as (
select fca_id,status,retail_lease_flag from retail 
UNION 
select fca_id,status,retail_lease_flag from lease 
-- select fca_id,status from cpov_base
-- union 
-- select fca_id,status from buso_base
)

SELECT distinct fca_id,status,retail_lease_flag,i_hshld_id_curr,vin,l_new_vhcl,c_sales_typ_orig,x_vhcl_make, x_nmplt,d_purchase,true_disp
from (
SELECT b.fca_id,status,retail_lease_flag,l_new_vhcl,i_hshld_id_curr,i_vin_first_9||i_vin_last_8 as vin,x_vhcl_make, x_nmplt,
LEAST(COALESCE(d_vhcl_dspsl,cast(CURRENT_DATE as VARCHAR)),
COALESCE(d_leas_end,cast(CURRENT_DATE as VARCHAR))
) as true_disp,l_cert_usd_vhcl,d_purchase,c_sales_typ_orig,
ROW_NUMBER()over(PARTITION by a.fca_id, CONCAT(i_vin_first_9,i_vin_last_8), d_purchase ORDER by t_stmp_upd desc) as p_rank
from ${owner} a 
inner join base b 
on a.fca_id=b.fca_id
where retail_lease_flag='Retail' 
and 
(date(d_purchase) BETWEEN (date('${train_ref_date}') - INTERVAL '10' year) and date('${train_ref_date}'))
and a.fca_id is not null 
and c_cntry_srce = '1'
and c_sales_typ_orig in ('1','L')
and upper(trim(c_mkt)) = 'U'
and upper(trim(c_co_car_catgy)) = 'FALSE'
and upper(trim(l_new_vhcl)) in ('NEW','USED')
)
where p_rank=1
and true_disp >= '${train_ref_date}'