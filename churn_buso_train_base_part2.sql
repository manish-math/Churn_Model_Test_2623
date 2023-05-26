with date_vhcl_dispsl as (
SELECT fca_id,vin,l_new_vhcl,c_sales_typ_orig,true_disp, x_vhcl_make, x_nmplt,l_cert_usd_vhcl,d_purchase
from (
  SELECT fca_id,l_new_vhcl, x_vhcl_make, x_nmplt,i_vin_first_9||i_vin_last_8 as vin,
  LEAST(COALESCE(d_vhcl_dspsl,cast(CURRENT_DATE as VARCHAR)),
  COALESCE(d_leas_end,cast(CURRENT_DATE as VARCHAR))
  ) as true_disp,l_cert_usd_vhcl,d_purchase,c_sales_typ_orig,
  ROW_NUMBER()over(PARTITION by fca_id, CONCAT(i_vin_first_9,i_vin_last_8), d_purchase ORDER by t_stmp_upd desc) as p_rank
  from ${owner}
  where
  fca_id in (select distinct fca_id from churn_buso_train_base_part1)
    and date(d_purchase) BETWEEN (date('${train_ref_date}') - INTERVAL '60' day) AND (date('${train_ref_date}') + INTERVAL '60' day)
  and c_cntry_srce = '1'
  and c_sales_typ_orig in ('1','L')
  and upper(trim(c_mkt)) = 'U'
  and upper(trim(c_co_car_catgy)) = 'FALSE'
  and upper(trim(l_new_vhcl)) in ('NEW','USED')
)
where p_rank=1
),

exclusion_repurchasers as
(
select distinct a.fca_id
from date_vhcl_dispsl a inner join churn_buso_train_base_part1 b 
on a.fca_id=b.fca_id
where
date(a.d_purchase) BETWEEN (date(b.true_disp) - INTERVAL '60' day) AND (date(b.true_disp) + INTERVAL '30' day)
AND upper(trim(a.l_new_vhcl)) in ('NEW')
and a.true_disp>= '${train_ref_date}'
),

disposalin_ref_30 as
(select distinct fca_id,1 as churn_30 
from churn_buso_train_base_part1
where
date(true_disp) BETWEEN (date('${train_ref_date}') + INTERVAL '1' day) and (date('${train_ref_date}') + INTERVAL '30' day)
and fca_id not in (select distinct fca_id from exclusion_repurchasers)
-- GROUP by 2
),

mixed_base as
(
select fca_id,count(distinct(l_new_vhcl))
from churn_buso_train_base_part1 
group by 1
having count(distinct(l_new_vhcl)) > 1
),

final_disp as (
SELECT distinct fca_id,i_hshld_id_curr,status,vin,d_purchase,true_disp,l_new_vhcl,c_sales_typ_orig,retail_lease_flag,x_vhcl_make, x_nmplt,
churn_30
from (
    select distinct a.*,DENSE_RANK()over(PARTITION by vin ORDER by d_purchase desc) as p_rank,
    COALESCE(churn_30,0) churn_30
    from churn_buso_train_base_part1 a 
    left join disposalin_ref_30 b 
    on a.fca_id=b.fca_id
)
where p_rank=1
),

base as (
select *,case when fca_id in (select fca_id from mixed_base) then 'MIXED'
else l_new_vhcl end as type_vehicle from final_disp
),

mixed_hh_base as (
SELECT i_hshld_id_curr,count(distinct l_new_vhcl) cnt  from 
base 
group by 1 
)

SELECT distinct a.*, 
case when cnt>1 then 'MIXED' else l_new_vhcl end as household_type 
from base a 
inner join mixed_hh_base b 
on a.i_hshld_id_curr=b.i_hshld_id_curr