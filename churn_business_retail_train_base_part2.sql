with date_vhcl_dispsl as (
SELECT fca_id,vin,l_new_vhcl,c_sales_typ_orig,true_disp,l_cert_usd_vhcl,d_purchase
from (
  SELECT fca_id,l_new_vhcl,i_vin_first_9||i_vin_last_8 as vin,
  LEAST(COALESCE(d_vhcl_dspsl,cast(CURRENT_DATE as VARCHAR)),
  COALESCE(d_leas_end,cast(CURRENT_DATE as VARCHAR))
  ) as true_disp,l_cert_usd_vhcl,d_purchase,c_sales_typ_orig,
  ROW_NUMBER()over(PARTITION by fca_id, CONCAT(i_vin_first_9,i_vin_last_8), d_purchase ORDER by t_stmp_upd desc) as p_rank
  from ${owner}
  where
  fca_id in (select distinct fca_id from churn_business_retail_base_part1)
  and date(d_purchase) BETWEEN (date('${train_ref_date}') - INTERVAL '60' day) AND (date('${train_ref_date}') + INTERVAL '60' day)
  and c_cntry_srce = '1'
  and c_sales_typ_orig in ('E','B','C')
  and upper(trim(c_mkt)) = 'U'
  and upper(trim(c_co_car_catgy)) = 'FALSE'
  and upper(trim(l_new_vhcl)) in ('NEW','USED')
)
where p_rank=1
and true_disp>='${train_ref_date}'
)
,
  
exclusion_repurchasers as (
select distinct a.fca_id
from churn_business_retail_base_part1 a
inner join  date_vhcl_dispsl b
on a.fca_id = b.fca_id
where
date(b.d_purchase) BETWEEN (date(a.true_disp) - INTERVAL '60' day) AND (date(a.true_disp) + INTERVAL '30' day)
AND upper(trim(b.l_new_vhcl)) in ('NEW')
)
,



disposalin_ref_30 as
(select distinct fca_id,1 as churn_30
from churn_business_retail_base_part1
where
date(true_disp) BETWEEN (date('${train_ref_date}') + INTERVAL '1' day) and (date('${train_ref_date}') + INTERVAL '30' day)
and fca_id not in (select distinct fca_id from exclusion_repurchasers)
)
,




-- bad_customers as
-- (
--   SELECT fca_id, count(distinct vin)
--   FROM date_vhcl_dispsl
--   group by 1
--   having count(distinct vin) > 15
-- ),



-- mixed_base as
-- (
-- select fca_id,count(distinct(l_new_vhcl))
-- from churn_bnso_retail_base_part1
-- group by 1
-- having count(distinct(l_new_vhcl)) > 1
-- ),




final_disp as (
SELECT distinct fca_id,i_consmr,status,i_hshld_id_curr,retail_lease_flag,vin,d_purchase,true_disp,l_new_vhcl,c_sales_typ_orig,x_vhcl_make, x_nmplt,
churn_30
from (
    select distinct a.*,DENSE_RANK()over(PARTITION by vin ORDER by d_purchase desc) as p_rank,
    COALESCE(churn_30,0) churn_30
    from churn_business_retail_base_part1 a
    left join disposalin_ref_30 b
    on a.fca_id=b.fca_id
-- where
-- a.fca_id not in (select fca_id from churn_bnso_buso_retail_lease)
)
where p_rank=1
),



-- select * from final_disp



base as (
select *
-- case when fca_id in (select fca_id from mixed_base) then 'MIXED'
-- else l_new_vhcl end as type_vehicle
from final_disp
),



-- mixed_hh_base as (
-- SELECT i_hshld_id_curr,count(distinct l_new_vhcl) cnt  from
-- base
-- group by 1
-- ),



-- base2 as (
-- SELECT a.*,
-- case when cnt>1 then 'MIXED' else l_new_vhcl end as household_type
-- from base a
-- inner join mixed_hh_base b
-- on a.i_hshld_id_curr=b.i_hshld_id_curr
-- ),



vin_disp_base as (
SELECT *,1 as vin_disp_30 from base
where churn_30 = 1
and date(true_disp) BETWEEN (date('${train_ref_date}') + INTERVAL '1' day) and (date('${train_ref_date}') + INTERVAL '30' day)
)



SELECT a.*,COALESCE(vin_disp_30,0) vin_disp_30
from base
a left join vin_disp_base b
on a.fca_id=b.fca_id
and a.vin=b.vin