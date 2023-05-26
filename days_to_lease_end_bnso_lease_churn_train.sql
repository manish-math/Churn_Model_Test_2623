with days_to_lease_end_std as (
  select * from (
select 
  distinct fca_id,
  i_vin_first_9||i_vin_last_8 as vin,
  d_purchase,
  d_vhcl_dspsl,
  d_leas_end,
  DATE_DIFF('day', date('${train_ref_date}'), cast(d_leas_end as date)) as days_to_lease_end_ft,
  ROW_NUMBER()over(partition by i_consmr, i_vin_first_9||i_vin_last_8, d_purchase order by t_stmp_upd desc) as row_rank
from ${owner}
where c_cntry_srce = '1'
  and c_sales_typ_orig in ('L')
  and fca_id in (select fca_id from ${bnso_lease_train_base})
  and cast(d_purchase as date) between cast('${train_ref_date}' as date) - interval '10' year and cast('${train_ref_date}' as date)
  and (cast(d_leas_end as date) >= cast('${train_ref_date}' as date) or (d_leas_end is null))
)
  where row_rank=1
)  ,

days_left as
(
select 
  b.vin,b.fca_id,
  COALESCE(days_to_lease_end_ft,0) as days_to_lease_end_std
from ${bnso_lease_train_base} a
inner join days_to_lease_end_std b 
on a.fca_id=b.fca_id and
a.vin = b.vin
)


select distinct fca_id,vin,days_to_lease_end_std
-- case when days_to_lease_end_std >= 0 and days_to_lease_end_std <=30 then '0-30'
-- when days_to_lease_end_std > 30 and days_to_lease_end_std <= 60 then '30-60'
-- when days_to_lease_end_std > 60 and days_to_lease_end_std <=90 then '60-90'
-- when days_to_lease_end_std > 90 and days_to_lease_end_std <=180 then '90-180'
-- when days_to_lease_end_std >180 and days_to_lease_end_std <= 360 then '180-360'
-- when days_to_lease_end_std > 360 and days_to_lease_end_std <= 720 then '360-720'
-- when days_to_lease_end_std > 720 and days_to_lease_end_std <= 1080 then '720-1080'
-- when days_to_lease_end_std > 1080 then '>1080' end as days_to_lease_end_std
from days_left
