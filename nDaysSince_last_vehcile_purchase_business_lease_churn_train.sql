with currently_owned as 
(
select distinct fca_id,d_purchase from (
  Select 
  fca_id,d_purchase,DENSE_RANK()over(PARTITION by fca_id ORDER by d_purchase desc) as recent_purchase
from ${owner}
where cast(d_purchase as date) between cast('${train_ref_date}' as date) - interval '10' year and cast('${train_ref_date}' as date)
and c_cntry_srce = '1'
-- and c_sales_typ_orig in ('1','L')
and upper(trim(c_mkt)) = 'U'
and upper(trim(c_co_car_catgy)) = 'FALSE'
and upper(trim(l_new_vhcl)) in ('NEW','USED')
and fca_id in (select fca_id from ${business_lease_base})
)
where recent_purchase=1
)

select distinct fca_id,
case 
when nDaysSince_last_purchase between 0 and 30 then '0-30'
when nDaysSince_last_purchase between 31 and 60 then '31-60'
when nDaysSince_last_purchase between 61 and 90 then '61-90'
when nDaysSince_last_purchase between 91 and 180 then '91-180'
when nDaysSince_last_purchase >180 then '>180'
end as nDaysSince_last_vehcile_purchase
from (
select distinct fca_id, nDaysSince_last_purchase from (
select distinct fca_id ,DATE_DIFF('day', date(d_purchase) ,date('${train_ref_date}')) as nDaysSince_last_purchase
from currently_owned 
)
)
