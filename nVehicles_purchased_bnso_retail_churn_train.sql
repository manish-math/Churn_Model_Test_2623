with nVehicles_purchased as 
(
  Select 
  fca_id,
  count(distinct(i_vin_first_9||i_vin_last_8)) as nVehicles_purchased_ft
from ${owner}
where cast(d_purchase as date) between cast('${train_ref_date}' as date) - interval '10' year and cast('${train_ref_date}' as date)
and c_cntry_srce = '1'
and c_sales_typ_orig in ('1','L')
and upper(trim(c_mkt)) = 'U'
and upper(trim(c_co_car_catgy)) = 'FALSE'
and upper(trim(l_new_vhcl)) in ('NEW','USED')
and fca_id in (select fca_id from ${bnso_retail_train_base})
group by 1
)

select distinct fca_id,
case when nVehicles_purchased_ft = 1 then '1'
when nVehicles_purchased_ft = 2 then '2'
when nVehicles_purchased_ft = 3 then '3'
else '>3' end as nVehicles_purchased_lifetime
from nVehicles_purchased 