with nVehicles_purchased as 
(
  Select 
  fca_id,
  count(distinct(i_vin_first_9||i_vin_last_8)) as nVehicles_purchased_ft
from ${owner}
where cast(d_purchase as date) between cast('${train_ref_date}' as date) - interval '10' year and cast('${train_ref_date}' as date)
and trim(c_cntry_srce) like '1'
and trim(c_sales_typ_orig) in ('E')
and fca_id in (select fca_id from ${business_lease_base})
group by 1
)

select b.fca_id,
case when nVehicles_purchased_ft = 1 then '1'
when nVehicles_purchased_ft = 2 then '2'
when nVehicles_purchased_ft = 3 then '3'
else '>3' end as nVehicles_leased_act_inact
from nVehicles_purchased a
inner join ${business_lease_base} b
on a.fca_id=b.fca_id