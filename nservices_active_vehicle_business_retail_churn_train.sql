with active_lease_vehicle as (
Select fca_id,(i_vin_first_9||i_vin_last_8) as vin,d_purchase, d_vhcl_dspsl, d_leas_end, c_vhcl_bdy_mod, l_new_vhcl, c_sales_typ_orig,
LEAST(COALESCE(d_vhcl_dspsl,cast(CURRENT_DATE as VARCHAR)),COALESCE(d_leas_end,cast(CURRENT_DATE as VARCHAR))) as true_disp,
ROW_NUMBER()over(partition by fca_id, i_vin_first_9||i_vin_last_8, d_purchase order by t_stmp_upd desc) as row_rank
from ${owner}
where (date(d_purchase) BETWEEN (date('${train_ref_date}') - INTERVAL '10' year) and date('${train_ref_date}'))
and (d_vhcl_dspsl > '${train_ref_date}'  or (d_vhcl_dspsl is null))
and (d_leas_end > '${train_ref_date}' or (d_leas_end is null))
and c_cntry_srce = '1'
and upper(trim(c_mkt)) = 'U'
and upper(trim(c_co_car_catgy)) = 'FALSE'
and upper(trim(l_new_vhcl)) in ('NEW', 'USED')
and fca_id in (select fca_id from ${business_retail_base})
)
,

no_of_serv_done as (
Select i_vin as vin ,i_ro,d_ro_invc,c_ro_reas                                   
from ${unif_serv_rec}                                     
where c_ro_reas in ('1','3','-1')
and (date(d_ro_invc) BETWEEN (date('${train_ref_date}') - INTERVAL '10' year) and date('${train_ref_date}'))
and i_vin in (select distinct vin from active_lease_vehicle)
)
,

nservices_leased_active  as 
(select distinct vin,services_done as nservices_leased_active
from 
(
  select a.vin, count(distinct trim(i_ro, ' ')) as services_done
from active_lease_vehicle a
inner join no_of_serv_done b 
on a.vin = b.vin 
group by 1
)
)


select distinct b.fca_id,b.vin,
case
when cast(nservices_leased_active as integer) = 1 then '1'
when cast(nservices_leased_active as integer) = 2 then '2'
when cast(nservices_leased_active as integer) = 3 then '3'
when cast(nservices_leased_active as integer) = 4 then '4'
when cast(nservices_leased_active as integer) >= 5 then '>=5'
end as nservices_active_vehicle
from ( 
select vin, nservices_leased_active from nservices_leased_active
) a 
inner join ${business_retail_base} b
on a.vin = b.vin 