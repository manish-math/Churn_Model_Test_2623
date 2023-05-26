With ownership as 
(
  select * from (
Select 
  distinct fca_id,(i_vin_first_9||i_vin_last_8) as vin,
  d_purchase,
  d_vhcl_dspsl,
  c_sales_typ_orig,      
  c_vhcl_bdy_mod,
  i_mod_yr,
  LEAST(COALESCE(cast(trim(d_vhcl_dspsl) as date),current_date),COALESCE(cast(trim(d_leas_end) as date),current_date)) as true_disp,
  ROW_NUMBER()over(partition by fca_id, i_vin_first_9||i_vin_last_8, d_purchase order by t_stmp_upd desc) as row_rank
from ${owner}
where (date(d_purchase) between  date('${train_ref_date}') - interval '10' year and date('${train_ref_date}'))
and fca_id in (select distinct fca_id from ${business_retail_base})
and c_cntry_srce = '1'
and c_sales_typ_orig in ('E','B','C')
and upper(trim(c_mkt)) = 'U'
and upper(trim(c_co_car_catgy)) = 'FALSE'
and upper(trim(l_new_vhcl)) in ('NEW','USED')
and LEAST(COALESCE(cast(trim(d_vhcl_dspsl) as date),current_date),COALESCE(cast(trim(d_leas_end) as date),current_date))
  between cast('${train_ref_date}' as date) - interval '10' year and cast('${train_ref_date}' as date)
  )
  where row_rank=1
)


select fca_id,count(distinct vin) num_vins_disposed from ownership
group by 1 