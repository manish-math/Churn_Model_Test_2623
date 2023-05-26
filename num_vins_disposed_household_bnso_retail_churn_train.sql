With ownership as 
(
  select * from (
Select 
  distinct fca_id, i_hshld_id_curr,
  (i_vin_first_9||i_vin_last_8) as vin,
  d_purchase,
  d_vhcl_dspsl,
  c_sales_typ_orig,      
  c_vhcl_bdy_mod,
  i_mod_yr,
  ROW_NUMBER()over(partition by fca_id, i_vin_first_9||i_vin_last_8, d_purchase order by t_stmp_upd desc) as row_rank
from ${owner}
where (date(d_purchase) between  date('${train_ref_date}') - interval '10' year and date('${train_ref_date}'))
and c_cntry_srce = '1'
and c_sales_typ_orig in ('1','L')
and upper(trim(c_mkt)) = 'U'
and upper(trim(c_co_car_catgy)) = 'FALSE'
and upper(trim(l_new_vhcl)) in ('NEW','USED')
and i_hshld_id_curr not like ''
and LEAST(COALESCE(cast(trim(d_vhcl_dspsl) as date),current_date),COALESCE(cast(trim(d_leas_end) as date),current_date))
  between cast('${train_ref_date}' as date) - interval '10' year and cast('${train_ref_date}' as date)
  )
  where row_rank=1
),

disp_in_hshld as (
select i_hshld_id_curr,count(distinct vin) num_vins_disposed from ownership
group by 1 
)

select fca_id,
case when num_vins_disposed <= 1 then '<=1'
when num_vins_disposed = 2 then '2'
when num_vins_disposed = 3 then '3'
when num_vins_disposed = 4 then '4'
when num_vins_disposed > 4 then '>4' end as num_vins_disposed_household
from (
select a.fca_id,
COALESCE(max(num_vins_disposed),0) num_vins_disposed
from ${bnso_retail_train_base} a 
inner join disp_in_hshld b
on a.i_hshld_id_curr=b.i_hshld_id_curr 
group by 1  
)