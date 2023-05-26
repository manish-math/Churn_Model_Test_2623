SELECT fca_id,count(DISTINCT vin) num_new_vins_pur_last_5_years from (
SELECT DISTINCT fca_id,vin
from (
SELECT fca_id,CONCAT(i_vin_first_9,i_vin_last_8) as vin,d_purchase,d_vhcl_dspsl,d_leas_end,l_new_vhcl,
LEAST(COALESCE(d_vhcl_dspsl,cast(CURRENT_DATE as VARCHAR)),COALESCE(d_leas_end,cast(CURRENT_DATE as VARCHAR))) as tru_disp,
ROW_NUMBER()over(PARTITION by fca_id,CONCAT(i_vin_first_9,i_vin_last_8),d_purchase ORDER by t_stmp_upd desc) as p_rank
from ${owner} 
where fca_id in (SELECT DISTINCT fca_id from ${buso_retail_base})
and (date(d_purchase) BETWEEN (date('${train_ref_date}') - INTERVAL '5' year) and date('${train_ref_date}')) 
and c_cntry_srce = '1'
and c_sales_typ_orig in ('1','L')
and upper(trim(c_mkt)) = 'U'
and upper(trim(c_co_car_catgy)) = 'FALSE'
and upper(trim(l_new_vhcl)) in ('NEW')
)
where p_rank=1
)
group by 1