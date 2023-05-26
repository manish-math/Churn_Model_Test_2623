SELECT DISTINCT fca_id,
DATE_DIFF('day',date(first_purchase),date('${train_ref_date}'))/365.0 as stellantis_own_tenure
from (
SELECT fca_id,min(d_purchase) first_purchase
from (
SELECT fca_id,CONCAT(i_vin_first_9,i_vin_last_8) as vin,d_purchase,d_vhcl_dspsl,d_leas_end,l_new_vhcl,
ROW_NUMBER()over(PARTITION by fca_id,CONCAT(i_vin_first_9,i_vin_last_8),d_purchase ORDER by t_stmp_upd desc) as p_rank
from ${owner} 
where
(date(d_purchase) BETWEEN (date('${train_ref_date}') - INTERVAL '10' year) and date('${train_ref_date}'))  
and fca_id in (SELECT DISTINCT fca_id from ${buso_retail_base})
and c_cntry_srce = '1'
and c_sales_typ_orig in ('1','L')
and upper(trim(c_mkt)) = 'U'
and upper(trim(c_co_car_catgy)) = 'FALSE'
and upper(trim(l_new_vhcl)) in ('NEW','USED')
)
where p_rank=1
group by 1 
)