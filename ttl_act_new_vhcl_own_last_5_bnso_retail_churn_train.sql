SELECT 
a.fca_id, COUNT(DISTINCT a.vin) as ttl_act_new_vhcl_own_last_5
FROM ${bnso_retail_train_base} as a
LEFT join
(
  SELECT * FROM
  (
  SELECT fca_id, d_vhcl_dspsl, CONCAT(i_vin_first_9,i_vin_last_8) as vin,
  LEAST(COALESCE(d_vhcl_dspsl,cast(CURRENT_DATE as VARCHAR)),COALESCE(d_leas_end,cast(CURRENT_DATE as VARCHAR))) as tru_disp,
  ROW_NUMBER()over(PARTITION BY fca_id, CONCAT(i_vin_first_9,i_vin_last_8),d_purchase ORDER BY t_stmp_upd DESC) as p_rank
  FROM ${owner} 
  WHERE 
  c_cntry_srce = '1'
  and c_sales_typ_orig in ('1','L')
  and upper(trim(c_mkt)) = 'U'
  and upper(trim(c_co_car_catgy)) = 'FALSE'
  and upper(trim(l_new_vhcl)) in ('NEW')
  AND date(d_purchase) BETWEEN (date('${train_ref_date}') - INTERVAL '5' year) AND
  date('${train_ref_date}')
  )
  WHERE p_rank=1
  and date(tru_disp)>date('${train_ref_date}')
) as b
on a.fca_id=b.fca_id
GROUP BY 1