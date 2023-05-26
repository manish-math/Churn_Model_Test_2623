SELECT DISTINCT fca_id,
case when stellantis_own_tenure between 0 and 1 then ' 0-1' 
when stellantis_own_tenure between 2 and 3 then ' 2-3' 
when stellantis_own_tenure between  4 and 5 then ' 4-5'
when stellantis_own_tenure >5 then ' >5' 
end as stellantis_own_tenure from( 
  select fca_id,
DATE_DIFF('day',date(first_purchase),date('${train_ref_date}'))/365.0 as stellantis_own_tenure
from (
SELECT fca_id,min(d_purchase) first_purchase
from (
SELECT fca_id,CONCAT(i_vin_first_9,i_vin_last_8) as vin,d_purchase,d_vhcl_dspsl,d_leas_end,l_new_vhcl,
ROW_NUMBER()over(PARTITION by fca_id,CONCAT(i_vin_first_9,i_vin_last_8),d_purchase ORDER by t_stmp_upd desc) as p_rank
from ${owner} 
where
(date(d_purchase) BETWEEN (date('${train_ref_date}') - INTERVAL '10' year) and date('${train_ref_date}'))  
and fca_id in (SELECT DISTINCT fca_id from ${business_retail_base})
)
where p_rank=1
group by 1 
)
)
group by 1, 2