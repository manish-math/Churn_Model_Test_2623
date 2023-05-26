with base_data as
(
select distinct fca_id,purchase_date,VIN,true_disposal,p_rank
from
(
select distinct fca_id, trim((i_vin_first_9 || i_vin_last_8), ' ') as VIN,
date(d_purchase) as purchase_date,
LEAST(COALESCE(date(d_vhcl_dspsl),CURRENT_DATE ),COALESCE(date(d_leas_end) ,CURRENT_DATE)) as true_disposal,
ROW_NUMBER()over(PARTITION by fca_id,CONCAT(i_vin_first_9,i_vin_last_8),d_purchase ORDER by t_stmp_upd desc) as p_rank
from ${owner}
where date(d_purchase) BETWEEN (date('${train_ref_date}') - INTERVAL '10' year) and date('${train_ref_date}')
and fca_id in (select distinct fca_id from ${buso_retail_base})
  and c_cntry_srce = '1'
  and c_sales_typ_orig in ('1','L')
  and upper(trim(c_mkt)) = 'U'
  and upper(trim(c_co_car_catgy)) = 'FALSE'
  and upper(trim(l_new_vhcl)) in ('NEW','USED')
) 
where p_rank=1
),

purchase_count as
(
select distinct fca_id,
count (distinct VIN)  as total_purchase_count
from base_data
where date(purchase_date) BETWEEN (date('${train_ref_date}') - INTERVAL '10' year) and date('${train_ref_date}')
group by 1
),

total_disposed as
(
select distinct fca_id,count(distinct VIN) as total_count_disposed 
from base_data
where date(true_disposal) <= date('${train_ref_date}')
group by 1
),

purchase_disposal as
(
select a.*,COALESCE(b.total_count_disposed,0) as total_count_disposed
from purchase_count as a
left join total_disposed as b
on a.fca_id = b.fca_id
)


select distinct c.fca_id,(total_count_disposed*1.0/total_purchase_count) as disposed_ratio
from ${buso_retail_base}  c
left join purchase_disposal d
on c.fca_id=d.fca_id
group by 1,2