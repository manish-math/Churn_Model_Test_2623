with prev_disposal as
(
select distinct fca_id,true_disposal,prev_disp  
from (
select fca_id,true_disposal,LAG(date(true_disposal),1) over(partition by fca_id order by DATE(true_disposal)) prev_disp 
from(
select distinct fca_id, i_vin_first_9||i_vin_last_8 ,
LEAST(COALESCE(date(d_vhcl_dspsl),CURRENT_DATE ),COALESCE(date(d_leas_end) ,CURRENT_DATE)) as true_disposal
from ${owner}
where
fca_id in (select fca_id from ${business_retail_base})
and c_cntry_srce = '1'
and upper(trim(c_mkt)) = 'U'
and upper(trim(c_co_car_catgy)) = 'FALSE'
)
where date(true_disposal) <= date('${train_ref_date}')
)
),


avg_days_between_disposals as(
select distinct fca_id, nAvgDaysBetween_disposals as nAvgDaysBetween_disposals
from (
select distinct fca_id,round(avg(time_between_dispose),2) as nAvgDaysBetween_disposals
 from(
select a.fca_id,a.true_disposal,prev_disp, DATE_DIFF('DAY',DATE(a.prev_disp),DATE(a.true_disposal))as time_between_dispose
from prev_disposal a
inner join ${business_retail_base} b
on a.fca_id=b.fca_id
)
group by 1
)
)

select distinct a.fca_id,
case 
when nAvgDaysBetween_disposals between 0 and 30 then '0-30'
when nAvgDaysBetween_disposals  between 30 and 60 then '30-60'
when nAvgDaysBetween_disposals  between 60 and 90 then '60-90'
when nAvgDaysBetween_disposals between 90 and 120 then '90-120'
when nAvgDaysBetween_disposals between 120 and 150 then '120-150'
when nAvgDaysBetween_disposals between 150 and 180 then '150-180'
when nAvgDaysBetween_disposals >180 then '>180'
end as nAvgDaysBetween_disposals
from avg_days_between_disposals a inner join ${business_retail_base} b
on a.fca_id = b.fca_id