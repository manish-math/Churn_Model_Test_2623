with prev_purchase as
(
select distinct fca_id,d_purchase,prev_pur  
from (
select distinct fca_id, i_vin_first_9||i_vin_last_8 ,
LAG(date(d_purchase),1) over(partition by fca_id order by DATE(d_purchase)) prev_pur ,d_purchase
from ${owner}
where
fca_id in (select fca_id from ${business_retail_base})
and c_cntry_srce = '1'
and upper(trim(c_mkt)) = 'U'
and upper(trim(c_co_car_catgy)) = 'FALSE'
and date(d_purchase) between date('${train_ref_date}') - interval '10' year and date('${train_ref_date}')
)
),


avg_days_between_purchases as(
select distinct fca_id, nAvgDaysBetween_purchases as nAvgDaysBetween_purchases from (
select distinct fca_id,round(avg(time_between_purchase),2) as nAvgDaysBetween_purchases from
(
select a.fca_id,a.d_purchase,prev_pur, DATE_DIFF('DAY',DATE(a.prev_pur),DATE(a.d_purchase))as time_between_purchase
from prev_purchase a
inner join ${business_retail_base} b
on a.fca_id=b.fca_id
)
group by 1
))

select distinct a.fca_id,
case 
when nAvgDaysBetween_purchases between 0 and 30 then '0-30'
when nAvgDaysBetween_purchases  between 30 and 60 then '30-60'
when nAvgDaysBetween_purchases  between 60 and 90 then '60-90'
when nAvgDaysBetween_purchases between 90 and 120 then '90-120'
when nAvgDaysBetween_purchases between 120 and 150 then '120-150'
when nAvgDaysBetween_purchases between 150 and 180 then '150-180'
when nAvgDaysBetween_purchases >180 then '>180'
end as nAvgDaysBetween_purchases
from avg_days_between_purchases a inner join ${business_retail_base} b
on a.fca_id = b.fca_id 
