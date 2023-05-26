with vin_level_churn as (
SELECT * from ${buso_retail_base}
)
,

ownership as
(
select * from
(
select
  distinct fca_id,
  concat(i_vin_first_9,i_vin_last_8) as i_vin,
  d_purchase,LEAST(COALESCE(d_vhcl_dspsl,cast(CURRENT_DATE as VARCHAR)),COALESCE(d_leas_end,cast(CURRENT_DATE as VARCHAR))) as true_disp,
  ROW_NUMBER() over(partition by fca_id, i_vin_first_9||i_vin_last_8, d_purchase order by t_stmp_upd desc) as row_rank
from ${owner}
where (date(d_purchase) BETWEEN (date('${train_ref_date}') - INTERVAL '10' year) and date('${train_ref_date}'))
  and  upper(trim(c_mkt)) = 'U'   
    and c_sales_typ_orig in ('1')           
  and upper(trim(c_co_car_catgy)) = 'FALSE'
and fca_id in (select distinct fca_id from vin_level_churn)   -- for CPOV retail
)
where
date(true_disp) >= date('${train_ref_date}')
and
row_rank=1
)


select distinct fca_id,
case
when no_of_hands_exchanged = 1 then '1'
when no_of_hands_exchanged = 2 then '2'
when no_of_hands_exchanged = 3 then '3'
when no_of_hands_exchanged = 4 then '4'
when no_of_hands_exchanged >=5 then '>=5'
end no_of_hands_exchanged
from (
select fca_id, COALESCE(avg(no_of_hands_exchanged),0) as no_of_hands_exchanged  from
(
select i_vin,cast(avg(cnt) as integer)  as  no_of_hands_exchanged   from
(
select a.i_vin,count(distinct b.fca_id) as cnt  
from ownership a inner join
(
select * from
(
select
  fca_id,
  concat(i_vin_first_9,i_vin_last_8) as i_vin,
  d_purchase,
  ROW_NUMBER()over(partition by fca_id, i_vin_first_9||i_vin_last_8, d_purchase order by t_stmp_upd desc) as row_rank
from ${owner}
where (date(d_purchase) BETWEEN (date('${train_ref_date}') - INTERVAL '10' year) and date('${train_ref_date}'))
  and  upper(trim(c_mkt)) = 'U'   
    and c_sales_typ_orig in ('1')           
  and upper(trim(c_co_car_catgy)) = 'FALSE'
)
where row_rank=1
) as b
on a.i_vin = b.i_vin
and a.d_purchase > b.d_purchase
and a.fca_id != b.fca_id
group by 1
)
group by 1
) as a
inner join vin_level_churn
as b
on a.i_vin=b.vin
group by 1 
)
