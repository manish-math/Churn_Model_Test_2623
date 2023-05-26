with service_downtime as (
select fca_id,vin,ROUND(avg(ABS(DATE_DIFF('day',date(d_ro_open),date(d_ro_invc)))),2) as avg_service_downtime 
from (
select distinct b.fca_id, b.vin,b.d_purchase, a.d_ro_invc, a.d_ro_open,b.true_disp as true_disposal
from ${unif_serv_rec} a 
inner join ${business_retail_base} b 
on a.fca_id=b.fca_id
and a.i_vin = b.vin
where
-- c_sales_typ_orig in ('E')
-- c_sales_typ_orig in ('B')
 c_ro_reas in ('1','3','-1')
and (date(d_ro_invc) BETWEEN (date('${train_ref_date}') - INTERVAL '10' year) and date('${train_ref_date}'))
and d_ro_open != '00010101')
where date(d_purchase) <= date(d_ro_invc)
and date(d_ro_invc)  <= date(true_disposal)
group by 1,2
)
,

churn_30 as (
select a.fca_id,a.avg_service_downtime,a.vin
from service_downtime a
inner join ${business_retail_base} b
on a.vin=b.vin
)

select distinct fca_id ,vin,
case 
when avg_service_downtime <= 1 then '<=1'
when avg_service_downtime > 1 then '>1'
end as avg_days_btw_repord_invcord
from churn_30