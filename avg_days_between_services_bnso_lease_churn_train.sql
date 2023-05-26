with days_since_last_service_base as
(
  select distinct fca_id, vin, d_purchase, i_ro, l_new_vhcl, c_sales_typ_orig, d_ro_invc
  from
   (
                  Select  
                    distinct fca_id,
                     vin,
                    d_purchase,
                   c_sales_typ_orig,
                   true_disp ,
                   l_new_vhcl                
                  from ${bnso_lease_train_base}
                  where
               
                cast(d_purchase as date) between cast('${train_ref_date}' as date) - interval '10' year and cast('${train_ref_date}' as date)
                  and l_new_vhcl='NEW'
                  and retail_lease_flag= 'Lease'

             ) as T3
            INNER join
              (
                Select i_vin,i_ro,d_ro_invc,c_ro_reas
                from ${unif_serv_rec}                                    
                where c_ro_reas in ('1','3','-1')
                and date(d_ro_invc) <= date('${train_ref_date}')
              ) as T4
              on T3.vin = T4.i_vin
            where date(d_purchase) <= date(d_ro_invc)
             and date(d_ro_invc)  <= date(true_disp )
        
       group by fca_id, vin, d_purchase, i_ro, l_new_vhcl, c_sales_typ_orig, d_ro_invc
      ),
  
avg_days_between_services_lease as
(
  Select distinct fca_id,vin, round(avg(daysbetweenservices_New),2) as nAvgDaysBetween_Services_forlease_New
  from
    (
    select *,
        case when l_new_vhcl='NEW' and c_sales_typ_orig in ('L') then daysbetweenservices_new_used else null end as daysbetweenservices_New        
    from
      (
        Select *,
       
                  day(timebetweenservices_used_new_vehicle) as daysbetweenservices_new_used
               
        from
          (  
          Select *,
          -- DATE(d_ro_invc) -Lag(DATE(d_ro_invc),1) over(partition by fca_id order by fca_id,DATE(d_ro_invc)) as timebetweenservices_used_new_vehicle
           DATE(d_ro_invc) -Lag(DATE(d_ro_invc),1) over(partition by fca_id, vin order by DATE(d_ro_invc))as timebetweenservices_used_new_vehicle
           from days_since_last_service_base
        )
      )
    )
  group by vin,
  fca_id
)
Select
  distinct a.fca_id,a.vin,
  case when nAvgDaysBetween_Services_forlease_New between 0 and 30 then '0-30'
  when nAvgDaysBetween_Services_forlease_New  between 31 and 60 then '31-60'
  when nAvgDaysBetween_Services_forlease_New  between 61 and 90 then '61-90'
  when nAvgDaysBetween_Services_forlease_New between 91 and 120 then '91-120'
when nAvgDaysBetween_Services_forlease_New between 121 and 150 then '121-150'
when nAvgDaysBetween_Services_forlease_New between 151 and 180 then '151-180'
when nAvgDaysBetween_Services_forlease_New >180 then '>180'
end as avg_days_between_services_leased_new
from ${bnso_lease_train_base} as a
left join avg_days_between_services_lease as c on a.fca_id=c.fca_id and a.vin = c.vin