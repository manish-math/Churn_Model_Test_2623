With days_since_last_service_base as
(
  select fca_id, vin, d_purchase, i_ro, l_new_vhcl, c_sales_typ_orig, d_ro_invc
  from
    (
      select  trim(fca_id, ' ') as fca_id,vin, trim(d_purchase, ' ') as d_purchase, trim(i_ro, ' ') as i_ro, trim(l_new_vhcl, ' ') as l_new_vhcl,
              trim(c_sales_typ_orig, ' ') as c_sales_typ_orig, trim(d_ro_invc, ' ') as d_ro_invc
      from
        (
        select *
        from
          (
            Select  distinct fca_id, vin, d_purchase, d_vhcl_dspsl,d_leas_end, i_ro,d_ro_invc,l_new_vhcl, c_sales_typ_orig,
                    case when LEAST(d_vhcl_dspsl,d_leas_end) > cast('${train_ref_date}' as varchar) then cast('${train_ref_date}' as varchar) else  LEAST(d_vhcl_dspsl,d_leas_end) end as true_disposal 
            from
            (
              Select distinct T1.fca_id, T1.vin, d_purchase, l_new_vhcl, c_sales_typ_orig, COALESCE(d_vhcl_dspsl,cast(CURRENT_DATE as VARCHAR)) as d_vhcl_dspsl, COALESCE(d_leas_end,cast(CURRENT_DATE as VARCHAR)) as d_leas_end 
              from
                (
                  select fca_id,vin
                  from ${buso_retail_base}
                ) as T1
                left join
                (
                  Select fca_id,(i_vin_first_9||i_vin_last_8) as vin,d_purchase, d_vhcl_dspsl, d_leas_end, l_new_vhcl, c_sales_typ_orig,
                  ROW_NUMBER()over(partition by fca_id, i_vin_first_9||i_vin_last_8, d_purchase order by t_stmp_upd desc) as row_rank
                  from ${owner}
                  where (date(d_purchase) BETWEEN (date('${train_ref_date}') - INTERVAL '10' year) and date('${train_ref_date}'))
                  and fca_id in (select distinct fca_id from ${buso_retail_base})
                  and i_vin_first_9||i_vin_last_8 in (select distinct vin from ${buso_retail_base})
                  and c_cntry_srce = '1'
                  and c_sales_typ_orig in ('1','L')
                  and upper(trim(c_mkt)) = 'U'
                  and upper(trim(c_co_car_catgy)) = 'FALSE'
                  and upper(trim(l_new_vhcl)) in ('USED')
                ) as T2
                on T1.fca_id = T2.fca_id
                and T1.vin=T2.vin 
                where row_rank = 1
              ) as T3
            left join
              (
                Select i_vin,i_ro,d_ro_invc,c_ro_reas                                   
                from ${unif_serv_rec}                                
                where c_ro_reas in ('1','3','-1')
                and i_vin in (select distinct vin from ${buso_retail_base})
                and date(d_ro_invc) between (date('${train_ref_date}') - INTERVAL '10' year) and date('${train_ref_date}')
              ) as T4
              on T3.vin = T4.i_vin
            )
          )
        where date(d_ro_invc) BETWEEN date(d_purchase) and date(true_disposal)
      )
  group by fca_id, vin, d_purchase, i_ro, l_new_vhcl, c_sales_typ_orig, d_ro_invc
)



 
select distinct fca_id,date_diff('day',date(last_serv_date),date('${train_ref_date}')) as days_since_last_service from (
select fca_id,max(d_ro_invc) last_serv_date from days_since_last_service_base
group by 1
) 