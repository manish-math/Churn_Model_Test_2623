select fca_id,max(mileage_recorded_from_last_service_for_active_owned_std) as mileage_recorded_from_last_service_for_active_owned_std from (
select DISTINCT base_table.fca_id,base_table.vin, mileage_recorded_from_last_service_for_active_owned as mileage_recorded_from_last_service_for_active_owned_std
from (
  (
    select fca_id,vin 
    from ${buso_retail_base}
  ) as base_table
  left join
  (
    select fca_id, vin,i_vhcl_milg as mileage_recorded_from_last_service_for_active_owned
    from
    (
      select fca_id, vin, cast(i_vhcl_milg as int) as i_vhcl_milg 
      from
      (
        Select  distinct fca_id, vin, d_purchase, true_disp, i_vhcl_milg, d_ro_invc,
                ROW_NUMBER () OVER (PARTITION BY fca_id,vin ORDER BY d_ro_invc DESC) as service_rank
        from
        (
          select  fca_id, vin, d_purchase, true_disp, d_ro_invc, i_vhcl_milg
          from
          (
            Select distinct T1.fca_id, T1.vin, d_purchase, true_disp, d_ro_invc, i_vhcl_milg 
            from
            (
              (
                select fca_id,vin,d_purchase,true_disp
                from ${buso_retail_base}
              ) as T1
            left join
            (
              Select fca_id,i_vin, d_ro_invc, i_vhcl_milg                              
              from ${unif_serv_rec}                                     
              where c_ro_reas in ('1','3','-1')
              and i_vin in (SELECT DISTINCT vin from ${buso_retail_base})
              and date(d_ro_invc) between (date('${train_ref_date}') - INTERVAL '10' year) and date('${train_ref_date}')
              and cast(i_vhcl_milg as int) between 0 and 538746
              group by 1,2,3,4
            ) as T4
            on T1.fca_id = T4.fca_id and T1.vin = T4.i_vin
          )
          group by T1.fca_id, T1.vin, d_purchase, true_disp, d_ro_invc, i_vhcl_milg
        )
        where date(d_ro_invc) BETWEEN date(d_purchase) and date(true_disp)
      )
        )
      where service_rank = 1
    )
    group by fca_id,vin,i_vhcl_milg
  ) as output_table
  on base_table.fca_id = output_table.fca_id and base_table.vin = output_table.vin
)
)
group by 1 