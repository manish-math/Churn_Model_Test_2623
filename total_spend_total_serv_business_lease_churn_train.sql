with service_base as (
select 
  i_vin,
  d_ro_invc, i_seq,
  case when a_labr < 0 then 0 else a_labr end as a_labr,
  case when a_misc < 0 then 0 else a_misc end as a_misc,
  case when a_extndd_sell_prc < 0 then 0 else a_extndd_sell_prc end as a_extndd_sell_prc
from 
(
    select 
      a.i_vin,
      a.d_ro_invc, a.i_seq,
      COALESCE(b.c_labr_typ,'Others') as c_labr_typ,
      round(sum(COALESCE(a_labr,0)),2) as a_labr,
      round(sum(COALESCE(a_misc,0)),2) as a_misc,
      round(sum(COALESCE(a_extndd_sell_prc,0)),2) as a_extndd_sell_prc
    from 
      (
      select 
        i_vin,
        cast(i_seq as INTEGER) as i_seq,
        cast(d_ro_invc as date) as d_ro_invc
      from ${stg_serv_rec}
      where i_vin in (select distinct vin from ${business_lease_base}) 
      -- c_ro_reas in ('1','3','-1')
      and 
      cast(d_ro_invc as date) between cast('${train_ref_date}' as date) - interval '10' year and cast('${train_ref_date}' as date)
      group by 1,2,3
      ) a
      left join 
      (
          select 
          i_vin,
          'C&W' as c_labr_typ,
          cast(i_seq as INTEGER) as i_seq,
          sum(cast(a_labr as double)) as a_labr,
          sum(cast(a_misc as double)) as a_misc 
        from ${stg_labr_info}
        where i_vin  in
              (select i_vin  
              from ${stg_serv_rec}
              where i_vin in (select distinct vin from ${business_lease_base}) 
              -- c_ro_reas in ('1','3','-1')
              and
               cast(d_ro_invc as date) between cast('${train_ref_date}' as date) - interval '10' year and cast('${train_ref_date}' as date)
              group by i_vin)
          and c_labr_typ in ('C','W') -- for out of pocket expenditure
          and (cast(a_labr as varchar) not like '%99999.9%')
          and (cast(a_misc as varchar) not like '%99999.9%')
        group by 1,2,3
      ) b
      on a.i_vin = b.i_vin 
        and a.i_seq = b.i_seq
        left join 
        (select 
            i_vin,
            cast(i_seq as INTEGER) as i_seq,
            sum(cast(a_extndd_sell_prc as double)) as a_extndd_sell_prc
          from ${stg_lab_part}
          where i_vin  in 
                (select i_vin  
                from ${stg_serv_rec}
                where i_vin in (select distinct vin from ${business_lease_base}) 
                -- c_ro_reas in ('1','3','-1')
                and
                cast(d_ro_invc as date) between cast('${train_ref_date}' as date) - interval '10' year and cast('${train_ref_date}' as date)
                group by i_vin)
            and c_labr_typ in ('C','W')
            and (a_extndd_sell_prc not like '%99999.9%')
          group by 1,2
          ) c
        on a.i_vin = c.i_vin 
          and a.i_seq = c.i_seq
        group by 1,2,3,4
)
where c_labr_typ = 'C&W'
),


ownership_base as 
(
  Select 
  distinct fca_id,
  (i_vin_first_9||i_vin_last_8) as vin,
  date(d_purchase) as d_purchase,
  d_vhcl_dspsl,
  d_leas_end,
  c_sales_typ_orig,      
  c_vhcl_bdy_mod,
  i_mod_yr,
  case when 
  LEAST(date(d_vhcl_dspsl),date(d_leas_end)) > cast('${train_ref_date}' as date) 
  then cast('${train_ref_date}' as date) 
  else LEAST(date(d_vhcl_dspsl),date(d_leas_end)) end as true_disposal,
  ROW_NUMBER()over(partition by fca_id, i_vin_first_9||i_vin_last_8, d_purchase order by t_stmp_upd desc) as row_rank
from ${owner}
where cast(d_purchase as date) between cast('${train_ref_date}' as date) - interval '10' year and cast('${train_ref_date}' as date)
and trim(c_cntry_srce) like '1'
  and fca_id in (select fca_id from ${business_lease_base})
  and i_vin_first_9||i_vin_last_8 in (select i_vin from service_base group by 1)
),
ownership_service_base as 
(   
select 
  a.fca_id,
  vin,
  i_seq,
  a.d_purchase,
  a.d_vhcl_dspsl,
  a.d_leas_end,
  COALESCE(a_labr,0) as a_labr,
  COALESCE(a_misc,0) as a_misc,
  cast(SUBSTRING(cast(b.d_ro_invc as varchar),1,10) as date) as d_ro_invc,
  a.true_disposal,
  COALESCE(a_extndd_sell_prc,0) as a_extndd_sell_prc
from ownership_base a 
inner join service_base b on a.vin = b.i_vin
  and cast(SUBSTRING(cast(b.d_ro_invc as varchar),1,10) as date) between a.d_purchase and COALESCE(a.true_disposal,cast('${train_ref_date}' as date))
where row_rank = 1
group by 1,2,3,4,5,6,7,8,9,10,11
),

totalspend_onservice as
(
select
  fca_id, vin,
  sum(a_labr) + sum(a_misc) + sum(a_extndd_sell_prc) as totalspend_onservice_ft
from ownership_service_base
group by 1,2
),

total_services as
(
select
  fca_id, vin,
  count(distinct(i_seq)) as nservice_ft
from ownership_service_base
group by 1,2
),

total_spend as (
select
  distinct a.fca_id,a.vin,
  COALESCE(round(totalspend_onservice_ft,2),0) as totalspend_onservice_ft,
  COALESCE(round(nservice_ft,2),0) as total_services_ft
  from ${business_lease_base} a
inner join totalspend_onservice b on a.fca_id = b.fca_id and a.vin=b.vin inner join total_services c
on a.fca_id = c.fca_id and a.vin=c.vin
)

select distinct
fca_id, vin,(1.0*totalspend_onservice_ft/total_services_ft) as avg_serv_spend,
case when totalspend_onservice_ft >= 0 and totalspend_onservice_ft <=150 then '0-150'
when totalspend_onservice_ft>150 and totalspend_onservice_ft <=350 then '150-350'
when totalspend_onservice_ft>350 and totalspend_onservice_ft <=700 then '350-700'
when totalspend_onservice_ft>700 and totalspend_onservice_ft <= 1000 then '700-1000'
when totalspend_onservice_ft >1000 then '>1000' end as totalspend_onservice_ft,
case when total_services_ft >= 0 and total_services_ft <=1 then '<=1'
when total_services_ft>1 and total_services_ft <=3 then '1 to 3'
when total_services_ft>3 and total_services_ft <=5 then '3 to 5'
when total_services_ft>5 and total_services_ft <= 10 then '5 to 10'
when total_services_ft >10 then '>10' end as total_services_ft
from total_spend