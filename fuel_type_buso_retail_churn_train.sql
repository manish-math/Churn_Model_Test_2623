with ownership as 
(
select distinct fca_id,
                     vin,
                    d_purchase,
                     c_sales_typ_orig,
                      retail_lease_flag       
                  from ${buso_retail_base}
),

service as 
(
select * from 
(
select i_vin,d_ro_open ,c_fuel_typ,
row_number () over ( partition by i_vin order by d_ro_open desc ) as p_rank
from ${unif_serv_rec}
where c_ro_reas in ('1','-1','-3')
and c_fuel_typ is not null
and (cast(d_ro_open as date) between cast('${train_ref_date}' as date) - interval '10' year and cast('${train_ref_date}' as date))
and (cast(d_ro_open as date) between date(d_purchase) and date(COALESCE(d_vhcl_dspsl,cast(CURRENT_DATE as VARCHAR))))
and trim(i_vin) in ( select distinct vin from ownership)
)
where p_rank=1
)


select distinct a.fca_id , 
COALESCE(max(c_fuel_typ_E),0) as c_fuel_typ_E,
COALESCE(max(c_fuel_typ_D),0) as c_fuel_typ_D,
COALESCE(max(c_fuel_typ_N),0) as c_fuel_typ_N,
COALESCE(max(c_fuel_typ_G),0) as c_fuel_typ_G,
COALESCE(max(c_fuel_typ_Y),0) as c_fuel_typ_Y,
COALESCE(max(c_fuel_typ_F),0) as c_fuel_typ_F,
COALESCE(max(c_fuel_typ_P),0) as c_fuel_typ_P
from ${buso_retail_base} as a  left join 
(
select fca_id,
case when trim(c_fuel_typ)='E' then 1 else 0 end as c_fuel_typ_E,
case when trim(c_fuel_typ)='D' then 1 else 0 end as c_fuel_typ_D,
case when trim(c_fuel_typ)='N' then 1 else 0 end as c_fuel_typ_N,
case when trim(c_fuel_typ)='G' then 1 else 0 end as c_fuel_typ_G,
case when trim(c_fuel_typ)='Y' then 1 else 0 end as c_fuel_typ_Y,
case when trim(c_fuel_typ)='F' then 1 else 0 end as c_fuel_typ_F,
case when trim(c_fuel_typ)='P' then 1 else 0 end as c_fuel_typ_P
from ownership inner join service 
on trim(ownership.vin) = trim(service.i_vin) 
)  as b 
on a.fca_id=b.fca_id
group by 1