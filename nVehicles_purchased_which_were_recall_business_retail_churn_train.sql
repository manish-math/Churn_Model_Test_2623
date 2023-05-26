with recalled_veh as (
SELECT trim(i_vin,' ') as i_vin,trim(d_recal_lnch, ' ') as d_recal_lnch, x_vhcl_recal_desc as recall_desc
,a.c_vhcl_recal as recal_code,l_vin
FROM ${stg_recal_sseo_vin} as a
INNER JOIN ${stg_recal_code} as b
on a.c_vhcl_recal=b.c_vhcl_recal
) 
,

Vehicles_purchased as 
(
Select distinct(i_vin_first_9||i_vin_last_8) as i_vin, fca_id
from ${owner}
where cast(d_purchase as date) between cast('${train_ref_date}' as date) - interval '10' year and cast('${train_ref_date}' as date)
and trim(c_cntry_srce) like '1'
and fca_id in (select fca_id from ${business_retail_base})
)
,

vehicles_pur_which_recalled as 
(
Select fca_id,a.i_vin,recal_code,recall_desc
from recalled_veh a 
inner join Vehicles_purchased b
on a.i_vin = b.i_vin
)

select distinct a.fca_id,
-- case when nveh_pur_recalled = 1 then '1'
-- when nveh_pur_recalled = 2 then '2'
-- when nveh_pur_recalled = 3 then '3'
-- else '>3' end 
nveh_pur_recalled as nVehicles_purchased_which_were_recall 
from 
(
select distinct fca_id,count(distinct i_vin) as nveh_pur_recalled  from vehicles_pur_which_recalled
group by 1
) 
a inner join 
${business_retail_base} b 
on a.fca_id = b.fca_id