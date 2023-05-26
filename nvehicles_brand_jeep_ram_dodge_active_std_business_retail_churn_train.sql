SELECT fca_id,sum(nvehicles_brand_ram_active_std) as nvehicles_brand_ram_active_std,
sum(nvehicles_brand_jeep_active_std) as nvehicles_brand_jeep_active_std,
sum(nvehicles_brand_dodge_active_std) as nvehicles_brand_dodge_active_std
from (
SELECT distinct a.fca_id,vin,
case when trim(upper(a.x_vhcl_make)) = 'RAM' then 1 else 0 end as nvehicles_brand_ram_active_std,
case when trim(upper(a.x_vhcl_make)) = 'JEEP' then 1 else 0 end as nvehicles_brand_jeep_active_std,
case when trim(upper(a.x_vhcl_make)) = 'DODGE' then 1 else 0 end as nvehicles_brand_dodge_active_std
from 
${business_retail_base} a 
inner join ${owner} b 
on vin=i_vin_first_9||i_vin_last_8
)
group by 1 