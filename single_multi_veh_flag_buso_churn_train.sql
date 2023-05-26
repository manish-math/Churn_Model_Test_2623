select distinct fca_id,case when cnt=1 then 'Single_vehicle_owners' else 'Multi_vehicle_owners' end as single_multi_veh_flag
from (  
select fca_id,count(DISTINCT vin) cnt 
from 
${buso_retail_base}
group by 1 
)
