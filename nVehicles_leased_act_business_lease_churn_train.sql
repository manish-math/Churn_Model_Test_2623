select fca_id,count(DISTINCT vin) as nVehicles_leased_act 
from ${business_lease_base}
where c_sales_typ_orig ='E'
group by 1