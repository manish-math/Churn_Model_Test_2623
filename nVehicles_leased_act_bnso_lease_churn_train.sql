select fca_id,count(DISTINCT vin) as nVehicles_leased_act 
from ${bnso_lease_train_base}
where c_sales_typ_orig ='L'
group by 1