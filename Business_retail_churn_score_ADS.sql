select distinct a.fca_id,a.vin,
single_multi_veh_flag,
 COALESCE(b.submitLeads_past90days, 0) submitLeads_past90days,
 stellantis_own_tenure,
 avg_days_btw_repord_invcord,
 COALESCE(num_vins_disposed,0) num_vins_disposed,
 days_since_last_service,
 mileage_recorded_from_last_service_for_active_owned_std,
COALESCE(nvehicles_brand_ram_active_std,0) nvehicles_brand_ram_active_std,
COALESCE(nvehicles_brand_jeep_active_std,0) nvehicles_brand_jeep_active_std,
COALESCE(nvehicles_brand_dodge_active_std,0) nvehicles_brand_dodge_active_std,
COALESCE(nVehicles_purchased_ft,'1') nVehicles_purchased_ft,
COALESCE(nVehicles_purchased_which_were_recall,0) nVehicles_purchased_which_were_recall,
COALESCE(nservices_active_vehicle,'1') nservices_active_vehicle,
COALESCE(total_services_ft,'<=1') total_services_ft,
COALESCE(totalspend_onservice_ft,'0-150') totalspend_onservice_ft,
nAvgDaysBetween_disposals,
nAvgDaysBetween_purchases
from ${business_retail_base} a 
left join leads_submit_last90_days_businsess_retail_scoring b
on a.fca_id=b.fca_id    
left join   stellantis_tenure_business_retail_churn_scoring c
on a.fca_id=c.fca_id    
left join   serv_tat_business_retail_churn_scoring  d
on a.fca_id=d.fca_id and a.vin=d.vin    
left join   num_vins_disposed_business_retail_churn_scoring e
on a.fca_id=e.fca_id    
left join   days_since_last_service_business_retail_churn_scoring f
on a.fca_id=f.fca_id and a.vin=f.vin    
left join   mileage_recorded_from_last_service_for_active_owned_std_business_retail_churn_scoring g
on a.fca_id=g.fca_id and a.vin=g.vin    
left join   nvehicles_brand_jeep_ram_dodge_active_std_business_retail_churn_scoring h
on a.fca_id=h.fca_id    
left join   nVehicles_purchased_which_were_recall_business_retail_churn_scoring i
on a.fca_id=i.fca_id    
left join   nVehicles_purchased_ft_business_retail_churn_scoring  j
on a.fca_id=j.fca_id    
left join   nservices_active_vehicle_business_retail_churn_scoring  k
on a.fca_id=k.fca_id and a.vin=k.vin
left join   total_spend_total_serv_business_retail_churn_scoring  l
on a.fca_id=l.fca_id and a.vin=l.vin  
left join   single_multi_veh_flag_business_retail_churn_scoring m
on a.fca_id=m.fca_id    
left join   avg_days_between_disposals_business_retail_churn_scoring n
on a.fca_id=n.fca_id
left join   avg_days_between_purchases_business_retail_churn_scoring o
on a.fca_id=o.fca_id