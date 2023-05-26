select distinct a.fca_id,a.vin,
single_multi_veh_flag,
days_since_last_service,
COALESCE(submitLeads_past90days, 0) submitLeads_past90days,
-- avg_vin_ownership_tenure,
stellantis_own_tenure,
-- nVehicles_purchased_lifetime,
-- COALESCE(avg_serv_spend,'0-75') avg_serv_spend,
-- avg_days_btw_repord_invcord,
COALESCE(totalspend_onservice_ft,'0-150') totalspend_onservice_ft,
COALESCE(total_services_ft,'<=1') total_services_ft,
COALESCE(avg_serv_spend,0) avg_serv_spend,
-- COALESCE(ttl_act_new_vhcl_own_last_5,0) ttl_act_new_vhcl_own_last_5_yr,
-- disposed_ratio,
COALESCE(lease_recall_open_sseo_std,0) lease_recall_open_sseo_std,
COALESCE(recall_type_safety,0) recall_type_safety,
COALESCE(recall_type_federal_emissn,0) recall_type_federal_emissn,
COALESCE(recall_type_customer_satisfaction,0) as recall_type_customer_satisfaction,
COALESCE(nVehicles_purchased_which_were_recall,'0') nVehicles_purchased_which_were_recall,
COALESCE(nservices_leased_vehicle,'0') nservices_leased_vehicle,
nVehicles_leased_act_inact,
nVehicles_leased_act,
COALESCE(days_to_lease_end_std,0) days_to_lease_end_std,
no_of_Vins_owned_in_household,
COALESCE(num_vins_disposed_household,'<=1') num_vins_disposed_household,
COALESCE(avg_days_between_services_leased_new,'0-30') avg_days_between_services_leased_new,
income_band,
-- in_market_prob,
nDaysSince_last_vehcile_purchase
from ${business_lease_base} a
left join 
day_since_last_service_business_lease_churn_scoring
b on trim(a.fca_id)=trim(b.fca_id) and a.vin=b.vin
left join 
leads_submit_last90_days_business_lease_churn_scoring
c on trim(a.fca_id)=trim(c.fca_id)
-- left join 
-- avg_vin_ownership_tenure_business_lease_churn_scoring
-- d on trim(a.fca_id)=trim(d.fca_id) and a.vin=d.vin
left join 
stellantis_tenure_business_lease_churn_scoring 
e on trim(a.fca_id)=trim(e.fca_id)
-- left join 
-- nVehicles_purchased_business_lease_churn_scoring
-- f on trim(a.fca_id)=trim(f.fca_id)
-- left join 
-- avg_serv_spend_business_lease_churn_scoring
-- g on trim(a.fca_id)=trim(g.fca_id) and a.vin=g.vin
-- left join 
-- serv_tat_business_lease_churn_scoring
-- h on trim(a.fca_id)=trim(h.fca_id) and a.vin=h.vin
left join 
total_spend_total_serv_business_lease_churn_scoring
i on trim(a.fca_id)=trim(i.fca_id) and a.vin=i.vin
-- left join
-- ttl_act_new_vhcl_own_last_5_yr_business_lease_churn_scoring
-- j on trim(a.fca_id)=trim(j.fca_id)
-- left join 
-- disp_ratio_business_lease_churn_scoring
-- k on trim(a.fca_id)=trim(k.fca_id)
left join 
recall_open_sseo_lease_std_business_lease_churn_scoring
l on trim(a.fca_id)=trim(l.fca_id) and a.vin=l.vin
left join 
recall_type_safety_business_lease_churn_scoring
m on trim(a.fca_id)=trim(m.fca_id) and a.vin=m.vin
left join 
recall_type_federal_emissn_business_lease_churn_scoring
n on trim(a.fca_id)=trim(n.fca_id) and a.vin=n.vin
left join 
recall_type_customer_satisfaction_business_lease_churn_scoring
o on trim(a.fca_id)=trim(o.fca_id) and a.vin=o.vin
left join 
nVehicles_purchased_which_were_recall_business_lease_churn_scoring
p on trim(a.fca_id)=trim(p.fca_id)
left join 
nservices_leased_vehicle_business_lease_churn_scoring
q on trim(a.fca_id)=trim(q.fca_id) and a.vin=q.vin
left join 
nVehicles_leased_act_inact_business_lease_churn_scoring
r on trim(a.fca_id)=trim(r.fca_id)
left join 
days_to_lease_end_business_lease_churn_scoring
s on trim(a.fca_id)=trim(s.fca_id) and a.vin=s.vin
left join 
nVehicles_leased_act_business_lease_churn_scoring
t on trim(a.fca_id)=trim(t.fca_id)
left join 
num_vins_disposed_household_business_lease_churn_scoring
u on trim(a.fca_id)=trim(u.fca_id)
left join 
no_of_Vins_owned_in_household_business_lease_churn_scoring
v on trim(a.fca_id)=trim(v.fca_id)
left join 
serv_tat_business_lease_churn_scoring
w on trim(a.fca_id)=trim(w.fca_id) and trim(a.vin)=trim(w.vin)
left join 
avg_days_between_services_business_lease_churn_scoring
x on trim(a.fca_id)=trim(x.fca_id) and trim(a.vin)=trim(x.vin)
left join 
single_multi_veh_flag_business_lease_churn_scoring
y on trim(a.fca_id)=trim(y.fca_id)
left join 
income_band_business_lease_churn_scoring
z on trim(a.fca_id)=trim(z.fca_id)
left join 
-- in_market_prob_business_lease_churn_scoring
-- aa on trim(a.fca_id)=trim(aa.fca_id)
-- left join 
nDaysSince_last_vehcile_purchase_business_lease_churn_scoring
ab on trim(a.fca_id)=trim(ab.fca_id)



