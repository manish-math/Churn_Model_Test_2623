select distinct a.fca_id,
status as population_flag,
retail_lease_flag,
single_multi_veh_flag,
days_since_last_service,
COALESCE(submitLeads_past90days, 0) submitLeads_past90days,
avg_vin_ownership_tenure,
stellantis_own_tenure,
nVehicles_purchased_lifetime,
COALESCE(avg_serv_spend,'0-75') avg_serv_spend,
avg_days_btw_repord_invcord,
COALESCE(totalspend_onservice_ft,'0-150') totalspend_onservice_ft,
COALESCE(total_services_ft,'<=1') total_services_ft,
COALESCE(ttl_act_new_vhcl_own_last_5,0) ttl_act_new_vhcl_own_last_5_yr,
disposed_ratio,
COALESCE(retail_recall_open_sseo_std,0) retail_recall_open_sseo_std,
COALESCE(recall_type_safety,0) recall_type_safety,
COALESCE(recall_type_federal_emissn,0) recall_type_federal_emissn,
COALESCE(recall_type_customer_satisfaction,0) as recall_type_customer_satisfaction,
COALESCE(nVehicles_purchased_which_were_recall,'0') nVehicles_purchased_which_were_recall,
nDaysSince_last_vehcile_purchase,
nvehicles_brand_ram_active_std,
nvehicles_brand_jeep_active_std,
nvehicles_brand_dodge_active_std,
no_of_Vins_owned_in_household,
COALESCE(num_vins_disposed_household,'<=1') num_vins_disposed_household,
mileage_recorded_from_last_service_for_active_owned_std,
income_band,
in_market_prob,
churn_30
,churn_90
from ${bnso_retail_train_base} a
left join 
day_since_last_service_bnso_retail_churn_train
b on trim(a.fca_id)=trim(b.fca_id) 
left join 
leads_submit_last90_days_bnso_retail_churn_train
c on trim(a.fca_id)=trim(c.fca_id)
left join 
avg_vin_ownership_tenure_bnso_retail_churn_train
d on trim(a.fca_id)=trim(d.fca_id) 
left join 
stellantis_tenure_bnso_retail_churn_train
e on trim(a.fca_id)=trim(e.fca_id)
left join 
nVehicles_purchased_bnso_retail_churn_train
f on trim(a.fca_id)=trim(f.fca_id)
left join 
avg_serv_spend_bnso_retail_churn_train
g on trim(a.fca_id)=trim(g.fca_id) 
left join 
serv_tat_bnso_retail_churn_train
h on trim(a.fca_id)=trim(h.fca_id) 
left join 
total_spend_total_serv_bnso_retail_churn_train
i on trim(a.fca_id)=trim(i.fca_id) 
left join
ttl_act_new_vhcl_own_last_5_yr_bnso_retail_churn_train
j on trim(a.fca_id)=trim(j.fca_id)
left join 
disp_ratio_bnso_retail_churn_train
k on trim(a.fca_id)=trim(k.fca_id)
left join 
retail_recall_open_sseo_std_bnso_retail_churn_train
l on trim(a.fca_id)=trim(l.fca_id) 
left join 
recall_type_safety_bnso_retail_churn_train
m on trim(a.fca_id)=trim(m.fca_id) 
left join 
recall_type_federal_emissn_bnso_retail_churn_train
n on trim(a.fca_id)=trim(n.fca_id) 
left join 
recall_type_customer_satisfaction_bnso_retail_churn_train
o on trim(a.fca_id)=trim(o.fca_id) 
left join 
nVehicles_purchased_which_were_recall_bnso_retail_churn_train
p on trim(a.fca_id)=trim(p.fca_id)
left join 
nDaysSince_last_vehcile_purchase_bnso_retail_churn_train
q on trim(a.fca_id)=trim(q.fca_id)
left join 
nvehicles_brand_jeep_ram_dodge_active_std_bnso_retail_churn_train
r on trim(a.fca_id)=trim(r.fca_id)
left join 
no_of_Vins_owned_in_household_bnso_retail_churn_train
s on trim(a.fca_id)=trim(s.fca_id)
left join 
in_market_prob_bnso_retail_churn_train
t on trim(a.fca_id)=trim(t.fca_id)
left join 
num_vins_disposed_household_bnso_retail_churn_train
u on trim(a.fca_id)=trim(u.fca_id)
left join 
mileage_recorded_from_last_service_for_active_owned_bnso_retail_churn_train
v on trim(a.fca_id)=trim(v.fca_id) 
left join 
single_multi_veh_flag_bnso_retail_churn_train
w on trim(a.fca_id)=trim(w.fca_id)
left join 
income_band_bnso_retail_churn_train
x on trim(a.fca_id)=trim(x.fca_id)