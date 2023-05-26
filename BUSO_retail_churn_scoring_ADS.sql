select distinct a.fca_id,
cast(date('${train_ref_date}') - INTERVAL '10' year as VARCHAR) as start_date,
cast(date('${train_ref_date}') as VARCHAR) as end_date,
cast(date('${train_ref_date}') as VARCHAR) as scoring_date,
status as population_flag,
retail_lease_flag,
single_multi_veh_flag,
days_since_last_service,
no_of_hands_exchanged,
c_fuel_typ_E,
c_fuel_typ_D,
c_fuel_typ_N,
c_fuel_typ_G,
c_fuel_typ_Y,
c_fuel_typ_F,
c_fuel_typ_P,
COALESCE(submitLeads_past90days, 0) submitLeads_past90days,
avg_vin_ownership_tenure,
stellantis_own_tenure,
nVehicles_purchased_lifetime,
avg_days_btw_repord_invcord,
COALESCE(totalspend_onservice_ft,'0-150') totalspend_onservice_ft,
COALESCE(total_services_ft,'<=1') total_services_ft,
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
COALESCE(num_new_vins_pur_last_5_years,0) as num_new_vins_pur_last_5_years
from ${buso_retail_base} a
left join 
day_since_last_service_buso_retail_churn_scoring
b on trim(a.fca_id)=trim(b.fca_id)
left join 
leads_submit_last90_days_buso_retail_churn_scoring
c on trim(a.fca_id)=trim(c.fca_id)
left join 
avg_vin_ownership_tenure_buso_retail_churn_scoring
d on trim(a.fca_id)=trim(d.fca_id)
left join 
stellantis_tenure_buso_retail_churn_scoring
e on trim(a.fca_id)=trim(e.fca_id)
left join 
nVehicles_purchased_buso_retail_churn_scoring
f on trim(a.fca_id)=trim(f.fca_id)
left join 
serv_tat_buso_retail_churn_scoring
h on trim(a.fca_id)=trim(h.fca_id) 
left join 
total_spend_total_serv_buso_retail_churn_scoring
i on trim(a.fca_id)=trim(i.fca_id) 
left join 
disp_ratio_buso_retail_churn_scoring
k on trim(a.fca_id)=trim(k.fca_id)
left join 
retail_recall_open_sseo_std_buso_retail_churn_scoring
l on trim(a.fca_id)=trim(l.fca_id) 
left join 
recall_type_safety_buso_retail_churn_scoring
m on trim(a.fca_id)=trim(m.fca_id) 
left join 
recall_type_federal_emissn_buso_retail_churn_scoring
n on trim(a.fca_id)=trim(n.fca_id) 
left join 
recall_type_customer_satisfaction_buso_retail_churn_scoring
o on trim(a.fca_id)=trim(o.fca_id) 
left join 
nVehicles_purchased_which_were_recall_buso_retail_churn_scoring
p on trim(a.fca_id)=trim(p.fca_id)
left join 
nDaysSince_last_vehcile_purchase_buso_retail_churn_scoring
q on trim(a.fca_id)=trim(q.fca_id)
left join 
nvehicles_brand_jeep_ram_dodge_active_std_buso_retail_churn_scoring
r on trim(a.fca_id)=trim(r.fca_id)
left join 
no_of_Vins_owned_in_household_buso_retail_churn_scoring
s on trim(a.fca_id)=trim(s.fca_id)
left join 
num_vins_disposed_household_buso_retail_churn_scoring
u on trim(a.fca_id)=trim(u.fca_id)
left join 
mileage_recorded_from_last_service_for_active_owned_buso_retail_churn_scoring
v on trim(a.fca_id)=trim(v.fca_id) 
left join 
single_multi_veh_flag_buso_churn_scoring
w on trim(a.fca_id)=trim(w.fca_id) 
left join 
fuel_type_buso_retail_churn_scoring
x on trim(a.fca_id)=trim(x.fca_id)
left join 
no_of_hands_exchanged_buso_retail_churn_scoring
y on trim(a.fca_id)=trim(y.fca_id)
left join 
num_new_vins_pur_last_5_years_buso_retail_churn_scoring
z on trim(a.fca_id)=trim(z.fca_id)