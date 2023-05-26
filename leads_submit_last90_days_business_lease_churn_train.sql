with leads_submitted as ( 
SELECT DISTINCT a.fca_id,1 as submitLeads_past90days
FROM ${cntct_actv} a 
inner join ${veh_interest} b 
on a.fca_id=b.fca_id
where 
c_actvy_typ <> 'Return Customer'
and UPPER(trim(c_actvy_typ_code)) <> 'CLED'
and (date(SUBSTR(t_stmp_cntct,1,10)) BETWEEN (date('${train_ref_date}') - INTERVAL '90' day) and date('${train_ref_date}'))  
and c_srce_code in ('101','250')
and a.fca_id in (SELECT DISTINCT fca_id from ${business_lease_base})
)

Select  a.fca_id,
    COALESCE(b.submitLeads_past90days, 0)  as submitLeads_past90days
from ${business_lease_base} a
inner join leads_submitted b 
on a.fca_id=b.fca_id