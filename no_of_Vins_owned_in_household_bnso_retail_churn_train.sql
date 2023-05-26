with Vins_owned_within_household as (
select i_hshld_id_curr,count(distinct i_vin_first_9||i_vin_last_8) as Vins_owned_within_household
from ${owner}
where c_cntry_srce = '1'
and (date(d_purchase) BETWEEN (date('${train_ref_date}') - INTERVAL '10' year) and date('${train_ref_date}'))
and LEAST(COALESCE(d_vhcl_dspsl,cast(CURRENT_DATE as VARCHAR)),
COALESCE(d_leas_end,cast(CURRENT_DATE as VARCHAR))) >='${train_ref_date}'
and c_sales_typ_orig in ('1','L')
and upper(trim(c_mkt)) = 'U'
and upper(trim(c_co_car_catgy)) = 'FALSE'
and upper(trim(l_new_vhcl)) in ('NEW','USED')
group by 1
)
,

fca_id_lvl as (
select distinct fca_id , Vins_owned_within_household
from ${bnso_retail_train_base} a
inner join Vins_owned_within_household b
on a.i_hshld_id_curr = b.i_hshld_id_curr
)



select fca_id,
case
when Vins_owned_within_household = 1 then '1'
when Vins_owned_within_household = 2 then '2'
when Vins_owned_within_household = 3 then '3'
when Vins_owned_within_household = 4 then '4'
when Vins_owned_within_household >= 5 then '>=5'
end as no_of_Vins_owned_in_household
from (
select fca_id,max(Vins_owned_within_household) Vins_owned_within_household
from fca_id_lvl
group by 1
)
group by 1,2