with mixed_base as
(
select fca_id,count(distinct(l_new_vhcl))
from churn_buso_retail_scoring_base_part1 
group by 1
having count(distinct(l_new_vhcl)) > 1
),

final_disp as (
SELECT distinct fca_id,i_hshld_id_curr,status,vin,d_purchase,true_disp,l_new_vhcl,c_sales_typ_orig,retail_lease_flag,x_vhcl_make, x_nmplt
from (
    select distinct *,DENSE_RANK()over(PARTITION by vin ORDER by d_purchase desc) as p_rank
    from churn_buso_retail_scoring_base_part1 
)
where p_rank=1
),

base as (
select *,case when fca_id in (select fca_id from mixed_base) then 'MIXED'
else l_new_vhcl end as type_vehicle from final_disp
),

mixed_hh_base as (
SELECT i_hshld_id_curr,count(distinct l_new_vhcl) cnt  from 
base 
group by 1 
)

SELECT a.*, 
case when cnt>1 then 'MIXED' else l_new_vhcl end as household_type 
from base a 
inner join mixed_hh_base b 
on a.i_hshld_id_curr=b.i_hshld_id_curr