SELECT fca_id,avg(avg_vin_ownership_tenure) as avg_vin_ownership_tenure from (
SELECT distinct fca_id,vin,
DATE_DIFF('day',date(d_purchase),date('${train_ref_date}'))/365.0 as avg_vin_ownership_tenure
from ${buso_retail_base}
where true_disp >= '${train_ref_date}'
and c_sales_typ_orig in ('1','L')
)
group by 1