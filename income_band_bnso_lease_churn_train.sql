with demo_master as
(
    select *
    from
    (
        select fca_id ,a_hshld_inc_band,
        row_number() over(partition by fca_id  order by t_stmp_upd desc) as rank
        from ${enr_cons_demo}
        where fca_id in (select distinct fca_id from ${bnso_lease_train_base})
    )
    where rank = 1
),

demo_variables as
(
select distinct fca_id ,
case
when trim(a_hshld_inc_band,' ') between  '1' and '2' then  'income_band_15K-25K'
when trim(a_hshld_inc_band,' ') between  '3' and '4' then  'income_band_25K_35K'
when trim(a_hshld_inc_band,' ') between  '5' and '6' then  'income_band_35K_45K'
when trim(a_hshld_inc_band,' ') between  '7' and '8' then  'income_band_45K_55K'
when trim(a_hshld_inc_band,' ') between  '9' and 'C' then  'income_band_55K-75K'
when trim(a_hshld_inc_band,' ') between  'D' and 'G' then  'income_band_75K_95K'
when trim(a_hshld_inc_band,' ') between  'H' and 'K' then  'income_band_95K_115K'
when trim(a_hshld_inc_band,' ') between  'L' and 'O' then  'income_band_115K_135K'
when trim(a_hshld_inc_band,' ') between  'P' and 'S' then  'income_band_135K_160K'
when trim(a_hshld_inc_band,' ') between  'T' and 'Z' then  'income_band_above_160K'
else 'Not Captured'
end income_band
from
demo_master
where fca_id in (select distinct fca_id from ${bnso_lease_train_base})
)


select distinct fca_id , income_band from demo_variables