create table if not exists churn_buso_retail_score_ads_hist
as Select *
from churn_buso_retail_scoring_ADS
;

Insert into churn_buso_retail_score_ads_hist
Select *
from churn_buso_retail_scoring_ADS;


