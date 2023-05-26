create table if not exists churn_business_lease_score_ads_hist
as Select *
from churn_business_lease_scoring_ADS
;

Insert into churn_business_lease_score_ads_hist
Select *
from churn_business_lease_scoring_ADS;