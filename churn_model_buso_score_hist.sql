create table if not exists churn_model_buso_score_hist
as Select *
from churn_buso_retail_score_current_date;

Insert into churn_model_buso_score_hist
Select *
from churn_buso_retail_score_current_date;
