create table if not exists ${historic_churn_model_scores}
as Select *
from ${churn_scores_single_table}

;
Insert into ${historic_churn_model_scores}
Select *
from ${churn_scores_single_table} ;