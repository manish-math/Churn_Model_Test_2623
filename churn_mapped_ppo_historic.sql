create table if not exists ${historic_churn_mapped_ppo_scores}
as Select *
from ${output_table_with_ppo_churn_scores}

;
Insert into ${historic_churn_mapped_ppo_scores}
Select *
from ${output_table_with_ppo_churn_scores};