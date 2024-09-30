#this one 
create_transaction_dataset <- function() {
  #latest active awards with accrual amount
  active_awards_accrual_latest <- pipeline_dataset |> 
    filter(period == max(period)) |> 
    group_by(award_number, period, program_area) |> 
    summarise(last_qtr_accrual_amt = sum(last_qtr_accrual_amt, na.rm = TRUE), .groups = "drop")

    
  
  active_awards_one_row_transaction <- active_awards_df |> 
    select(award_number, activity_name) |> 
    left_join(phoenix_transaction_df, by = "award_number") |> 
    select(award_number, activity_name, transaction_disbursement,
           transaction_obligation, transaction_amt, 
           period, program_area) |> 
    left_join(active_awards_accrual_latest, by = c("award_number", "period", "program_area")) 
  
  return(active_awards_one_row_transaction)

}


create_pipeline_dataset<- function(){
  
  
  temp_pipeline <- phoenix_pipeline_df |> 
    select(-bilateral_obl_number) |>  #not needed for active awards
    group_by(across(where(~ !is.numeric(.)))) |>
    summarise(across(where(is.numeric) & !contains("total_estimated_cost"), ~ sum(.x, na.rm = TRUE)),
              .groups = "drop"  # Sum for other numeric values
    ) |>
  
  active_awards_one_row <- active_awards_df |> 
    left_join(temp_pipeline, by = c("award_number", "period")) |> 
    left_join(subobligation_summary_df, by = c("award_number", "period", "program_area")) |> 
    left_join(phoenix_transaction_df, by = c("award_number", "period", "program_area")) 

 return(active_awards_one_row)
  
}




