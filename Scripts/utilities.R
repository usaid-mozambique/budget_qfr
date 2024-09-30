create_expired_awards_dataset <- function(){
  
  temp_pipeline <- phoenix_pipeline_df |> 
    filter(award_number %in% expired_award_number) |> 
    select(award_number, period, undisbursed_amt, bilateral_obl_number) |>
    group_by(award_number, period, bilateral_obl_number) |>
    summarise(undisbursed_amt = sum(undisbursed_amt, na.rm = TRUE), .groups = "drop") |> 
    mutate(doag_new_old = case_when(
      str_detect(bilateral_obl_number, paste(vars_new_doag, collapse = "|")) ~ "new",
      TRUE ~ "old"),
      unliquidated_obligation_new_doag = case_when(doag_new_old == "new" ~ undisbursed_amt,
                                                   TRUE ~ 0),
      unliquidated_obligation_old_doag = case_when(doag_new_old == "old" ~ undisbursed_amt,
                                                   TRUE ~ 0)
    ) |> 
    select(-bilateral_obl_number) |> 
    group_by(award_number, period) |> 
    summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop")
  
  
  temp <- expired_awards_df |>
    left_join(temp_pipeline, by = c("award_number", "period")) |> 
    left_join(close_out_tracker_df, by = c("award_number", "period")) |> 
    mutate(to_be_deobligated = replace_na(to_be_deobligated, 0))
  
  
  latest_period <- temp %>%
    summarise(max_period = max(period, na.rm = TRUE)) %>%
    pull(max_period)
  
  # Step 2: Filter the dataset for the latest period
  temp <- temp %>%
    filter(period == latest_period)
  
  return(temp)
}






#this one 
create_transaction_dataset <- function() {
  #latest active awards with accrual amount
  active_awards_accrual_latest <- pipeline_dataset |> 
    filter(period == max(period)) |> 
    group_by(award_number, period, program_area) |> 
    summarise(last_qtr_accrual_amt = sum(last_qtr_accrual_amt, na.rm = TRUE), .groups = "drop")
  
  
  
  active_awards_one_row_transaction <- active_awards_df |> 
    select(award_number, activity_name) |> 
    distinct() |> #needed as there are multiple lines due to period
    left_join(phoenix_transaction_df, by = "award_number") |> 
    select(award_number, activity_name, transaction_disbursement,
           transaction_obligation, transaction_amt, 
           period, program_area) |> 
    left_join(active_awards_accrual_latest, by = c("award_number", "period", "program_area")) 
  
}


#TODO - check for duplicate rows
create_pipeline_dataset<- function(){
  
  active_awards_one_row <- active_awards_df |> 
    left_join(phoenix_pipeline_df, by = c("award_number", "period")) |> 
    
    #remove bilateral_obl_number to avoid duplicates in subobligation summary
    select(-bilateral_obl_number) |> 
    group_by(across(where(~ !is.numeric(.)))) |> 
    summarise(
      total_estimated_cost = max(total_estimated_cost),  # Max value for total estimated cost
      across(where(is.numeric) & !contains("total_estimated_cost"), ~ sum(.x, na.rm = TRUE)),.groups = "drop"  # Sum for other numeric values
    ) |> 
    
    
    
    left_join(subobligation_summary_df, by = c("award_number", "period", "program_area")) |> 
    left_join(phoenix_transaction_df, by = c("award_number", "period", "program_area")) #|> 
  
  
  
  return(active_awards_one_row)
  
}



