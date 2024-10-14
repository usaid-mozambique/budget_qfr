library(tidyverse)
library(blingr)

options(scipen = 999)  #remove scientific notation

# FOLDER PATHS-------------------------
SUBOBLIGATION_SUMMARY_PATH <-  "Data/subobligation_summary/"
AWARDS_PATH <- "Data/active_awards/"
PHOENIX_TRANSACTION_PATH <- "Data/phoenix_transactions/"
PHOENIX_PIPELINE_PATH <- "Data/phoenix_pipeline/"

#FILTERS ------------------------------------------------
EVENT_TYPE_FILTER <- c("OBLG_UNI", "OBLG_SUBOB")
DISTRIBUTION_FILTER <- c("656-M", "656-GH-M", "656-W", "656-GH-W")
REMOVE_AWARDS <- c("MEL")


#READ AND CLEAN ALL DATA ---------------------------------------------
#1. Active Awards - maintained locally-----------------------

#TODO remove if start date is after the quarter  
awards_input_file <- dir(AWARDS_PATH,
                         full.name = TRUE,
                         pattern = "*.xlsx")

active_awards_df <- map(awards_input_file, ~blingr::clean_awards(.x, "Active Awards")) |> 
    bind_rows() |> 
    filter(!str_detect(activity_name, paste(REMOVE_AWARDS, collapse = "|"))) 

#3. List of all awards to pull from Phoenix Data ----------------------------
#all active award IDs
all_award_number <- active_awards_df |> 
    select(award_number) |> 
    distinct() |> 
    pull()

#4. Subobligation Summary - maintained locally by team---------------------

sub_obligation_input_file <- dir(SUBOBLIGATION_SUMMARY_PATH, 
                                 full.name = TRUE, 
                                 pattern = "*.xlsx")


subobligation_summary_df <- map(sub_obligation_input_file, blingr::clean_subobligation_summary) |> 
    bind_rows() # |> 
#    mutate(active_awards_fiscal_year = as.numeric(str_extract(period, "(?<=FY)[0-9]{2}")) + 2000)


#6. Phoenix Transaction--------------

phoenix_transaction_input_file <- dir(PHOENIX_TRANSACTION_PATH,
                                      full.name = TRUE,
                                      pattern = "*.xlsx")


phoenix_transaction_df <- map(phoenix_transaction_input_file, 
                              ~blingr::clean_phoenix_transaction(.x, all_award_number,
                                                                 DISTRIBUTION_FILTER)) |> 
    bind_rows() |> 
    select(-c(program_area_name, transaction_date_month, fiscal_year, fiscal_quarter, transaction_amt, transaction_obligation)) |> 
    group_by(award_number, period, program_area) |>
    #summarise all numeric 
    dplyr::summarise(dplyr::across(dplyr::where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop")


#7. Phoenix pipeline -------------------------

phoenix_pipeline_input_file <- dir(PHOENIX_PIPELINE_PATH,
                                   full.name = TRUE,
                                   pattern = "*.xlsx")

phoenix_pipeline_df <- map(phoenix_pipeline_input_file, 
                           ~blingr::clean_phoenix_pipeline(.x, all_award_number, 
                                                           EVENT_TYPE_FILTER,
                                                           DISTRIBUTION_FILTER)) |>
    bind_rows() |> 
    select(-c(bilateral_obl_number, pipeline_amt, undisbursed_amt)) |> 
    group_by(across(where(~ !is.numeric(.)))) |> 
    summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop")



# CREATE DATASETS-----------------------------
#1. Pipeline ----------------------


pipeline_dataset <- active_awards_df |> 
    left_join(phoenix_pipeline_df, by = c("award_number", "period")) |> 
    left_join(subobligation_summary_df, by = c("award_number", "period", "program_area")) |> 
    left_join(phoenix_transaction_df, by = c("award_number", "period", "program_area"))

write_csv(pipeline_dataset,"Dataout/pipeline.csv")

#2. Transaction -------------------

transaction_dataset <- active_awards_df |> 
    select(award_number, activity_name) |> 
    distinct() |> #needed as there are multiple lines due to period
    left_join(phoenix_transaction_df, by = "award_number") |> 
    left_join(phoenix_pipeline_df, by = c("award_number", "period", "program_area")) |> 
    select(-c(program_area_name,document_amt, disbursement_amt))
    
write_csv(transaction_dataset, "Dataout/transaction.csv")




