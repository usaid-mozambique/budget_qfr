library(tidyverse)
library(blingr)


# FOLDER PATHS-------------------------
SUBOBLIGATION_SUMMARY_PATH <-  "Data/subobligation_summary/"
AWARDS_PATH <- "Data/active_awards/"
PHOENIX_TRANSACTION_PATH <- "Data/phoenix_transactions/"
PHOENIX_PIPELINE_PATH <- "Data/phoenix_pipeline/"
#CLOSE_OUT_TRACKER_PATH <- "Data/close_out_tracker/"

#FILTERS ------------------------------------------------
EVENT_TYPE_FILTER <- c("OBLG_UNI", "OBLG_SUBOB")
DISTRIBUTION_FILTER <- c("656-M", "656-GH-M", "656-W", "656-GH-W")
REMOVE_AWARDS <- c("MEL")

#vars_new_doag <- c(
#    "656-DOAG-656-22-020-DRG",
#    "656-DOAG-656-22-019-EDU",
#    "656-DOAG-656-22-019-IH",
#    "656-DOAG-656-22-021-NUT",
#    "656-DOAG-656-22-020-EG",
#    "656-DOAG-656-22-021-ENV",
#    "656-DOAG-656-22-021-WASH"
#)
#TODO remove if start date is after the quarter  

#READ ALL FUNCTIONS ------------------------------------'

source("Scripts/utilities.R")


#READ AND CLEAN ALL DATA ---------------------------------------------
#1. Active Awards - maintained locally-----------------------


awards_input_file <- dir(AWARDS_PATH,
                         full.name = TRUE,
                         pattern = "*.xlsx")

active_awards_df <- map(awards_input_file, ~blingr::clean_awards(.x, "Active Awards")) |> 
    bind_rows() |> 
    filter(!str_detect(activity_name, paste(REMOVE_AWARDS, collapse = "|"))) 


write_csv(active_awards_df, "Dataout/new_active_awards.csv")




#3. List of all awards to pull from Phoenix Data ----------------------------
#all active award IDs
active_award_number <- active_awards_df |> 
    select(award_number) |> 
    distinct() |> 
    pull()





#4. Subobligation Summary - maintained locally by team---------------------

sub_obligation_input_file <- dir(SUBOBLIGATION_SUMMARY_PATH, 
                                 full.name = TRUE, 
                                 pattern = "*.xlsx")


subobligation_summary_df <- map(sub_obligation_input_file, blingr::clean_subobligation_summary) |> 
    bind_rows() |> 
mutate(active_awards_fiscal_year = as.numeric(str_extract(period, "(?<=FY)[0-9]{2}")) + 2000)

write_csv(subobligation_summary_df, "Dataout/new_subobligation_summary.csv")



#6. Phoenix Transaction--------------

phoenix_transaction_input_file <- dir(PHOENIX_TRANSACTION_PATH,
                                      full.name = TRUE,
                                      pattern = "*.xlsx")


phoenix_transaction_df <- map(phoenix_transaction_input_file, 
                              ~blingr::clean_phoenix_transaction(.x, active_award_number,
                                                          DISTRIBUTION_FILTER)) |> 
    bind_rows() |> 
    select(-program_area_name, -transaction_date_month) |> 
    group_by(award_number, fiscal_year, fiscal_quarter, period, program_area) |>
    #summarise all numeric 
    dplyr::summarise(dplyr::across(dplyr::where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop")


write_csv(phoenix_transaction_df, "Dataout/new_phoenix_transaction.csv")

phoenix_transaction_cumulative_df <- phoenix_transaction_df |> 
    blingr::create_phoenix_transaction_cumulative()



#7. Phoenix pipeline -------------------------

phoenix_pipeline_input_file <- dir(PHOENIX_PIPELINE_PATH,
                                   full.name = TRUE,
                                   pattern = "*.xlsx")

phoenix_pipeline_df <- map(phoenix_pipeline_input_file, 
                           ~blingr::clean_phoenix_pipeline(.x, active_award_number, 
                                                           EVENT_TYPE_FILTER,
                                                           DISTRIBUTION_FILTER)) |>
    bind_rows()


write_csv(phoenix_pipeline_df, "Dataout/new_phoenix_pipeline.csv")

# CREATE DATASETS-----------------------------
#1. Pipeline ----------------------
pipeline_dataset <- create_pipeline_dataset() |> 
    left_join(phoenix_transaction_cumulative_df, by = c("award_number", 
                                                        "active_awards_fiscal_year" = "fiscal_year",
                                                        "program_area")) 

write_csv(pipeline_dataset,"Dataout/pipeline.csv")

#2. Transaction -------------------
transaction_dataset <- create_transaction_dataset()
write_csv(transaction_dataset, "Dataout/transaction.csv")
