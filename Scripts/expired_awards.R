library(tidyverse)
library(blingr)


# FOLDER PATHS-------------------------

AWARDS_PATH <- "Data/active_awards/"
PHOENIX_PIPELINE_PATH <- "Data/phoenix_pipeline/"
CLOSE_OUT_TRACKER_PATH <- "Data/close_out_tracker/"

#FILTERS ------------------------------------------------
EVENT_TYPE_FILTER <- c("OBLG_UNI", "OBLG_SUBOB")
DISTRIBUTION_FILTER <- c("656-M", "656-GH-M", "656-W", "656-GH-W")


vars_new_doag <- c(
    "656-DOAG-656-22-020-DRG",
    "656-DOAG-656-22-019-EDU",
    "656-DOAG-656-22-019-IH",
    "656-DOAG-656-22-021-NUT",
    "656-DOAG-656-22-020-EG",
    "656-DOAG-656-22-021-ENV",
    "656-DOAG-656-22-021-WASH"
)


#PROCESS DATA -------------------------------------------------------------------------
#1. Expired awards - maintained locally in same google sheet as active awards-----------------------

awards_input_file <- dir(AWARDS_PATH,
                         full.name = TRUE,
                         pattern = "*.xlsx")

expired_awards_df <- map(awards_input_file, ~blingr::clean_awards(.x, "Expired Awards")) |> 
    bind_rows() 

write_csv(expired_awards_df, "Dataout/new_expired_awards.csv")


expired_award_number <- expired_awards_df |> 
    select(award_number) |> 
    distinct() |> 
    pull()



#2. Close out tracker - maintained locally by team------------------------------

close_out_tracker_input_file <- dir(CLOSE_OUT_TRACKER_PATH,
                                    full.name = TRUE,
                                    pattern = "*.xlsx")

close_out_tracker_df <- map(close_out_tracker_input_file, blingr::clean_close_out_tracker) |> 
    bind_rows() 

write_csv(close_out_tracker_df, "Dataout/new_close_out_tracker.csv")



#7. Phoenix pipeline -----------------------------------------------------------

phoenix_pipeline_input_file <- dir(PHOENIX_PIPELINE_PATH,
                                   full.name = TRUE,
                                   pattern = "*.xlsx")

phoenix_pipeline_df <- map(phoenix_pipeline_input_file, 
                           ~blingr::clean_phoenix_pipeline(.x, expired_award_number, 
                                                           EVENT_TYPE_FILTER,
                                                           DISTRIBUTION_FILTER)) |>
    bind_rows()


write_csv(phoenix_pipeline_df, "Dataout/new_phoenix_pipeline_expired.csv")



# Create dataset------------------

create_expired_awards_dataset <- function(){
    
    temp_pipeline <- phoenix_pipeline_df |> 
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
        left_join(close_out_tracker_df, by = c("award_number", "period")) 
    
    
    latest_period <- temp %>%
        summarise(max_period = max(period, na.rm = TRUE)) %>%
        pull(max_period)
    
    # Step 2: Filter the dataset for the latest period
    temp <- temp %>%
        filter(period == latest_period)
    
    return(temp)
}


expired_awards_dataset <- create_expired_awards_dataset()
write_csv(expired_awards_dataset, "Dataout/expired_awards.csv")
