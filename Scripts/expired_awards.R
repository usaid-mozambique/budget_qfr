library(tidyverse)
library(blingr)

options(scipen = 999)  #remove scientific notation

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


#1. Expired awards - maintained locally in same google sheet as active awards-----------------------

awards_input_file <- dir(AWARDS_PATH, full.name = TRUE, pattern = "*.xlsx")

expired_awards_df <- map(awards_input_file,
                         ~ blingr::clean_awards(.x, "Expired Awards")) |>
    bind_rows() |> 
    filter(period == max(period, na.rm = TRUE)) |> 
    select(-period)


all_award_number <- expired_awards_df |>
    select(award_number) |>
    distinct() |>
    pull()

#2. Phoenix pipeline -------------------------

phoenix_pipeline_input_file <- dir(PHOENIX_PIPELINE_PATH,
                                   full.name = TRUE,
                                   pattern = "*.xlsx")

phoenix_pipeline_df <- map(
    phoenix_pipeline_input_file,
    ~ blingr::clean_phoenix_pipeline(.x, all_award_number, EVENT_TYPE_FILTER, DISTRIBUTION_FILTER)
) |>
    bind_rows() 
    

#Keep latest period and create calculations for new and old DOAG undisbursed amt
phoenix_pipeline_expired_df <-  phoenix_pipeline_df |> 
    select(award_number, period, bilateral_obl_number, undisbursed_amt) |>
    group_by(award_number, period, bilateral_obl_number) |>
    summarise(undisbursed_amt = sum(undisbursed_amt, na.rm = TRUE),
              .groups = "drop") |>
    mutate(
        doag_new_old = case_when(str_detect(
            bilateral_obl_number, paste(vars_new_doag, collapse = "|")
        ) ~ "new", TRUE ~ "old"),
        unliquidated_obligation_new_doag = case_when(doag_new_old == "new" ~ undisbursed_amt, TRUE ~ 0),
        unliquidated_obligation_old_doag = case_when(doag_new_old == "old" ~ undisbursed_amt, TRUE ~ 0)
    ) |>
    select(-bilateral_obl_number) |>
    group_by(award_number, period) |>
    summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop") |>
    filter(period == max(period, na.rm = TRUE))


#3. Close out tracker - maintained locally by team------

close_out_tracker_input_file <- dir(CLOSE_OUT_TRACKER_PATH,
                                    full.name = TRUE,
                                    pattern = "*.xlsx")

close_out_tracker_df <- map(close_out_tracker_input_file,
                            blingr::clean_close_out_tracker) |>
    bind_rows() |> 
    filter(period == max(period, na.rm = TRUE)) |> 
    select(-period)


# CREATE DATASET---------------------------------------------------
# Expired Awards (one rows per award, per quarter)-------------
expired_awards_dataset <-  phoenix_pipeline_expired_df |>
    left_join(expired_awards_df, by = "award_number") |>
    left_join(close_out_tracker_df, by = c("award_number"))  |>
    mutate(to_be_deobligated = replace_na(to_be_deobligated, 0)) |> 
    select(-c(period, u_s_org_local, start_date, total_estimated_cost, ))



write_csv(expired_awards_dataset, "Dataout/expired_awards.csv")
