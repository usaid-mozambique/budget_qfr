required_packages <- c("dplyr","purrr", "tidyr","stringr", "janitor", "lubridate", "glamr",
                       "readxl", "readr")

missing_packages <- required_packages[!(required_packages %in% installed.packages()
                                        [,"Package"])]

if(length(missing_packages) >0){
    install.packages(missing_packages, repos = c("https://usaid-oha-si.r-universe.dev",
                                                 "https://cloud.r-project.org"))
}


library(glamr)

# OTHER SETUP  - only run one-time ------------------------------------------

folder_setup() 
folder_setup(folder_list = list("Data/subobligation_summary", 
                                "Data/active_awards", 
                                "Data/phoenix_transactions", 
                                "Data/phoenix_pipeline",
                                "Data/close_out_tracker"))