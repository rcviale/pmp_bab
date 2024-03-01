# Define a vector of ETF symbols to download data for, including a benchmark
etfs <- c("PBUS",
          "EWC",
          "EWO",
          "EWK",
          "EDEN",
          "EFNL",
          "EWQ",
          "EWG",
          "EIRL",
          "EIS",
          "EWI",
          "EWN",
          "NORW",
          "PGAL",
          "EWP",
          "EWD",
          "EWL",
          "EWU",
          "EWZ",
          "ECH",
          "ICOL",
          "EWW",
          "EPU",
          "EGPT",
          "GREK",
          "KWT",
          "EPOL",
          "QAT",
          "KSA",
          "EZA",
          "TUR",
          "UAE",
          "MCHI",
          "INDA",
          "EIDO",
          "EWY",
          "EWM",
          "EPHE",
          "EWT",
          "^990100-USD-STRD")

# Download historical data for the ETFs from Yahoo Finance
df <- tidyquant::tq_get(etfs,
                        from = "1970-01-01", # Start date for data download
                        to   = Sys.Date())   # End date (current date)

# Save the downloaded data as an RDS file
df |> 
  readr::write_rds("Data/etfs.rds")

# Read the saved ETF data, select necessary columns, and calculate monthly returns
readr::read_rds("Data/etfs.rds") |> 
  dplyr::select(symbol, 
                date, 
                adjusted) |> 
  dplyr::group_by(symbol) |> 
  dplyr::mutate(lret = tidyquant::RETURN(adjusted), # Calculate log returns
                date = lubridate::ceiling_date(date, unit = "month") - 1) |>  # Convert to end-of-month dates
  dplyr::group_by(symbol, 
                  date) |> 
  dplyr::mutate(lret = sum(lret, na.rm = T)) |>  # Aggregate monthly returns, remove NAs
  dplyr::slice_tail(n = 1) |>  # Keep only the last record for each month
  dplyr::ungroup() |> 
  readr::write_rds("Data/monthly_etfs.rds")

# Extract the benchmark returns from the monthly data
readr::read_rds("Data/monthly_etfs.rds") |> 
  dplyr::filter(symbol == "^990100-USD-STRD") |>  # Filter for benchmark symbol
  dplyr::select(date, 
                lret) |> 
  dplyr::rename(benchmark = lret) |>  # Rename lret to benchmark for clarity
  readr::write_rds("Data/benchmark_lrets.rds")

# Join ETF data with benchmark returns and calculate betas using a rolling window
readr::read_rds("Data/monthly_etfs.rds") |> 
  dplyr::filter(symbol != "^990100-USD-STRD") |>  # Exclude benchmark from ETF data
  dplyr::left_join(y  = readr::read_rds("Data/benchmark_lrets.rds"),
                   by = "date") |>  # Join with benchmark returns
  dplyr::group_by(symbol) |> 
  dplyr::mutate(beta = slider::slide2_dbl(.x        = lret, # ETF returns
                                          .y        = benchmark, # Benchmark returns
                                          .f        = ~lm(.x ~ .y)$coefficients[2], # Linear regression to estimate beta
                                          .before   = 47L, # Window size for rolling calculation
                                          .complete = T)) |>  # Require complete window
  dplyr::ungroup() -> temp

# Calculate weights for the Betting Against Beta strategy and compute strategy returns
temp |> 
  dplyr::arrange(date, symbol) |> # Sort data by date and symbol
  tidyr::drop_na(beta) |>  # Remove rows with NA beta values
  dplyr::group_by(date) |> # Group data by date to perform operations within each month
  dplyr::mutate(rank     = rank(beta, na.last = "keep"), # Rank ETFs based on beta values
                avg_rank = mean(rank), # Calculate the average rank for later use in weighting
                constant = 2 / sum(abs(rank - avg_rank)), # Calculate a constant for weighting based on rank distance from average
                wgt      = constant * -(rank - avg_rank), # Compute weights inversely proportional to beta rank distance from average
                norm_wgt = ifelse(wgt > 0, # Normalize weights so that each leg has beta +-1
                                  beta * wgt / sum(beta[wgt > 0] * wgt[wgt > 0]), # Normalize weights for positive bets
                                  beta * -wgt / sum(beta[wgt < 0] * wgt[wgt < 0]))) |> # Normalize weights for negative bets
  dplyr::group_by(symbol) |> # Group by symbol to compute weighted returns for each ETF
  dplyr::mutate(r_bab = lret * (slider::slide_dbl(.x      = norm_wgt, # Apply sliding window to calculate rolling sum of normalized weights
                                                  .f      = ~sum(.x), # Function to sum weights within window
                                                  .before = 1L) - norm_wgt)) |> # Subtract current weight to simulate rebalancing
  dplyr::group_by(date) |> # Group by date to aggregate results across all ETFs
  dplyr::select(date, r_bab) |> # Select relevant columns for output
  dplyr::transmute(r_bab = sum(r_bab)) |> # Sum the r_bab values to get total return for each date
  dplyr::distinct() |> # Ensure uniqueness in date-r_bab pairs
  dplyr::ungroup() |> # Remove grouping for further operations
  dplyr::mutate(cum_bab = cumsum(r_bab)) # Calculate cumulative returns of the BAB strategy

                                  