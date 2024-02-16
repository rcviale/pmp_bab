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

df <- tidyquant::tq_get(etfs,
                        from = "1970-01-01",
                        to   = Sys.Date())

df |> 
  readr::write_rds("Data/etfs.rds")

readr::read_rds("Data/etfs.rds") |> 
  dplyr::select(symbol, 
                date, 
                adjusted) |> 
  dplyr::group_by(symbol) |> 
  dplyr::mutate(lret = tidyquant::RETURN(adjusted),
                date = lubridate::ceiling_date(date, unit = "month") - 1) |> 
  dplyr::group_by(symbol, 
                  date) |> 
  dplyr::mutate(lret = sum(lret, na.rm = T)) |>  #FIXME: dump non-full months before this
  dplyr::slice_tail(n = 1) |> 
  dplyr::ungroup() |> 
  readr::write_rds("Data/monthly_etfs.rds")

readr::read_rds("Data/monthly_etfs.rds") |> 
  dplyr::filter(symbol == "^990100-USD-STRD") |> 
  dplyr::select(date, 
                lret) |> 
  dplyr::rename(benchmark = lret) |> 
  readr::write_rds("Data/benchmark_lrets.rds")

readr::read_rds("Data/monthly_etfs.rds") |> 
  dplyr::filter(symbol != "^990100-USD-STRD") |> 
  dplyr::left_join(y  = readr::read_rds("Data/benchmark_lrets.rds"),
                   by = "date") |> 
  dplyr::group_by(symbol) |> 
  dplyr::mutate(beta = slider::slide2_dbl(.x        = lret,
                                          .y        = benchmark,
                                          .f        = ~lm(.x ~ .y)$coefficients[2],
                                          .before   = 11L,
                                          .complete = T)) |> 
  dplyr::group_by(date) -> temp # |> 

temp |> 
  dplyr::arrange(date, symbol) |> 
  dplyr::mutate(rank     = rank(beta, na.last = "keep"),
                avg_rank = mean(rank),
                N        = max(rank),
                constant = 2 / sum(abs(rank - avg_rank)),
                wgt      = constant * (rank - avg_rank),
                norm_wgt = ifelse(wgt > 0, 
                                  beta * wgt / sum(beta[wgt > 0] * wgt[wgt > 0]),
                                  -abs(beta * wgt) / sum(abs(beta[wgt < 0] * wgt[wgt < 0]))),
                b_port   = ifelse(wgt > 0,
                                  sum(beta[wgt > 0] * norm_wgt[wgt > 0]),
                                  sum(beta[wgt < 0] * norm_wgt[wgt < 0]))) -> temp

temp |> 
  dplyr::mutate(r_port   = ifelse(wgt < 0,
                                  sum(lret[norm_wgt < 0] * abs(norm_wgt[norm_wgt < 0])),
                                  sum(-lret[norm_wgt > 0] * norm_wgt[norm_wgt > 0]))) |>
  dplyr::select(date, b_port, r_port) |> 
  dplyr::distinct() |> 
  na.omit() |> 
  dplyr::transmute(r_bab = sum(r_port / b_port)) |> 
  dplyr::distinct() |> 
  dplyr::ungroup() |> 
  dplyr::mutate(cum_bab = cumsum(r_bab))
