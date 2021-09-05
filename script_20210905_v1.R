library("arrow")
library("rlist")
library("data.table")
library("tidyverse")
library("Metrics")
library("caret")
library("lubridate")



# 0. path ------------------------------------------------------------------

inputpath <- "./input/optiver-realized-volatility-prediction"
stock_name <- list.files("./input/optiver-realized-volatility-prediction/book_train.parquet/")

path_book_train <- list.files("./input/optiver-realized-volatility-prediction/book_train.parquet",
                              pattern = "[.]parquet$",
                              recursive = T,
                              full.names = T)
path_book_test <- list.files("./input/optiver-realized-volatility-prediction/book_test.parquet",
                             pattern = "[.]parquet$",
                             recursive = T,
                             full.names = T)
path_trade_train <- list.files("./input/optiver-realized-volatility-prediction/trade_train.parquet",
                              pattern = "[.]parquet$",
                              recursive = T,
                              full.names = T)
path_trade_test <- list.files("./input/optiver-realized-volatility-prediction/trade_test.parquet",
                             pattern = "[.]parquet$",
                             recursive = T,
                             full.names = T)



# 1. load ------------------------------------------------------------------

#1 train/test/sub
train <- fread(file = paste0(inputpath, "/train.csv"))
test <- fread(file = paste0(inputpath, "/test.csv"))
submission <- fread(file = paste0(inputpath, "/sample_submission.csv"))

stock_id <- sort(unique(train$stock_id))



# #2 book
# book_train <- map(as.list(1:length(path_book_train)),
#                   ~setDT(read_parquet(file = path_book_train[.x])))
# names(book_train) <- stock_id
# # rbindlist(book_train, idcol = "stock_id")
# 
# book_test <- setDT(read_parquet(file = path_book_test))
# book_test$stock_id <- 0
# 
# 
# 
# #3 trade
# trade_train <- map(as.list(1:length(path_book_train)), 
#                    ~setDT(read_parquet(file = path_trade_train[.x])))
# names(trade_train) <- stock_id
# # rbindlist(trade_train, idcol = "stock_id")
# 
# 
# trade_test <- setDT(read_parquet(file = path_trade_test))
# trade_test$stock_id <- 0





# 2. explore ---------------------------------------------------------------

trade_train[[1]] %>% 
  group_by(time_id) %>% 
  summarize(sdp = sd(price))







for (i in 1:length(path_book_train)){
  stock <- read_parquet(path_book_train[i])
  stock <- stock %>% mutate(wap= (bid_price1 * ask_size1 +ask_price1 * bid_size1) / (
    bid_size1 + ask_size1)) %>% 
    mutate(log_return = log(wap/lag(wap))) %>% 
    mutate(wap2= (bid_price2 * ask_size2 +ask_price2 * bid_size2) / (
      bid_size2 + ask_size2)) %>% 
    mutate(log_return2 = log(wap2/lag(wap2))) %>% 
    mutate(spread = ask_price1 - bid_price1) %>% 
    mutate(vol = ask_size1 + bid_size1) %>% 
    mutate(imb = ask_size1 - bid_size1)  %>% 
    group_by(time_id) %>%
    mutate(volat=sqrt(sum(diff(log((bid_price1 * ask_size1 + ask_price1 * bid_size1) 
                                   / (ask_size1 + bid_size1)))**2))) %>% 
    mutate(volat2=sqrt(sum(diff(log((bid_price2 * ask_size2 + ask_price2 * bid_size2) 
                                    / (ask_size2 + bid_size2)))**2))) %>%
    summarize(sd_bp1 = mean(bid_price1),
              min_bp1 = min(bid_price1),
              sd_ap1 = sd(ask_price1),
              min_ap1 = sd(ask_price1),
              log_ret = mean(log_return),
              log_ret2 = mean(log_return2),
              volat = mean(volat),
              volat2 = mean(volat2),
              price_spread = mean(spread),
              volume = mean(vol),
              imbalance = mean(imb)
    )
  
  ## add stock_id to rows
  stock <- stock %>% mutate(stock_id = as.numeric(str_sub(stock_name[i],10)))
  vol_df <- vol_df %>% add_row(stock)
}

























