

# http://api.bitcoincharts.com/v1/csv/

df_10 <- subset(df, df$dim_total_bookings >= 10)

df <- read.csv("/Users/matthewpotts/Downloads/bitstampUSD.csv", header = FALSE)

con <- gzcon(url(paste("http://api.bitcoincharts.com/v1/csv/","bitstampUSD.csv.gz", sep = "")))
txt <- readLines(con)
dat <- read.csv(textConnection(txt))

df$date <- as.Date(df$V1 / 60 / 60 / 24, origin = '1970-01-01')
df$time <- as.POSIXct(as.numeric(df$V1), origin = '1970-01-01', tz = 'GMT')
df$purchase_amt <- df$V2 * df$V3
df$date <- as.character(df$date)
df$V2 <- as.numeric(df$V2)
df$V3 <- as.numeric(df$V3)

df$hour <- as.POSIXlt(df$time)$hour

head(df)
max(df$time)
max(df$my_col)
library(sqldf)

grouped <- sqldf("
                 select date
,sum(purchase_amt) / sum(V3) avg_price
from df
group by date
                 ")

getwd()
write.csv(grouped, "grouped.csv")

hourly_trades <- sqldf("
                 select date
                 ,hour
                 ,sum(purchase_amt) / sum(V3) avg_price
                 from df
                 group by date, hour
                 ")
library(zoo)
 
hourly_trades$ma20 <- c(rep(0,20-1), rollmean(hourly_trades$avg_price, 20)) 
hourly_trades$ma50 <- c(rep(0,50-1), rollmean(hourly_trades$avg_price, 50))
hourly_trades$diff <- hourly_trades$ma20 - hourly_trades$ma50

hourly_trades$year <- format(as.Date(hourly_trades$date, format="%Y-%m-%d"),"%Y")

hourly_trades$buysell <- NA
hourly_trades$buysell[hourly_trades$diff > 0] <- 1
hourly_trades$buysell[hourly_trades$diff < 0] <- 0
rownames(hourly_trades) <- NULL

hourly_trades <- subset(hourly_trades, !is.na(hourly_trades$buysell))
rownames(hourly_trades) <- NULL
hourly_trades$id <- rownames(hourly_trades)



x <- hourly_trades$buysell

groups <- c()
count <- seq(1,nrow(hourly_trades))
counter <- 0

for (i in count) {
  diff <- x[i + 1] - x[i]
  groups <- c(groups, counter)
  if (diff != 0) {
    counter <- counter + 1
  }
}

hourly_trades$groups <- groups

DT <- data.table::data.table(hourly_trades)

DT <- DT[, runstart := 1:.N, by = groups]

hourly_trades <- as.data.frame(DT)




hourly_trades_2018 <- subset(hourly_trades, hourly_trades$year >= "2019")
rownames(hourly_trades_2018) <- NULL
hourly_trades_2018$id <- rownames(hourly_trades_2018)
x <- hourly_trades_2018$buysell

groups <- c()
count <- seq(1,nrow(hourly_trades_2018))
counter <- 0

for (i in count) {
  diff <- x[i + 1] - x[i]
  groups <- c(groups, counter)
  if (diff != 0) {
    counter <- counter + 1
  }
}

hourly_trades_2018$groups <- groups
DT <- data.table::data.table(hourly_trades_2018)

DT <- DT[, runstart := 1:.N, by = groups]

hourly_trades_2018 <- as.data.frame(DT)


x <- hourly_trades$buysell
count <- seq(1,nrow(hourly_trades))
counter <- 0

for (i in count) {
  if (i == 1) {
    amount <- c(100)
    bitcoins <- c(100/hourly_trades$avg_price[i])
  } else {
    diff <- x[i + 1] - x[i]
    lag <- x[i] - x[i-1]
    if (lag > 0) {
      amount <- c(amount, bitcoins[i-1]*hourly_trades$avg_price[i])
      bitcoins <- c(bitcoins, amount[i-1]/hourly_trades$avg_price[i])
    } else if (lag < 0) {
      amount <- c(amount, bitcoins[i-1]*hourly_trades$avg_price[i])
      bitcoins <- c(bitcoins, 0)
    } else {
      amount <- c(amount, amount[i-1])
      bitcoins <- c(bitcoins, bitcoins[i-1])
    }
  }
}

hourly_trades$amount <- amount
hourly_trades$bitcoins <- bitcoins

y <- rle(hourly_trades$buysell)


DT_2018 <- subset(DT, DT$year >= "2018")

hourly_trades_2018 <- subset(hourly_trades, hourly_trades$year >= "2018")

getwd()
write.csv(hourly_trades, "hourly_trades.csv")
write.csv(hourly_trades, "hourly_trades_2011_returns.csv")

rm(df)
