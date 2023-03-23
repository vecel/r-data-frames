# Dane pochodza ze strony: https://travel.stackexchange.com/

Comments <- read.csv("Comments.csv.gz")
Users <- read.csv("Users.csv.gz")
Posts <- read.csv("Comments.csv.gz")

library(sqldf)
library(dplyr)
library(data.table)
setDT(Comments)
setDT(Users)
setDT(Posts)


sql_1 <- function(Users) {
  sqldf("
    SELECT Location, SUM(UpVotes) as TotalUpVotes
    FROM Users
    WHERE Location != ''
    GROUP BY Location
    ORDER BY TotalUpVotes DESC
    LIMIT 10")
}

base_1 <- function(Users) {
  # Grupuje dane po Location i licze sume UpVotes w kazdej grupie
  df <- aggregate(
    Users$UpVotes,
    by=list(Users$Location),
    FUN=sum
    )
  
  # Naprawiam nazwy kolumn
  colnames(df) <- c("Location", "TotalUpVotes")
  
  # Usuwam wiersz, w ktorym lokalizacja jest pusta
  df <- df[ df$Location != "", ]
  # Alternatywnie  df <- subset(df, Location != "")
  
  # Sortuje malejaco po TotalUpVotes i wybieram pierwsze dziesiec wynikow
  df <- df[order(df$TotalUpVotes, decreasing=TRUE), ]
  df <- df[1:10, ]
  
  # Naprawiam nazwy wierszy dla estetyki
  rownames(df) <- NULL
  return(df)
}

dplyr_1 <- function(Users) {
  Users %>% 
    group_by(Location) %>%
    summarise(TotalUpVotes=sum(UpVotes)) %>%
    arrange(desc(TotalUpVotes)) %>%
    filter(Location != "") %>%
    slice_head(n=10)
  # w ramce Users
  # grupuje po Location
  # podsumowuje kazda grupe suma z UpVotes
  # sortuje nierosnaco
  # usuwan pusta Location
  # wybieram 10 pierwszych rekordow
  
  # Konwersja tibble -> data.frame (po zapisaniu wynikowej ramki jako df)
  # as.data.frame(df)
}

table_1 <- function(Users) {
  Users[ 
    Location != "", .(TotalUpVotes = sum( UpVotes )), by=Location
      ][order(-TotalUpVotes)
        ][1:10]
  # Z Users
  # wybieram wiersze, gdzie Location nie jest puste, wyliczam sume UpVotes grupujac po Location
  # wynik sortuje niemalejaco po -TotalUpVotes (czyli nierosnaco po TotalUpVotes)
  # i wybieram 10 pierwszych wierszy poprzedniego wyniku
}

all_equal( sql_1(Users), base_1(Users) )
all_equal( sql_1(Users), dplyr_1(Users) )
all_equal( sql_1(Users), table_1(Users) )

# microbenchmark::microbenchmark(
#   sqldf = sql_1(Users),
#   base = base_1(Users),
#   dplyr = dplyr_1(Users),
#   table = table_1(Users)
# )


