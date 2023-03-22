# Dane pochodza ze strony: https://travel.stackexchange.com/

Comments <- read.csv("Comments.csv.gz")
Users <- read.csv("Users.csv.gz")
Posts <- read.csv("Comments.csv.gz")

library(sqldf)
library(dplyr)


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

all_equal(
  sql_1(Users),
  base_1(Users),
  dplyr_1(Users)
)

compare::compare(
  sql_1(Users),
  base_1(Users),
  dplyr(Users)
)
