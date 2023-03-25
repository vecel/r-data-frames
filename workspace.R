# Dane pochodza ze strony: https://travel.stackexchange.com/

Comments <- read.csv("Comments.csv.gz")
Users <- read.csv("Users.csv.gz")
Posts <- read.csv("Posts.csv.gz")

library(sqldf)
library(dplyr)
library(data.table)
setDT(Comments)
setDT(Users)
setDT(Posts)

{
  
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

}

{

sql_2 <- function(Posts) {
  sqldf("SELECT STRFTIME('%Y', CreationDate) AS Year, STRFTIME('%m', CreationDate) AS Month,
    COUNT(*) AS PostsNumber, MAX(Score) AS MaxScore
    FROM Posts
    WHERE PostTypeId IN (1, 2)
    GROUP BY Year, Month
    HAVING PostsNumber > 1000")
}

base_2 <- function(Posts) {
  # Dodaje kolumny Year i Month
  df <- cbind(Posts, Year = sprintf("%04d", year(Posts$CreationDate)), Month = sprintf("%02d", month(Posts$CreationDate)))
  
  # Usuwam wiersze, w ktorych PostTypeId jest rozne od 1 i 2
  df <- df[ df$PostTypeId %in% c(1, 2), ]
  
  # Grupuje po Year i Month, zliczam liczbe rekordow i max ze Score w kazdej grupie
  df <- aggregate(
    cbind(df$Score),
    by=list(df$Year, df$Month),
    FUN = function(x) c(length(x), max(x))
  )
  
  # Wynikiem powyzszego aggregate jest ramka danych, w ktorej podsumowane wyniki
  # sa macierzami a nie kolumnami. Tutaj "wyplaszczam" wynik do zwyklej ramki damych
  df <- do.call(data.frame, df)
  
  # Naprawiam nazwy kolumn
  colnames(df) <- c("Year", "Month", "PostsNumber", "MaxScore")
  
  # Usuwam wiersze, w ktorych PostsNumber jest <= 1000
  df <- df[ df$PostsNumber > 1000, ]
  
  # dla estetyki sortuje i naprawiam numeracje wierszy
  df <- df[ order(df$Year, df$Month), ]
  rownames(df) <- NULL
  df
}

all_equal(sql_2(Posts), base_2(Posts))

}
