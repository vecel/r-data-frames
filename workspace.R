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
  # w ramce Users
  # grupuje po Location
  # podsumowuje kazda grupe suma z UpVotes
  # sortuje nierosnaco
  # usuwan pusta Location
  # wybieram 10 pierwszych rekordow
  Users %>% 
    group_by(Location) %>%
    summarise(TotalUpVotes=sum(UpVotes)) %>%
    arrange(desc(TotalUpVotes)) %>%
    filter(Location != "") %>%
    slice_head(n=10)
  
  
  # Konwersja tibble -> data.frame (po zapisaniu wynikowej ramki jako df)
  # as.data.frame(df)
}

table_1 <- function(Users) {
  # Z Users
  # wybieram wiersze, gdzie Location nie jest puste, wyliczam sume UpVotes grupujac po Location
  # wynik sortuje niemalejaco po -TotalUpVotes (czyli nierosnaco po TotalUpVotes)
  # i wybieram 10 pierwszych wierszy poprzedniego wyniku
  Users[ 
    Location != "", .(TotalUpVotes = sum( UpVotes )), by=Location
      ][order(-TotalUpVotes)
        ][1:10]
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

dplyr_2 <- function(Posts) {
  # Z Posts
  # rozwazam tylko te wiersze, w ktorych PostTypeId = 1 lub 2
  # dodaje kolumny Year i Month
  # po ktorych grupuje
  # i zliczam liczbe rekordow oraz maksimum ze Score
  # wybieram tylko te wiersze, gdzie liczba rekordow > 1000
  Posts %>%
    filter(PostTypeId %in% c(1, 2)) %>%
    mutate(Year=sprintf("%04d", year(CreationDate)), Month=sprintf("%02d", month(CreationDate))) %>%
    group_by(Year, Month) %>%
    summarise(PostsNumber=n(), MaxScore=max(Score), .groups="drop") %>%
    filter(PostsNumber > 1000)
}

table_2 <- function(Posts) {
  # Z Posts
  # Rozwazam wiersze, w ktorych PostTypeId = 1 lub 2, wyznaczam Year, Month. W grupach Year, Month zliczam liczbe rekordow (PostsNumber) i maksimum z Score
  # Wybieram te wiersze, w ktorych PostsNumber > 1000
  Posts[PostTypeId %in% c(1, 2), 
        .(Year=sprintf("%04d", year(CreationDate)), Month=sprintf("%02d", month(CreationDate)), Score)][
          , .(PostsNumber=.N, MaxScore=max(Score)), by=.(Year, Month)
        ][PostsNumber > 1000]
}

# Przez konwersje z daty sqldf dziala najszybciej
all_equal(sql_2(Posts), base_2(Posts))
all_equal(sql_2(Posts), dplyr_2(Posts))
all_equal(sql_2(Posts), table_2(Posts))

# microbenchmark::microbenchmark(
#   sqldf = sql_2(Posts),
#   base = base_2(Posts),
#   dplyr = dplyr_2(Posts),
#   table = table_2(Posts)
# )

}

{

sql_3 <- function(Posts, Users) {
  sqldf("SELECT Id, DisplayName, TotalViews
    FROM (
      SELECT OwnerUserId, SUM(ViewCount) as TotalViews
      FROM Posts
      WHERE PostTypeId = 1
      GROUP BY OwnerUserId
    ) AS Questions
    JOIN Users
    ON Users.Id = Questions.OwnerUserId
    ORDER BY TotalViews DESC
    LIMIT 10")
}

base_3 <- function(Posts, Users) {
  # Tworze ramke Questions do zlaczenia z Users
  # Wybieram z Posts rekordy, ktorych wartosc PostTypeId = 1 (biore tylko wartosci kolumn OwnerUserId, ViewCount)
  Questions <- Posts[ Posts$PostTypeId==1, c("OwnerUserId", "ViewCount")]
  
  # Grupuje po OwnerUserId i zliczam sume ViewCount w kazdej grupie 
  Questions <- aggregate(
    Questions$ViewCount,
    by=list(Questions$OwnerUserId),
    sum
  )
  
  # Naprawiam nazwy kolumn
  colnames(Questions) <- c("OwnerUserId", "TotalViews")
  
  # Lacze ramki Users i Questions po wartosci Id
  # Lacze te i tylko te wiersze, ktore maja Users.Id = Questions.OwnerUserId, wiersze bez pary nie sa brane pod uwage
  df <- merge(
    Users,
    Questions,
    by.x="Id",
    by.y="OwnerUserId"
  )
  
  # Wybieram interesujace mnie kolumny
  df <- df[ , c("Id", "DisplayName", "TotalViews")]
  
  # Sortuje nierosnaco po TotalViews
  df <- df[ order(df$TotalViews, decreasing = TRUE), ]
  
  # Dla estetyki
  rownames(df) <- NULL
  
  # Wybieram pierwsze 10 rekordow
  df <- df[1:10,]
  return(df)
}

dplyr_3 <- function(Posts, Users) {
  # Tworze ramke Questions
  # Z Posts
  # wybieram wiersze z PostTypeId = 1
  # grupuje po OwnerUserId
  # rozwazam dwie kolumny: OwnerUserId oraz ViewCount
  # zliczam sume ViewCount w kazdej grupie
  Questions <- Posts %>% 
    filter(PostTypeId == 1) %>%
    group_by(OwnerUserId) %>%
    select(OwnerUserId, ViewCount) %>%
    summarise(TotalViews = sum(ViewCount))
  
  # Lacze ramki Users i Questions po wartosci Id i OwnerUserId
  # wybieram interesujace mnie kolumny
  # sortuje nierosnaco po TotalViews
  # wybieram 10 pierwszych rekordow
  inner_join(
    Users,
    Questions,
    by=join_by(Id == OwnerUserId)
  ) %>%
    select(Id, DisplayName, TotalViews) %>%
    arrange(desc(TotalViews)) %>%
    slice_head(n=10)
}

table_3 <- function(Posts, Users) {
  # Tworze ramke Questions (patrz funkcja table_1)
  Questions <- Posts[PostTypeId==1, .(TotalViews = sum( ViewCount )), by=OwnerUserId]

  # Lacze ramki Users i Questions po kolumnach Id i OwnerUserId
  # sortuje nierosnaco po TotalViews i wybieram kolumny Id, DisplayName, TotalViews
  # wybieram 10 pierwszych rekordow
  merge(Users, Questions, by.x="Id", by.y="OwnerUserId")[
    order(TotalViews, decreasing=TRUE), .(Id, DisplayName, TotalViews)
  ][1:10]
}

sql_3_result <- sql_3(Posts, Users)
base_3_result <- base_3(Posts, Users)
dplyr_3_result <- dplyr_3(Posts, Users)
table_3_result <- table_3(Posts, Users)

all_equal(sql_3_result, base_3_result)
all_equal(sql_3_result, dplyr_3_result)
all_equal(sql_3_result, table_3_result)

# Wywolanie ponizszej funkcji trwa (pewnie) okolo 40 min
# microbenchmark::microbenchmark(
#   sqldf = sql_3(Posts),
#   base = base_3(Posts),
#   dplyr = dplyr_3(Posts),
#   table = table_3(Posts),
#   times = 10
# )

}

{
  
sql_4 <- function(Posts, Users) {
  sqldf("SELECT DisplayName, QuestionsNumber, AnswersNumber, Location, Reputation, UpVotes, DownVotes
    FROM (
      SELECT *
      FROM (
        SELECT COUNT(*) as AnswersNumber, OwnerUserId
        FROM Posts
        WHERE PostTypeId = 2
        GROUP BY OwnerUserId
        ) AS Answers
      JOIN
        (
          SELECT COUNT(*) as QuestionsNumber, OwnerUserId
          FROM Posts
          WHERE PostTypeId = 1
          GROUP BY OwnerUserId
        ) AS Questions
      ON Answers.OwnerUserId = Questions.OwnerUserId
      WHERE AnswersNumber > QuestionsNumber
      ORDER BY AnswersNumber DESC
      LIMIT 5
      ) AS PostsCounts
    JOIN Users
    ON PostsCounts.OwnerUserId = Users.Id")
}

base_4 <- function(Posts, Users) {
  Answers <- Posts[ Posts$PostTypeId == 2, ]
  Answers <- aggregate(
    Answers$OwnerUserId,
    by=list(Answers$OwnerUserId),
    length
  )
  Answers <- Answers[, c(2,1)]
  colnames(Answers) <- c("AnswersNumber", "OwnerUserId")
  
  # Uwaga: base pomija NA, sqldf zlicza je jako jedna grupe
  
  Questions <- Posts[ Posts$PostTypeId == 1, ]
  Questions <- aggregate(
    Questions$OwnerUserId,
    by=list(Questions$OwnerUserId),
    length
  )
  Questions <- Questions[, c(2,1)]
  colnames(Questions) <- c("QuestionsNumber", "OwnerUserId")
  
  PostsCount <- merge(
    Answers,
    Questions,
    by="OwnerUserId"
  )
  PostsCount <- PostsCount[ PostsCount$AnswersNumber > PostsCount$QuestionsNumber, ]
  PostsCount <- PostsCount[ order(PostsCount$AnswersNumber, decreasing = TRUE), ]
  PostsCount <- PostsCount[1:5, ]
  
  df <- merge(
    Users,
    PostsCount,
    by.x="Id",
    by.y="OwnerUserId"
  )
  df <- df[ , c("DisplayName", "QuestionsNumber", "AnswersNumber", "Location", "Reputation", "UpVotes", "DownVotes")]
  return(df)
}

dplyr_4 <- function(Posts, Users) {
  Answers <- Posts %>%
    filter(PostTypeId == 2) %>%
    group_by(OwnerUserId) %>%
    summarise(AnswersNumber = n())
  
  Questions <- Posts %>%
    filter(PostTypeId == 1) %>%
    group_by(OwnerUserId) %>%
    summarise(QuestionsNumber = n())
  
  PostsCount <- inner_join(
    Answers,
    Questions,
    by=join_by(OwnerUserId)
  ) %>%
    filter(AnswersNumber > QuestionsNumber & !is.na(OwnerUserId)) %>%
    arrange(desc(AnswersNumber)) %>%
    slice_head(n=5)
  
  inner_join(
    Users,
    PostsCount,
    by=join_by(Id==OwnerUserId)
  ) %>%
    select(DisplayName, QuestionsNumber, AnswersNumber, Location, Reputation, UpVotes, DownVotes)
}

table_4 <- function(Posts, Users) {
  Answers <- Posts[
    PostTypeId==2, 
    .(AnswersNumber = .N),
    by=OwnerUserId]
  
  Questions <- Posts[
    PostTypeId==1,
    .(QuestionsNumber = .N),
    by=OwnerUserId]
  
  PostsCount <- merge(
    Answers,
    Questions,
    by="OwnerUserId"
  )[!is.na(OwnerUserId) & AnswersNumber > QuestionsNumber][
    order(AnswersNumber, decreasing = TRUE)
  ][1:5]
  
  merge(
    Users,
    PostsCount,
    by.x="Id",
    by.y="OwnerUserId"
  )[, c("DisplayName", "QuestionsNumber", "AnswersNumber", "Location", "Reputation", "UpVotes", "DownVotes")]
  
}

sql_4_result <- sql_4(Posts, Users)
base_4_result <- base_4(Posts, Users)
dplyr_4_result <- dplyr_4(Posts, Users)
table_4_result <- table_4(Posts, Users)

all_equal(sql_4_result, base_4_result)
all_equal(sql_4_result, dplyr_4_result)
all_equal(sql_4_result, table_4_result)

microbenchmark::microbenchmark(
  sqldf = sql_4(Posts, Users),
  base = base_4(Posts, Users),
  dplyr = dplyr_4(Posts, Users),
  table = table_4(Posts, Users)
)

}



