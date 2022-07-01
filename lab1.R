#1. Wczytaj plik autaSmall.csv i wypisz pierwsze 5 wierszy

# https://mega.nz/file/5BF3TSwQ#zOXoJLAd4kHyPmn-75tfmel_iswpoxXT97AmH6qpsm0
# https://api.openweathermap.org/data/2.5/weather?q=Warszawa&appid=1765994b51ed366c506d5dc0d0b07b77
#getwd()
#?read.csv

data <- read.csv("./autaSmall.csv", encoding = "UTF-8")

df1 <- head(data,5)

df1
length(df1)

#2. Pobierz dane pogodowe z REST API

#install.packages("jsonlite")
#install.packages("httr")

library(jsonlite)
require(httr)

#httr::GET()

endpoint <- "https://api.openweathermap.org/data/2.5/weather?q=Warszawa&appid=1765994b51ed366c506d5dc0d0b07b77"
response <- GET(endpoint)
response
content <- content(response, "text")
content

fromJSON(endpoint)
fromJSON(content)

weatherDf <- as.data.frame(fromJSON(endpoint))
weatherDf <- as.data.frame(fromJSON(content))

View(weatherDf)


#3.Napisz funckję zapisującą porcjami danych plik csv do tabeli  w SQLite



install.packages("DBI")
install.packages("RSQLite")

library(DBI)
library(RSQLite)

#?read.table
#?file
#?dbWriteTable

# 
# i <- 0
# repeat{
#   if(i>5){
#     break
#   }
#   print(i)
#   i <- i+1
# }



# 1
df1 <- read.table("autaSmall.csv", header=TRUE, sep=",", fileEncoding = "UTF-8", nrows=10)

# 2

con <- dbConnect(SQLite(), "auta.sqlite")
fileCon <- file(description = "autaSmall.csv", open="r", encoding = "UTF-8")

df1 <- read.table(fileCon, header=TRUE, sep=",", fill=TRUE, fileEncoding = "UTF-8", nrows=90)
View(df1)
myColNames <- names(df1)
myColNames
dbWriteTable(con, "tabela", df1, append=FALSE, overwrite=TRUE)
print(df1)

# zais do bazy

# ?nrow

print(nrow(df1))
repeat {
  if(nrow(df1)==0) {
    close(fileCon)
    dbDisconnect(con)
    break
  }
  df1 <- read.table(fileCon, col.names= myColNames, sep=",", fill=TRUE, fileEncoding = "UTF-8", nrows=90)
  dbWriteTable(con, "tabela", df1, append=TRUE, overwrite=FALSE)
  print(nrow(df1))
}

View(df1)



readToBase<-function(filepath,dbConn,tablename,size=100, sep=",",header=TRUE,delete=TRUE, encoding = "UTF-8"){

  ap = !delete
  ov = delete
  fileCon <- file(description = filepath, open="r", encoding = encoding)
  
  df <- read.table(fileCon, header=header, sep=sep, fill=TRUE, fileEncoding = encoding, nrows=size)
  myColNames <- names(df)

  dbWriteTable(dbConn, tablename, df, append=ap, overwrite=ov)
#  print(df1)

  print(nrow(df))
  repeat {
    if(nrow(df)==0) {
      close(fileCon)
      dbDisconnect(con)
      break
    }
    df <- read.table(fileCon, col.names= myColNames, sep=sep, fill=TRUE, fileEncoding = encoding, nrows=size)
    dbWriteTable(dbConn, tablename, df1, append=TRUE, overwrite=FALSE)
    print(nrow(df))
  }
}


dbConn <- dbConnect(SQLite(), "auta.sqlite")
filepath = "autaSmall.csv"

readToBase(filepath, dbConn, "auta2", 1000)

# 3a. Napisz funkcję zapisującą porcjami danych plik csv do tabeli w SQLite
# Utworzenie bazy na podstawie pliku auta2.csv - 3.2GB

install.packages("DBI")
install.packages("RSQLite")
library(DBI)
library(RSQLite)

con <- dbConnect(SQLite(), "auta2.sqlite")

readToBase<-function(filepath,con,tablename,size=100, sep=",",header=TRUE,delete=TRUE, encoding="UTF-8"){
  ap = !delete
  ov = delete
  
  fileCon <- file(description=filepath, open = "r", encoding = encoding)
  
  df1 <- read.table(fileCon, header = TRUE, sep=sep, fill=TRUE,
                    fileEncoding = encoding, nrows = size)
  if( nrow(df1)==0)
    return(0)
  myColNames <- names(df1)
  dbWriteTable(con, tablename, df1, append=ap, overwrite=ov)
  # zapis do bazy
  repeat{
    if(nrow(df1)==0){
      close(fileCon)
      dbDisconnect(con)
      break;
    }
    df1 <- read.table(fileCon, col.names = myColNames, sep=sep,
                      fileEncoding = encoding, nrows = size)
    dbWriteTable(con, tablename, df1, append=TRUE, overwrite=FALSE)
  }
}

readToBase("auta2.csv", con, "auta2", 1000)


#4.Napisz funkcję znajdującą tydzień obserwacji z największą średnią ceną ofert korzystając z zapytania SQL.

dbConn <- dbConnect(SQLite(), "auta.sqlite")
res <- dbSendQuery(dbConn, "SELECT tydzien, avg_week_price 
          FROM 
          (
            SELECT tydzien, AVG(cena) as avg_week_price 
            FROM auta2
            GROUP BY tydzien
          ) 
          WHERE avg_week_price=(SELECT  max(avg_week_price) 
                        FROM (select tydzien, AVG(cena) as avg_week_price 
                              FROM auta2 GROUP BY tydzien))")
df_z_bazy <- dbFetch(res)
print(df_z_bazy)
dbClearResult(res)
dbDisconnect(dbConn)

#5. Podobnie jak w poprzednim zadaniu napisz funkcję znajdującą tydzień obserwacji z największą średnią ceną ofert  tym razem wykorzystując REST api.

#    http://54.37.136.190:8000/__docs__/

library(httr)
library(jsonlite)

url <- "http://54.37.136.190:8000/week?t="
df_weeks_avg_price = NULL
i = 0
repeat
{
  i <- i + 1 
  page <- i
  week_url <- paste(url, page, sep="")
  getWeek <- GET(week_url)
  getWeek_text <- content(getWeek, "text")
  getWeek_json <- fromJSON(getWeek_text, flatten = TRUE)
  getWeek_df <- as.data.frame(getWeek_json)
  getWeek_avg_price <- mean(getWeek_df$cena, na.rm = TRUE)
  print(getWeek_avg_price)
  if(getWeek_avg_price == 0) {
    break;
  }
  df_weeks_avg_price = rbind(df_weeks_avg_price, data.frame(page, getWeek_avg_price))
}

getWeek_max_avg_price <- subset(df_weeks_avg_price, df_weeks_avg_price$getWeek_avg_price == max(df_weeks_avg_price$getWeek_avg_price))
View(getWeek_max_avg_price)

write.csv(df_weeks_avg_price,"df_weeks_avg_price.csv", row.names = FALSE)





