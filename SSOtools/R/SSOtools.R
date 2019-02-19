
library("httr")
library("jsonlite")
library("dplyr")
library("tidyr")
library("foreign")
library("reshape2")
library("lubridate")
library("xlsx")
library("sqldf")
library("foreign")
library("car")
library("ggplot2")
library("sqldf")
library("xlsx")
library("rms")
library("lubridate")
library("rapportools")
library("haven")


swingexport <- function()
{
  
  df <- data.frame()  
  
  for(sheet in 1:length(2000:year(Sys.Date())))
  {
    
    r <- read.xlsx2("Stad in Cijfers - Databank - Presentaties.xls", 
                    sheetIndex = sheet, startRow=2)
    
    names(r)  <- gsub("(\\.)", "", names(r))
    names(r)  <- gsub("aantal", "", names(r))
    names(r)  <- gsub("wijk_", "", names(r))
    
    r$jaar <- 2000+ sheet-1
    
    r <- r[-nrow(r), ]  
    
    df <- rbind(df,r)
    
  }
  
  for(i in 1:length(names(df)))
  {
    
    df[[i]] <- ifelse(grepl("^[ \t\n]*$", df[[i]]), NA,
                      ifelse(df[[i]] == "-", NA,
                             df[[i]]))
  }
  
  lookup <- read.csv2("conv.csv")
  lookup$buurt <- toupper(lookup$buurt)
  lookup$buurt  <- gsub("BUURT_", "", lookup$buurt)
  lookup$buurt  <- gsub("_", "-", lookup$buurt)
  names(df)[1] <- "buurt"
  df <- merge(df,lookup, all.x=T, by="buurt")
  return(df)
  
}

swingexportwijk <- function()
{
  
  df <- data.frame()  
  
  for(sheet in 1:length(2002:year(Sys.Date())))
  {
    
    r <- read.xlsx2("Stad in Cijfers - Databank - Presentaties (6).xls", 
                    sheetIndex = sheet, startRow=2)
    
    names(r)  <- gsub("(\\.)", "", names(r))
    names(r)  <- gsub("aantal", "", names(r))
    names(r)  <- gsub("wijk_", "", names(r))
    
    
    
    r$jaar <- 2002+ sheet-1
    
    r <- r[-nrow(r), ]  
    
    df <- rbind(df,r)
    
  }
  
  for(i in 1:length(names(df)))
  {
    
    df[[i]] <- ifelse(grepl("^[ \t\n]*$", df[[i]]), NA,
                      ifelse(df[[i]] == "-", NA,
                             ifelse(df[[i]] == ".", NA,     
                                    df[[i]])))
    
    df[[i]] <- gsub("\\s*\\([^\\)]+\\)", "", df[[i]])
    df[[i]] <- gsub(",", ".", df[[i]])
  }
  
  return(df)
  
}

indicator <- function(df, x, y, z)
{
  
  df$x<- as.numeric(df[,which(colnames(df)==x)])
  df$y<- as.numeric(df[,which(colnames(df)==y)])
  
  a <- as.data.frame(sqldf('select wijk, jaar, x/y a from df')) #where 
  
  a <- na.omit(a)
  
  b <- as.data.frame(sqldf('select min.wijk, min(min.jaar), min.a ,max.wijk, max(max.jaar) jaar,max.a, (max.a-min.a)/min.a*100 b
                           from a min
                           join a max ON (min.wijk=max.wijk) group by min.wijk, max.wijk '))
  
  c <- as.data.frame(sqldf('select df1.jaar,df1.wijk, df1.x/df3.x_all c
                           from df df1
                           join (select jaar, sum(x) x_all from df where jaar between (select max(jaar)-10 from df ) and (select max(jaar) from df) group by jaar) df3 on df1.jaar=df3.jaar
                           where df1.jaar between (select max(jaar)-10 from df ) and (select max(jaar) from df ) 
                           group by df1.wijk,df1.jaar '))
  
  
  d <- merge(a, c, by=c("wijk","jaar"))
  
  
  e <- as.data.frame(sqldf('select t.jaar, t.wijk, t.c,t.a, GR.b
                           from d t JOIN b GR on GR.wijk=t.wijk where t.jaar = (select distinct jaar from b )  '))
  
  
  f <- merge(c, a, by=c("wijk","jaar"))
  
  
  g <- as.data.frame(sqldf('select t.jaar, t.wijk, t.c,t.a, GR.b
                           from f t JOIN b GR on GR.wijk=t.wijk where t.jaar = (select distinct jaar from b )  '))
  
  h <- paste(z,"_FENOMEEN", sep="")
  i <- paste(z,"_GROEI", sep="")
  j <- paste(z,"_IMPACT", sep="")
  
  names(g) <- c("jaar","wijk",j,h,i)
  
  g <- g[,-1]
  
  return(g)
}

indicator2 <- function(df, x, y, z, ratio = TRUE)
{
  if(ratio==F)
  {
    
    df$x<- as.numeric(df[,which(colnames(df)==x)])
    df$y<- as.numeric(df[,which(colnames(df)==y)])
    
    a <- as.data.frame(sqldf('select wijk, jaar, x a from df
                             where y > 80')) 
    
    a <- na.omit(a)
    
    b <- as.data.frame(sqldf('select min.wijk, min(min.jaar), min.a ,max.wijk, max(max.jaar) jaar,max.a, (max.a-min.a)/min.a*100 b
                             from a min
                             join a max ON (min.wijk=max.wijk) group by min.wijk, max.wijk '))
    
    
    c <- as.data.frame(sqldf('select t.jaar, t.wijk,t.a, GR.b
                             from a t JOIN b GR on GR.wijk=t.wijk where t.jaar = (select distinct jaar from b )  '))
    
    h <- paste(z,"_FENOMEEN", sep="")
    i <- paste(z,"_GROEI", sep="")
    
    names(c) <- c("jaar","wijk",h,i)
    
    c <- c[,-1]
    
    return(c)
    
  } 
  
  else 
  {
    df$x<- as.numeric(df[,which(colnames(df)==x)])
    df$y<- as.numeric(df[,which(colnames(df)==y)])
    
    a <- as.data.frame(sqldf('select wijk, jaar, x/y a from df
                                where jaar between (select max(jaar)-10 from df group by wijk) and (select max(jaar) from df group by wijk)'))
    
    a <- na.omit(a)
    
    b <- as.data.frame(sqldf('select min.wijk, min(min.jaar), min.a ,max.wijk, max(max.jaar) jaar,max.a, (max.a-min.a)/min.a*100 b
                             from a min
                             join a max ON (min.wijk=max.wijk) group by min.wijk, max.wijk '))
    
    c <- as.data.frame(sqldf('select df1.jaar,df1.wijk, df1.x/df3.x_all c
                             from df df1
                             join (select jaar, sum(x) x_all from df where jaar between (select max(jaar)-10 from df ) and (select max(jaar) from df) group by jaar) df3 on df1.jaar=df3.jaar
                             where df1.jaar between (select max(jaar)-10 from df ) and (select max(jaar) from df ) 
                             group by df1.wijk,df1.jaar '))
    
    
    d <- merge(a, c, by=c("wijk","jaar"))
    
    
    e <- as.data.frame(sqldf('select t.jaar, t.wijk, t.c,t.a, GR.b
                             from d t JOIN b GR on GR.wijk=t.wijk where t.jaar = (select distinct jaar from b )  '))
    
    
    f <- merge(c, a, by=c("wijk","jaar"))
    
    
    g <- as.data.frame(sqldf('select t.jaar, t.wijk, t.c,t.a, GR.b
                             from f t JOIN b GR on GR.wijk=t.wijk where t.jaar = (select distinct jaar from b )  '))
    
    h <- paste(z,"_FENOMEEN", sep="")
    i <- paste(z,"_GROEI", sep="")
    j <- paste(z,"_IMPACT", sep="")
    
    names(g) <- c("jaar","wijk",j,h,i)
    
    g <- g[,-1]
    
    return(g)
  }
}

missing <- function(x, excl=NA)
{
  if(!is.na(excl[1]))
  {
    
    z <- which(names(x) %in% excl)
    df <- x[,-z]
    
    for(i in 1:length(names(df)))
    {
      
      df[[i]] <- ifelse(grepl("^[ \t\n]*$", df[[i]]), NA,
                        ifelse(df[[i]] == "--", NA,
                               ifelse(df[[i]] == 0, NA,
                                      df[[i]])))
    }
    
    b <- x[, z]
    df <- cbind(df,b)
    return(df)
    
  }
  else 
  {
    df <- x
    
    for(i in 1:length(names(df)))
    {
      
      df[[i]] <- ifelse(grepl("^[ \t\n]*$", df[[i]]), NA,
                        ifelse(df[[i]] == "--", NA,
                               ifelse(df[[i]] == 0, NA,
                                      df[[i]])))
    }
  }
  return(df)
}

automatic_recode <- function(df,x,table)
{
  
  df$newtype<- df[,which(colnames(df)==x)]
  
  for(i in 1:length(table$old))
  {
    a <- table$old[i]
    b <- table$new[i]
    
    df[which(df$newtype == a),which(colnames(df)=="newtype")] <- b
    
  }
  
  return(df)
  
}
