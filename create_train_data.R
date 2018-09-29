rm(list = ls())
setwd('/home/sambeet/data/kaggle/santader product recommendation/')
library(data.table)
library(dplyr)
library(readr)
library(tidyr)
library(lubridate)
library(fasttime)

set.seed(1)
df   <- (fread("train_ver2.csv"))
features <- names(df)[grepl("ind_+.*ult.*",names(df))]
test <- (fread("test_ver2.csv"))

df                     <- df %>% arrange(fecha_dato) %>% as.data.table()
df$month.id            <- as.numeric(factor((df$fecha_dato)))
df$month.previous.id   <- df$month.id - 1
test$month.id          <- max(df$month.id) + 1
test$month.previous.id <- max(df$month.id)

# Test data will contain the status of products for the previous month, which is a feature. The training data currently contains the status of products as labels, and will later be joined to the previous month to get the previous month's ownership as a feature. I choose to do it in this order so that the train/test data can be cleaned together and then split. It's just for convenience.
test <- merge(test,df[,names(df) %in% c(features,"ncodpers","month.id"),with=FALSE],by.x=c("ncodpers","month.previous.id"),by.y=c("ncodpers","month.id"),all.x=TRUE)

df <- rbind(df,test)

df[,fecha_dato:=fastPOSIXct(fecha_dato)]
df[,fecha_alta:=fastPOSIXct(fecha_alta)]
unique(df$fecha_dato)
df$month <- month(df$fecha_dato)

sapply(df,function(x)any(is.na(x)))

age.change  <- df[month.id>6,.(age,month,month.id,age.diff=c(0,diff(age))),by="ncodpers"]
age.change  <- age.change[age.diff==1]
age.change  <- age.change[!duplicated(age.change$ncodpers)]
setkey(df,ncodpers)
df <- merge(df,age.change[,.(ncodpers,birthday.month=month)],by=c("ncodpers"),all.x=TRUE,sort=FALSE)
df$birthday.month[is.na(df$birthday.month)] <- 7 # July is the only month we don't get to check for increment so if there is no update then use it
df$age[df$birthday.month <= 7 & df$month.id<df$birthday.month] <- df$age[df$birthday.month <= 7 & df$month.id<df$birthday.month]  - 1 # correct ages in the first 6 months

df$age[is.na(df$age)] <- -1

df$age <- round(df$age)
df <- as.data.frame(df)

sum(is.na(df$ind_nuevo))
months.active <- df[is.na(df$ind_nuevo),] %>%
    group_by(ncodpers) %>%
    summarise(months.active=n())  %>%
    select(months.active)
max(months.active)
df$ind_nuevo[is.na(df$ind_nuevo)] <- 1 
sum(is.na(df$antiguedad))
summary(df[is.na(df$antiguedad),]%>%select(ind_nuevo))

new.antiguedad <- df %>% 
    dplyr::select(ncodpers,month.id,antiguedad) %>%
    dplyr::group_by(ncodpers) %>%
    dplyr::mutate(antiguedad=min(antiguedad,na.rm=T) + month.id - 6) %>% #month 6 is the first valid entry, so reassign based upon that reference
    ungroup() %>%
    dplyr::arrange(ncodpers) %>%
    dplyr::select(antiguedad)
df <- df %>%
    arrange(ncodpers) # arrange so that the two data frames are aligned
df$antiguedad <- new.antiguedad$antiguedad

df$antiguedad[df$antiguedad<0] <- -1

elapsed.months <- function(end_date, start_date) {
    12 * (year(end_date) - year(start_date)) + (month(end_date) - month(start_date))
}
recalculated.antiguedad <- elapsed.months(df$fecha_dato,df$fecha_alta)
df$antiguedad[!is.na(df$fecha_alta)] <- recalculated.antiguedad[!is.na(df$fecha_alta)]
df$ind_nuevo <- ifelse(df$antiguedad<=6,1,0) # reassign new customer index
gc()

df$fecha_alta[is.na(df$fecha_alta)] <- median(df$fecha_alta,na.rm=TRUE)
table(df$indrel)
df$indrel[is.na(df$indrel)] <- 1
df <- df %>% select(-tipodom,-cod_prov)
sapply(df,function(x)any(is.na(x)))
sum(is.na(df$ind_actividad_cliente))
df$ind_actividad_cliente[is.na(df$ind_actividad_cliente)] <- median(df$ind_actividad_cliente,na.rm=TRUE)
unique(df$nomprov)
df$nomprov[df$nomprov==""] <- "UNKNOWN"
sum(is.na(df$renta))
gc()
df$renta[is.na(df$renta)] <- -1
gc()
sum(is.na(df$ind_nomina_ult1))
df[is.na(df)] <- 0
str(df)

char.cols <- names(df)[sapply(df,is.character)]
for (name in char.cols){
    print(sprintf("Unique values for %s:", name))
    print(unique(df[[name]]))
}

df$indfall[df$indfall==""]                 <- "N"
df$tiprel_1mes[df$tiprel_1mes==""]         <- "A"
df$indrel_1mes[df$indrel_1mes==""]         <- "1"
df$indrel_1mes[df$indrel_1mes=="P"]        <- "5"
df$indrel_1mes <- as.factor(as.integer(df$indrel_1mes))

df$pais_residencia[df$pais_residencia==""] <- "UNKNOWN"
df$sexo[df$sexo==""]                       <- "UNKNOWN"
df$ult_fec_cli_1t[df$ult_fec_cli_1t==""]   <- "UNKNOWN"
df$ind_empleado[df$ind_empleado==""]       <- "UNKNOWN"
df$indext[df$indext==""]                   <- "UNKNOWN"
df$indresi[df$indresi==""]                 <- "UNKNOWN"
df$conyuemp[df$conyuemp==""]               <- "UNKNOWN"
df$segmento[df$segmento==""]               <- "UNKNOWN"

gc()

features <- grepl("ind_+.*ult.*",names(df))
df[,features] <- lapply(df[,features],function(x)as.integer(round(x)))

create.lag.feature <- function(dt, # should be a data.table!
                               feature.name, # name of the feature to lag
                               months.to.lag=1,# vector of integers indicating how many months to lag
                               by=c("ncodpers","month.id"), # keys to join data.tables by
                               na.fill = NA)  
{
    # get the feature and change the name to avoid .x and .y being appending to names
    dt.sub <- dt[,mget(c(by,feature.name))]
    names(dt.sub)[names(dt.sub) == feature.name] <- "original.feature"
    original.month.id <- dt.sub$month.id
    added.names <- c()
    for (month.ago in months.to.lag){
        print(paste("Collecting information on",feature.name,month.ago,"month(s) ago"))
        colname <- paste("lagged.",feature.name,".",month.ago,"months.ago",sep="")
        added.names <- c(colname,added.names)
        # This is a self join except the month is shifted
        dt.sub <- merge(dt.sub,
                        dt.sub[,.(ncodpers,
                                  month.id=month.ago+original.month.id,
                                  lagged.feature=original.feature)],
                        by=by,
                        all.x=TRUE,
                        sort=FALSE)
        names(dt.sub)[names(dt.sub)=="lagged.feature"] <- colname
        # dt.sub[[colname]][is.na(dt.sub[[colname]])] <- dt.sub[["original.feature"]][is.na(dt.sub[[colname]])]
    }
    df <- merge(dt,
                dt.sub[,c(by,added.names),with=FALSE],
                by=by,
                all.x=TRUE,
                sort=FALSE)
    df[is.na(df)] <- na.fill
    return(df)
}

#Lag features
df <- as.data.table(df)
gc()
df <- create.lag.feature(df,'ind_actividad_cliente',1:11,na.fill=0)
df[,last.age:=lag(age),by="ncodpers"]
df$turned.adult <- ifelse(df$age==20 & df$last.age==19,1,0)
df <- as.data.frame(df)

features <- names(df)[grepl("ind_+.*ult.*",names(df))]

test <- df %>%
    filter(month.id==max(df$month.id))
df <- df %>%
    filter(month.id<max(df$month.id))
write.csv(df,"cleaned_train.csv",row.names=FALSE)
write.csv(test,"cleaned_test.csv",row.names=FALSE)

library(data.table)
# setwd('~/kaggle/competition-santander/')
df     <- fread("cleaned_train.csv")
labels <- names(df)[grepl("ind_+.*_+ult",names(df))]
cols   <- c("ncodpers","month.id","month.previous.id",labels)
df     <- df[,names(df) %in% cols,with=FALSE]

# connect each month to the previous one
df     <- merge(df,df,by.x=c("ncodpers","month.previous.id"),by.y=c("ncodpers","month.id"),all.x=TRUE)

# entries that don't have a corresponding row for the previous month will be NA and
# I will treat these as if that product was owned 
df[is.na(df)] <- 0

# for each product, the difference between the current month on the left and the
# previous month on the right indicates whether a product was added (+1), dropped (-1),
# or unchanged (0)
products <- rep("",nrow(df))
for (label in labels){
    colx  <- paste0(label,".x")
    coly  <- paste0(label,".y")
    diffs <- df[,.(get(colx)-get(coly))]
    products[diffs>0] <- paste0(products[diffs>0],label,sep=" ")
}
gc()
# write results
df <- df[,.(ncodpers,month.id,products)]
write.csv(df,"purchased-products.csv",row.names=FALSE)

apk <- function(k, actual, predicted)
{
    if (length(actual)==0){return(0.0)}
    score <- 0.0
    cnt <- 0.0
    for (i in 1:min(k,length(predicted)))
    {
        if (predicted[i] %in% actual && !(predicted[i] %in% predicted[0:(i-1)]))
        {
            cnt   <- cnt + 1
            score <- score + cnt/i 
        }
    }
    score <- score / min(length(actual), k)
    if (is.na(score)){
        debug<-0
    }
    return(score)
}

mapk <- function (k, actual, predicted)
{
    if( length(actual)==0 || length(predicted)==0 ) 
    {
        return(0.0)
    }
    
    scores <- rep(0, length(actual))
    for (i in 1:length(scores))
    {
        scores[i] <- apk(k, actual[[i]], predicted[[i]])
    }
    score <- mean(scores)
    score
}

library(data.table)
df     <- fread("cleaned_train.csv")
labels <- names(df)[grepl("ind_+.*_+ult",names(df))]
cols   <- c("ncodpers","month.id","month.previous.id",labels)
df     <- df[,names(df) %in% cols,with=FALSE]
df     <- merge(df,df,by.x=c("ncodpers","month.previous.id"),by.y=c("ncodpers","month.id"),all.x=TRUE)

df[is.na(df)] <- 0
products <- rep("",nrow(df))
num.transactions <- rep(0,nrow(df))
purchase.frequencies <- data.frame(ncodpers=df$ncodpers, month.id=(df$month.previous.id + 2))
for (label in labels){
    colx  <- paste0(label,".x") # x column is the left data frame and contains more recent information
    coly  <- paste0(label,".y")
    diffs <- df[,.(ncodpers,month.id,change=get(colx)-get(coly))]
    # num.transactions counts the number of adds and drops
    num.transactions <- num.transactions + as.integer(diffs$change!=0)
    diffs[diffs<0] <- 0 # only consider positive cases for the purchase frequency
    setkey(diffs,ncodpers)
    d <- diffs[,.(frequency = cumsum(change)),by=ncodpers]
    purchase.frequencies[[paste(label,"_purchase.count",sep="")]] <- d$frequency
    
}
purchase.frequencies$num.transactions <- num.transactions
purchase.frequencies <- purchase.frequencies %>%
    dplyr::group_by(ncodpers) %>%
    dplyr::mutate(num.transactions = cumsum(num.transactions))
write.csv(purchase.frequencies,"purchase.frequencies.csv",row.names=FALSE)

months.since.owned<- function(dt,products,months.to.search,default.value = 999){
    
    for (product in products){
        print(paste("Finding months since owning",product))
        colname <- paste(product,".last.owned",sep="")
        dt[[colname]] <- default.value
        for (month.ago in seq(months.to.search,1,-1)){
            cur.colname <- paste(product,"_",month.ago,"month_ago",sep="")
            dt[[colname]][dt[[cur.colname]] == 1] <- month.ago
        }
    }
    return(dt)
    
}


#####################
library(tidyr)
library(xgboost)
library(plyr)
library(dplyr)
library(data.table)
library(ggplot2)
library(caret)
library(pROC)
library(lubridate)
library(fasttime)

set.seed(1)
# Train on month 5 and 11 and validate on 17 for CV data then
# train on month 6 and 12 and predict on test. The second months are separated
# into a separate variable so I can turn on/off using them
val.train.month <- 5
val.test.month  <- 17
train.month     <- 6
extra.train.months.val <- c(11)
extra.train.months.test <- c(12)

months.to.keep  <- c(val.train.month,val.test.month,train.month,extra.train.months.val,extra.train.months.test)
df   <- fread("cleaned_train.csv")
test <- fread("cleaned_test.csv")

# add activity index previous month
recent.activity.index <- merge(rbind(df[,.(ncodpers,month.id,ind_actividad_cliente,
                                           segmento)],
                                     test[,.(ncodpers,month.id,ind_actividad_cliente,
                                             segmento)]),
                               df[,.(ncodpers,month.id=month.id+1,
                                     old.ind_actividad_cliente=ind_actividad_cliente,
                                     old.segmento=segmento)],
                               by=c("ncodpers","month.id"),
                               sort=FALSE)
# all.x=TRUE) # might not want all.x here, means people that weren't customers last month will be considered to change activity
recent.activity.index[,activity.index.change:=ind_actividad_cliente-old.ind_actividad_cliente]
recent.activity.index[,segmento.change:=as.integer(segmento!=old.segmento)]
df   <- merge(df,recent.activity.index[,.(ncodpers,
                                          month.id,
                                          old.ind_actividad_cliente,
                                          activity.index.change,
                                          old.segmento,
                                          segmento.change)],
              by=c("ncodpers","month.id"),all.x=TRUE)

test <- merge(test,recent.activity.index[,.(ncodpers,
                                            month.id,
                                            old.ind_actividad_cliente,
                                            activity.index.change,
                                            old.segmento,
                                            segmento.change)],
              by=c("ncodpers","month.id"),all.x=TRUE)
gc()
df$old.segmento[is.na(df$old.segmento)] <- df$segmento[is.na(df$old.segmento)] 
df$ind_actividad_cliente[is.na(df$ind_actividad_cliente)] <- df$old.ind_actividad_cliente[is.na(df$ind_actividad_cliente)] 

df[is.na(df)] <- 0

products <- names(df)[grepl("ind_+.*_+ult",names(df))]
gc()
# create a data frame with just the product ownership variables so we can create lag ownership features
products.owned <- df %>%
    select(ncodpers,month.id,one_of(products)) %>%
    as.data.table()

df   <- as.data.table(df)
test <- as.data.table(test)
original.month.id <- products.owned$month.id
df <- df[month.id %in% months.to.keep,]


test <- test[,!names(test) %in% products,with=FALSE] #lazy, but I'm removing product ownership because it is about to be readded month by month
gc()
# create features indicating whether or not a product was owned in each of the past
# X months. for each lag, match the month with the earlier one and through some name manipulation
# extract whether the product was owned or not
for (month.ago in 1:11){
    print(paste("Collecting data on product ownership",month.ago,"months ago..."))
    products.owned[,month.id:=original.month.id+month.ago]
    df <- merge(df,products.owned,by=c("ncodpers","month.id"),suffixes = c('.x','.y'))
    change.names <- names(df)[grepl("\\.y",names(df))]
    new.names <- gsub("\\.y",paste("_",month.ago,"month_ago",sep=""),change.names)
    names(df)[grepl("\\.y",names(df))] <- new.names
    
    #I'm being lazy here...
    change.names <- names(df)[grepl("\\.x",names(df))]
    new.names <- gsub("\\.x","",change.names)
    names(df)[grepl("\\.x",names(df))] <- new.names
    
    
    test <- merge(test,products.owned,by=c("ncodpers","month.id"),all.x=TRUE)
    
    change.names <- names(test)[grepl("\\.y",names(test))]
    new.names <- gsub("\\.y",paste("_",month.ago,"month_ago",sep=""),change.names)
    names(test)[grepl("\\.y",names(test))] <- new.names
    
    change.names <- names(test)[grepl("\\.x",names(test))]
    new.names <- gsub("\\.x","",change.names)
    names(test)[grepl("\\.x",names(test))] <- new.names
    
}
names(test)[names(test) %in% products] <- paste(names(test)[names(test) %in% products],"_1month_ago",sep="")

# there will be NA values where there isn't a match to the left side since we used all.x=TRUE, assume those correspond
# to products that were not owned
df[is.na(df)] <- 0
test[is.na(test)] <- 0

# get the number of months since each product was owned
df <- months.since.owned(df,products,12)
test <- months.since.owned(test,products,12)
df <- as.data.frame(df)
test <- as.data.frame(test)


# compute total number of products owned previous month
gc()
df$total_products <- rowSums(df[,names(df) %in% names(df)[grepl("ind.*1month\\_ago",names(df))]],na.rm=TRUE)
test$total_products <- rowSums(test[,names(test) %in% names(test)[grepl("ind.*1month\\_ago",names(test))]],na.rm=TRUE)

# save the month id for use creating window ownership features
products.owned$month.id <- original.month.id

# windows of product ownership. For each window size look back at previous months and see if the product was 
# ever owned. I do this by adding the value of the ownership variable X months ago for X = 1:window.size
# then converting to a binary indicator if the value is positive (meaning it was owned at least once)
for (product in products){
    for (window.size in 2:6){
        print(paste("Getting ownership for",product,"within last",window.size,"months"))
        colname <- paste(product,".owned.within.",window.size,"months",sep="")
        df[[colname]]   <- 0
        test[[colname]] <- 0
        for (month.ago in 1:window.size){
            current.col     <- paste(product,"_",month.ago,"month_ago",sep="")
            df[[colname]]   <- df[[colname]]  + df[[current.col]]
            test[[colname]] <- test[[colname]]  + test[[current.col]]
        }
        df[[colname]]   <- as.integer(df[[colname]] > 0)
        test[[colname]] <- as.integer(test[[colname]] > 0)
    }
}

# add in purchase frequency feature for each product
purchase.frequencies <- fread("purchase.frequencies.csv")
            
df   <- merge(df,purchase.frequencies,by=c("month.id","ncodpers"),all.x = TRUE)
test <- merge(test,purchase.frequencies,by=c("month.id","ncodpers"), all.x=TRUE)
df[is.na(df)] <- 0
test[is.na(test)] <- 0
gc()
# fix some rare value that was causing an error
df$sexo[df$sexo=="UNKNOWN"] <- "V"
test$sexo[test$sexo=="UNKNOWN"] <- "V"

# append "_target" so I can keep straight which are the target variables and which indicate ownership as a feature
new.names <- names(df)
new.names[new.names %in% products] <- paste(new.names[new.names %in% products],"_target",sep="")
names(df) <- new.names

labels <- names(df)[grepl(".*_target",names(df))]
purchase.w <- names(df)[grepl(".*.count",names(df))]
# products <- names(df)[grepl("ind_+.*_+ult",names(df)) & !grepl(".*_target|.count|month\\_ago",names(df))]
ownership.names <- names(df)[grepl("month\\_ago",names(df))]


test$ind_empleado[test$ind_empleado=="S"] <- "N" # Some rare value that was causing errors with factors later
char.cols <- names(test)[sapply(test,is.character)]
test[,char.cols] <- lapply(test[,char.cols], as.factor)

df$ind_empleado[df$ind_empleado=="S"] <- "N"
char.cols <- names(df)[sapply(df,is.character)]
df[,char.cols] <- lapply(df[,char.cols], as.factor)

# force the factor levels to be the same 
factor.cols <- names(test)[sapply(test,is.factor)]
for (col in factor.cols){
    df[[col]] <- factor(df[[col]],levels=levels(test[[col]]))
}
df$ult_fec_cli_1t[is.na(df$ult_fec_cli_1t)] <- "UNKNOWN"

# only keep entries where customers purchased products and the month matches one of our sets
purchased <- as.data.frame(fread("purchased-products.csv"))
ids.val.train   <- purchased$ncodpers[purchased$month.id %in% val.train.month & (purchased$products!="")]
ids.val.test    <- purchased$ncodpers[purchased$month.id %in% val.test.month & (purchased$products!="")]
ids.train       <- purchased$ncodpers[purchased$month.id %in% train.month & (purchased$products!="")]

extra.train.ids.val <- purchased$ncodpers[purchased$month.id %in% extra.train.months.val & (purchased$products!="")]
extra.train.ids.test <- purchased$ncodpers[purchased$month.id %in% extra.train.months.test & (purchased$products!="")]

# convert the birthday month feature to a named factor
df$birthday.month   <- factor(month.abb[df$birthday.month],levels=month.abb)
test$birthday.month <- factor(month.abb[test$birthday.month],levels=month.abb)

df$month   <- factor(month.abb[df$month],levels=month.abb)
test$month <- factor(month.abb[test$month],levels=month.abb)

# discard some columns that are no longer useful
df <- select(df,-fecha_alta,-fecha_dato,-month.previous.id)

# separate the data into the various parts
extra.train.val <- df %>% 
    filter(ncodpers %in% extra.train.ids.val & month.id %in% extra.train.months.val)

extra.train.test <- df %>% 
    filter(ncodpers %in% extra.train.ids.test & month.id %in% extra.train.months.test)

val.train <- df %>% 
    filter(ncodpers %in% ids.val.train & month.id %in% val.train.month)

val.test <- df %>% 
    filter(ncodpers %in% ids.val.test & month.id %in% val.test.month) 

df <- df %>% 
    filter(ncodpers %in% ids.train & month.id %in% train.month) 

test <- test %>% 
    dplyr::select(-fecha_alta,-fecha_dato,-month.previous.id) 

# save as binary for faster loading
save(df,test,val.train,val.test,extra.train.val,extra.train.test,file="data_prepped.RData")

library(data.table)
library(dplyr)
df <- fread("purchased-products.csv")
full <- fread("cleaned_train.csv")
labels <- grepl("\\_ult1",names(full))
full$total.products <- rowSums(full[,labels,with=FALSE])
df[["counts"]] <- sapply(strsplit(df$products," "),length)
df <- merge(df,full[,.(month.id,ncodpers,total.products)],by=c("month.id","ncodpers"),all.x=TRUE,sort=FALSE)
# df <- df %>%
# mutate(counts=(function(x)length(strsplit(x," ")))(products))
purchase.count <- rbind(data.table(ncodpers=df$ncodpers,month.id=df$month.id),
                        data.table(ncodpers=unique(df$ncodpers),month.id=18))
original.month.id <- df$month.id
for (month.ago in 1:4){
    print(paste("Collecting number of purchases",month.ago,"months ago"))
    colname <- paste("num.purchases.",month.ago,".months.ago",sep="")
    df[,month.id:=original.month.id + month.ago]
    tmp <- merge(purchase.count,df[,.(ncodpers,month.id,counts)],by=c("ncodpers","month.id"),sort=FALSE,all.x=TRUE)
    purchase.count[[colname]] <- tmp$counts
    
}

for (month.ago in 1:5){
    print(paste("Counting total products",month.ago,"months ago"))
    
    colname <- paste("total.products.",month.ago,".months.ago",sep="")
    df[,month.id:=original.month.id + month.ago]
    tmp <- merge(purchase.count,df[,.(ncodpers,month.id,total.products)],by=c("ncodpers","month.id"),sort=FALSE,all.x=TRUE)
    purchase.count[[colname]] <- tmp$total.products
}

purchase.count[is.na(purchase.count)] <- 0
write.csv(purchase.count,"purchase-count.csv",row.names=FALSE)

library(tidyr)
library(xgboost)
library(plyr)
library(dplyr)
library(data.table)
library(ggplot2)
library(caret)
library(pROC)
library(lubridate)

set.seed(1)
use.resampling.weights <- FALSE
use.many.seeds         <- F
if (use.many.seeds){
    rand.seeds <- 1:10
} else{
    rand.seeds <- 1
}
# read data
load("data_prepped.RData")
use.extra.train.FLAG = TRUE
if (use.extra.train.FLAG){
    val.train <- rbind(val.train,extra.train.val)
    df       <- rbind(df,extra.train.test)
}
labels  <- names(df)[grepl(".*_target",names(df)) & !grepl("ahor|aval",names(df))] # target values

purchase.count <- fread("purchase-count.csv")
df   <- merge(df,purchase.count,by=c("ncodpers","month.id"),sort=FALSE)
test <- merge(test,purchase.count,by=c("ncodpers","month.id"),sort=FALSE)
val.train   <- merge(val.train,purchase.count,by=c("ncodpers","month.id"),sort=FALSE)
val.test <- merge(val.test,purchase.count,by=c("ncodpers","month.id"),sort=FALSE)
rm(purchase.count)

gc()
purchased <- as.data.frame(fread("purchased-products.csv"))

products.df <- df %>%
    select(ncodpers,month.id) %>%
    merge(purchased,by=c("ncodpers","month.id"),sort=FALSE) %>%
    filter(products!="")

set.seed(1)
products.df$products <- sapply(strsplit(products.df$products, " "), function(x) sample(x,1))
products.df$products <- factor(products.df$products,levels=gsub("\\_target","",labels))
product.names.save<-gsub("\\_target","",labels)
products.df$products <- as.integer(products.df$products)-1

products.val <- val.train %>%
    select(ncodpers,month.id) %>%
    merge(purchased,by=c("ncodpers","month.id"),sort=FALSE) %>%
    filter(products!="")

set.seed(1)
products.val$products <- sapply(strsplit(products.val$products, " "), function(x) sample(x,1))
products.val$products <- factor(products.val$products,levels=gsub("\\_target","",labels))
products.val$products <- as.integer(products.val$products)-1

train.labels <- list()
train.labels[["products"]] <- as(data.matrix(products.df[["products"]]),'dgCMatrix')

train.labels.val <- list()
train.labels.val[["products"]] <- as(data.matrix(products.val[["products"]]),'dgCMatrix')

june.fractions  <- table(products.df$products[products.df$month.id==6])
june.fractions  <- june.fractions / sum(june.fractions)
total.fractions <- table(products.df$products)
total.fractions <- total.fractions / sum(total.fractions)
prod.weights.df     <- (june.fractions / total.fractions)



may.fractions   <- table(products.val$products[products.val$month.id==5])
may.fractions   <- may.fractions / sum(may.fractions)
total.fractions <- table(products.val$products)
total.fractions <- total.fractions / sum(total.fractions)
prod.weights.val     <- (may.fractions / total.fractions)


if (use.resampling.weights){
    df.weights  <- prod.weights.df[products.df$products+1]
    val.weights <- prod.weights.val[products.val$products+1]
} else {
    df.weights <- rep(1,nrow(df))
    val.weights <- rep(1,nrow(val.train))
    
}

# make sure the factor levels agree
factor.cols <- names(test)[sapply(test,is.factor)]
gc()

for (col in factor.cols){
    df[[col]] <- factor(df[[col]],levels=union(levels(df[[col]]),levels(test[[col]])))
    val.train[[col]] <- factor(val.train[[col]],levels=union(levels(val.train[[col]]),levels(val.test[[col]])))
}

# there's a bunch of features related to the products, and thus they have similar
# names. Separate them out to keep things straight
labels               <- names(df)[grepl(".*_target",names(df)) & !grepl("ahor|aval",names(df))] # target values
purchase.w           <- names(df)[grepl(".*.count",names(df))] # number of times a product has been bought in the past 5 months
ownership.names      <- names(df)[grepl("month\\_ago",names(df)) & !grepl("month\\.previous",names(df))] # various features indicating whether or not a product was owned X months ago
drop.names           <- names(df)[grepl("dropped",names(df))] # various features indicating whether or not a product was owned X months ago
add.names            <- names(df)[grepl("added",names(df))] # various features indicating whether or not a product was owned X months ago
num.added.names      <- names(df)[grepl("num\\.added",names(df))]  # total number of products added X months ago
num.purchases.names  <- names(df)[grepl("num\\.purchases",names(df))]  # total number of products added X months ago
total.products.names <- names(df)[grepl("total\\.products",names(df))]  # total number of products owned X months ago
owned.within.names   <- names(df)[grepl("owned\\.within",names(df))]  # whether or not each product was owned with X months
# numeric features to use
numeric.cols <- c("age",
                  "renta",
                  "antiguedad",
                  purchase.w,
                  "total_products",
                  "num.transactions",
                  num.purchases.names)

categorical.cols <- c("sexo",
                      "ind_nuevo",
                      "ind_empleado",
                      "segmento",
                      "nomprov",
                      "indext",
                      "indresi",
                      "indrel",
                      "tiprel_1mes",
                      ownership.names,
                      owned.within.names,
                      "segmento.change",
                      "activity.index.change",
                      "ind_actividad_cliente",
                      "month",
                      "birthday.month")



# one-hot encode the categorical features
ohe <- dummyVars(~.,data = df[,names(df) %in% categorical.cols])
ohe <- as(data.matrix(predict(ohe,df[,names(df) %in% categorical.cols])), "dgCMatrix")
ohe.test <- dummyVars(~.,data = test[,names(test) %in% categorical.cols])
ohe.test <- as(data.matrix(predict(ohe.test,test[,names(test) %in% categorical.cols])), "dgCMatrix")
ohe.val.train <- dummyVars(~.,data = val.train[,names(val.train) %in% categorical.cols])
ohe.val.train <- as(data.matrix(predict(ohe.val.train,val.train[,names(val.train) %in% categorical.cols])), "dgCMatrix")
ohe.val.test <- dummyVars(~.,data = val.test[,names(val.test) %in% categorical.cols])
ohe.val.test <- as(data.matrix(predict(ohe.val.test,val.test[,names(val.test) %in% categorical.cols])), "dgCMatrix")

# remember the id's for people and months for later since all that actually goes
# into xgboost is the raw feature data
save.id       <- df$ncodpers
save.month.id <- df$month.id
save.month    <- df$month
save.id.test       <- test$ncodpers
save.month.id.test <- test$month.id
df         <- cbind(ohe,data.matrix(df[,names(df) %in% numeric.cols]))
test       <- cbind(ohe.test,data.matrix(test[,names(test) %in% numeric.cols]))

save.id.val       <- val.train$ncodpers
save.month.id.val <- val.train$month.id
save.id.test.val       <- val.test$ncodpers
save.month.id.test.val <- val.test$month.id
save.month.val    <- val.train$month
val.train         <- cbind(ohe.val.train,data.matrix(val.train[,names(val.train) %in% numeric.cols]))
val.test       <- cbind(ohe.val.test,data.matrix(val.test[,names(val.test) %in% numeric.cols]))
set.seed(1)

# use a 75/25 train/test split so we can compute MAP@7 locally. The test set
# is predicted using a model trained on all of the training data
train.ind  <- createDataPartition(1:nrow(df),p=0.75)[[1]]

# tuning hyperparameters to optimize MAP@7 must be done manually. I previously did 
# a grid search and these parameters were okay so I commented it out for now. You just 
# simply scan parameters and save the ones that gave you the best local MAP@7 on the validation data

test.save <- test
best.map <- 0
# for (depth in c(3,5,7,9,11,15)){
# for (eta in c(0.01,0.025, 0.05,0.1,0.25,0.5)){
depth <- 7
eta <- 0.05
test <- test.save
predictions         <- list()
predictions_val     <- list()
predictions_val_future     <- list()

build.predictions.xgboost <- function(df, test, label, label.name,depth,eta, weights, rand.seeds=0){
    library(xgboost)
    # df:         training data
    # test:       the data to predict on
    # label:      vector containing the target label
    # label.name: name of the label
    # depth:      XGBoost max tree depth
    # eta:        XGBoost learning rate
    for (rand.seed.num in 1:length(rand.seeds)){
        print(paste("Building model with random seed ", rand.seeds[rand.seed.num]))
        set.seed(rand.seeds[rand.seed.num])
        dtrain <- xgb.DMatrix(data = df, label=label, weight=weights)
        model <- xgboost(data = dtrain,
                         max.depth = depth, 
                         eta = eta, nthread = 8,
                         nround = 175, 
                         objective = "multi:softprob", 
                         num_class=22, #hardcoded!
                         verbose =1 ,
                         print.every.n = 10)
        imp <- xgb.importance(feature_names = colnames(df),model=model)
        save(imp,file=paste("IMPORTANCE_multiclass_",gsub("\\_target","",label.name),".RData",sep=""))
        print(imp)
        if (rand.seed.num == 1) {# initialize predictions on first time
            preds <- predict(model,test)
        } else {
            preds <- predict(model,test) + preds
        }
    }
    predictions        <- list(preds / length(rand.seeds))
    names(predictions) <- paste(gsub("_target","",label.name),"_pred",sep="")
    return(predictions)
}


# loop over the labels and create predictions of the validation data and training data
# for each
label.count <- 1
for (label in c("products")){
    # the syntax for indexing train.labels is messy but functional
    # predictions_val <- c(predictions_val,build.predictions.xgboost(df[train.ind,],df[-train.ind,],train.labels[[label]][train.ind,1,drop=F],label,depth,eta) )
    # accuracy <- mean(train.labels[[label]][-train.ind,1]==round(predictions_val[[label.count]]))
    # print(sprintf("Accuracy for label %s = %f",label,accuracy)) # accuracy not super useful for this task
    # if (accuracy < 1){ # perfect accuracy causes some error with pROC
    # print(pROC::auc(roc(train.labels[[label]][-train.ind,1],predictions_val[[label.count]])))
    # } else {
    # print("auc perfect")
    # }
    
    # now predict on the testing data
    downweight.factor <- 1
    # predictions <- c(predictions,build.predictions.xgboost(df,test,train.labels[[label]],label,depth,eta,ifelse(save.month=="Jun",1,downweight.factor)) )
    # predictions_val_future <- c(predictions_val_future,build.predictions.xgboost(val.train,val.test,train.labels.val[[label]],label,depth,eta,ifelse(save.month.val=="May",1,downweight.factor)) )
    
    predictions <- c(predictions,build.predictions.xgboost(df,test,train.labels[[label]],label,depth,eta,weights=df.weights,rand.seeds))
    predictions_val_future <- c(predictions_val_future,build.predictions.xgboost(val.train,val.test,train.labels.val[[label]],label,depth,eta,weights=val.weights,rand.seeds))
    label.count <- label.count + 1
}

predictions[[1]]     <- matrix(predictions[[1]],nrow=nrow(test),byrow = TRUE)
predictions_val_future[[1]] <- matrix(predictions_val_future[[1]],nrow=(nrow(val.test)),byrow = TRUE)
colnames(predictions[[1]]) <- product.names.save
colnames(predictions_val_future[[1]]) <- product.names.save

# collect the results
predictions <- as.data.table(predictions[[1]])
# predictions_val <- as.data.table(predictions_val)
predictions_val_future <- as.data.table(predictions_val_future[[1]])

names.to.change <- names(predictions)
names.to.change <- gsub("products\\_pred\\.","",names.to.change)
names.to.change <- paste(names.to.change,"_pred",sep="")
names(predictions) <- names.to.change

names.to.change <- names(predictions_val_future)
names.to.change <- gsub("products\\_pred\\.","",names.to.change)
names.to.change <- paste(names.to.change,"_pred",sep="")
names(predictions_val_future) <- names.to.change


test        <- as.data.table(cbind(data.frame(data.matrix(test)),predictions))
val_future        <- as.data.table(cbind(data.frame(data.matrix(val.test)),predictions_val_future))

# can drop some of the data at this point and put back the id's
test <- test[,grepl("ind_+.*_+ult",names(test)),with=FALSE]
test$ncodpers <- save.id.test
test$month.id <- save.month.id.test

val_future <- val_future[,grepl("ind_+.*_+ult",names(val_future)),with=FALSE]
val_future$ncodpers <- save.id.test.val
val_future$month.id <- save.month.id.test.val

products <- gsub("_target","",labels)


# the features containing "1month_ago" will tell us whether or not a product is a new purchase in our predictions
owned.products <- names(test)[grepl("1month\\_ago",names(test)) & !(grepl("_pred",names(test)))]

# save the products for use in the recommendation script
save(products,file="project/Santander/lib/products.Rdata")

# put the predictions in the right format 
test <- test %>%
    select(ncodpers,month.id,contains("_pred"),contains("1month"))
names(test)[grepl("1month",names(test))] <- gsub("\\_1month\\_ago","",names(test)[grepl("1month",names(test))])

val_future <- val_future %>%
    select(ncodpers,month.id,contains("_pred"),contains("1month"))
names(val_future)[grepl("1month",names(val_future))] <- gsub("\\_1month\\_ago","",names(val_future)[grepl("1month",names(val_future))])
# save the results

val.recs.future  <- get.recommendations(as.data.table(val_future),products)
val_future$added_products <- val.recs.future$added_products

purchased <- as.data.frame(fread("purchased-products.csv"))
val_future <- val_future %>%
    merge(purchased,by=c("ncodpers","month.id"))
MAP <- mapk(k=7,strsplit(val_future$products, " "),strsplit(val_future$added_products," "))
print(paste("Validation future MAP@7 = ",MAP))

# if (MAP > best.map){
# best.map <- MAP
# out.recs <- test.recs
# best.depth <- depth
# best.eta <- eta

# }
# }
# }

write.csv(test,"xgboost_preds_test_multiclass_best.csv",row.names = FALSE)
write.csv(val_future,"xgboost_preds_val_future_multiclass_best.csv",row.names = FALSE)

#############


data_2$month = month(data_2$fecha_dato)
data_2$year = year(data_2$fecha_dato)
data_2 = data_2 %>% filter(year == 2015)
data_2 = data_2 %>% filter(month == 5 | month == 6)
gc()
data_2 = data_2 %>% select(ncodpers,month,ind_ahor_fin_ult1:ind_recibo_ult1)
data_1 = gather(data_2,key=product,value=holding,ind_ahor_fin_ult1:ind_recibo_ult1)
rm(data_2);gc()
data_1 = data_1 %>% group_by(ncodpers) %>% mutate(count = n()) %>% ungroup() %>% filter(count == 48) 
data_1 = data_1 %>% select(-count)
data_1 = data_1 %>% replace_na(replace = list(holding = 0))
min_month = min(data_1$month)
data_1$month = ifelse(data_1$month == min_month,'prev_month','current_month')
gc()
data_1 = spread(data = data_1,key = month,value = holding)
gc()
data_1$n01 = ifelse((data_1$current_month == 1) & (data_1$prev_month == 0),1,0)
data_1 = data_1 %>% group_by(ncodpers) %>% mutate(new_bought = sum(n01) >= 1)
data_1 = data_1 %>% ungroup() %>% filter(new_bought == TRUE)
data_1$n00 = ifelse((data_1$current_month == 0) & (data_1$prev_month == 0),1,0)
data_1 = data_1 %>% filter(n01 == 1 | n00 == 1)
write.csv(data_1,'062015.csv',row.names = F)

x = read_csv('062015.csv')
x = x[x$n01 == 1,] %>% select(-n00,-n11,-n01)
write.csv(x,'june_2015.csv',row.names = F)
