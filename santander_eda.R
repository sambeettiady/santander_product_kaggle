rm(list=ls())

library(data.table)
library(dplyr)
library(ggplot2)
library(lubridate)
library(readr)
library(tidyr)
library(ggthemes)

setwd('/home/sambeet/data/kaggle/santader product recommendation/')

#selectrow <- c("fecha_dato","fecha_alta","pais_residencia") # Rows that I want to analize
train = read_csv("train_ver2.csv")
test = read_csv("test_ver2.csv")

dim(train)
dim(test)

str(train)
str(test)

#Extract date and time
train$fecha_dato = ymd(train$fecha_dato) # Format date
train$fecha_alta = ymd(train$fecha_alta)
train$year_dato = year(train$fecha_dato) # Extract year
train$year_alta = year(train$fecha_alta)
train$month_dato = month(train$fecha_dato,label=T) # Extract month
train$month_alta = month(train$fecha_alta,label=T)
train$weekday_alta = wday(train$fecha_alta,label=T)
#train = as.data.table(train)

#Plot by day of week
x1 = train %>% group_by(weekday_alta) %>% summarise(count = n()) %>% filter(weekday_alta != 'NA')
ggplot(x1,aes(x = weekday_alta,y = count))+ ylab('Count of New Customers') +
    xlab('Count') + geom_bar(stat='identity',fill='red',col='black') + ggtitle("Number of customers that became 'first holder' by day of week") +
    theme_fivethirtyeight() + theme(axis.title = element_text(), axis.title.x = element_text()) + ylab('Count of Customers') + xlab('Day of week')
 
#Plot by year and month
x1 = train %>% filter(year_alta >= 2013 & year_alta <= 2015) %>% group_by(month_alta,year_alta) %>% summarise(count = n())
ggplot(x1,aes(x = month_alta,y=count,fill = factor(year_alta)))+geom_bar(stat="identity",col='black')+
    ggtitle("Number of customers that became 'first holder' by month (2013-15)") + facet_wrap(~year_alta) + 
    theme_fivethirtyeight() + theme(axis.title = element_text(), axis.title.x = element_text()) + 
    ylab('Count of New Customers') + xlab('Month') + labs(fill='Year')

#There is a significant rise in July that remains until Autumn, 
#this might be because July is the first month in Spain with vacations, 
#and also we can see that between September and October has the most number of first holders, 
#that is because in Spain, the academic calendar starts in September-October and is considered like a "new year",
#very prone to doing new things, for example, to open a new bank account.

#Gross income of the household
x1 = train[!is.na(train$renta),c('ind_empleado','renta')]
ggplot(x1,aes(x = ind_empleado,y = renta,fill=ind_empleado)) + theme_fivethirtyeight() +
    ggtitle("Gross income of the household by Employee index") +
    geom_boxplot(na.rm=TRUE)+scale_y_log10() + theme(axis.title = element_text(), axis.title.x = element_text()) + xlab('Employee Type') + ylab('Gross Income') + 
    scale_fill_discrete(name = "Employee Index",labels = c("Active", "Ex-employed","Filial","Not an employee","Passive"))

#Income by segmentation
x1 = train[!is.na(train$renta),c('segmento','renta')] %>% filter(segmento != 'NA')
ggplot(x1,aes(x = segmento,y = renta,fill=segmento))+theme_fivethirtyeight()+
    ggtitle("Gross Income of the household by segmentation")+
    geom_boxplot(na.rm=TRUE)+scale_y_log10()+ theme(axis.title = element_text(), axis.title.x = element_text()) +
    xlab('Customer Segment') + ylab('Gross Income') + 
    scale_fill_discrete(name = "Segment",labels = c("Top", "Particular","Graduates"))

#Age
x = train %>% filter(age<110) %>% select(ncodpers,ind_ahor_fin_ult1:ind_recibo_ult1) %>%  group_by(ncodpers) %>% 
    summarise_all(.funs = sum)
y = train %>% filter(age<110) %>% group_by(ncodpers) %>% summarise(avg_age = round(mean(age,na.rm=TRUE)))
x = x %>% left_join(y,'ncodpers')
x = x %>% replace(., is.na(.),0)
z = x %>% select(avg_age,ncodpers) %>% group_by(avg_age) %>% summarise(count_of_customers = n())
x = x %>% select(-ncodpers) %>% group_by(avg_age) %>% summarise_all(.funs = sum)
x = x %>% mutate(tot_products = rowSums(.[2:25]))
col_names = names(x)[2:25]
for(i in 1:24){
    x[,col_names[i]] = x[,col_names[i]]/x[,'tot_products']
}
summary(x)
x = x %>% select(-tot_products) %>% left_join(z,'avg_age')
y = x %>% gather(key = product,value = pct,ind_ahor_fin_ult1:ind_recibo_ult1)
y$value = y$count_of_customers*y$pct
labels = c('Saving Account','Guarantees','Current Account','Derivada Account','Payroll Account','Junior Account',
           'Mas particular Account','Particular Account', 'Particular Plus Account','Short-term Deposits',
           'Medium-term Deposits','Long-term Deposits','E-account','Funds','Mortgage','Pensions','Loans','Taxes',
           'Credit Card','Securities','Home Account','Payroll','Pensions','Direct Debit')
breaks = names(x)[2:25]
for(i in 1:24){
    y$product[y$product == breaks[i]] = labels[i]
}
top_products = c('Current Account','Direct Debit','Particular Account','Payroll Account','E-account',
                 'Payroll','Taxes','Credit Card','Long-term Deposits','Particular Plus Account')
for(i in 1:24){
    if(!(labels[i] %in% top_products)){
        print(i)
        y$product[y$product == labels[i]] = 'Others'
    }
}
ggplot(y,aes(x=avg_age,y=value,fill=product)) + geom_bar(stat='identity',position="stack") + theme_fivethirtyeight() +
    theme(axis.title = element_text(), axis.title.x = element_text()) + ylab('Count of Customers') + xlab('Age') + 
    ggtitle("Product Distribution with Age") + labs('Products')

z1 = train %>% filter(age<110) %>% group_by(ncodpers) %>% summarise(avg_age = round(mean(age,na.rm=TRUE)))
ggplot(z1,aes(avg_age)) + geom_bar() + stat_count(col='black',fill='red') + ggtitle("Number of persons by Age")+
theme_fivethirtyeight() + theme(axis.title = element_text(), axis.title.x = element_text()) + ylab('Count of Customers') + xlab('Age')

#There are some people with age <18 and with more than 110, we might remove that, lets see how many people are there:
#We have length(train[age<18,unique(ncodpers)]) people with <18 years 
#and length(train[age>=110,unique(ncodpers)]) with more than  110 years

# Consider columns with missing values
sapply(train, function(x) any(is.na(x)))
sapply(test, function(x) any(is.na(x)))
sapply(train, function(x) is.character(x) && any(x==""))
sapply(test, function(x) is.character(x) && any(x==""))

# Missing values in target columns ind_nomina_ult1 and ind_nom_pens_ult1
# Missing test values in indrel_1mes but not in train
# Missing test values in cod_prov and renta (also missing in train)
# Empty test values in sexo, ult_fec_cli_1t, tiprel_1mes,
# conyuemp, canal_entrada, nomprov and segmento (also missing in train)

# Month column
# More train observations in more recent months
# Exactly one date as expected in test with no missing dates
table(train$fecha_dato, useNA = "ifany") 
plot(table(train$fecha_dato, useNA = "ifany"))
table(test$fecha_dato, useNA = "ifany")

# Customer id column analysis
length(unique(train$ncodpers)) # Nearly a million train customers
# Bimodal distribution with most customers present in entire period
table(train$ncodpers, useNA = "ifany")
hist(table(train$ncodpers, useNA = "ifany")) 
# No new customers in the test data
all(test$ncodpers %in% unique(train$ncodpers)) 
