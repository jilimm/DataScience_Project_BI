# to push everything to tableau
library(DBI)
library(RPostgreSQL)
library(readr)
library(dplyr)
library(lubridate)
library(tidyr) #to replace na values
#importing data
# use the sql file to put in the column names????
Customers_data <- read_delim(file="./BIRTSample/birt-database-2_0_1/ClassicModels/mysql/datafiles/Customers.txt",
                    delim = ",", col_names = FALSE,quote='"',na = c("NULL")) 
cols <- c('customerNumber','customerName','contactLastName','contactFirstName','phone','addressLine1','addressLine2','city','state','postalCode','country','salesRepEmployeeNumber','creditLimit')
colnames(Customers_data) <- cols
Employees_data <- read_delim(file="./BIRTSample/birt-database-2_0_1/ClassicModels/mysql/datafiles/Employees.txt",
                             delim = ",", col_names = FALSE,quote='"',na = c("NULL"))  
cols <- c('employeeNumber','lastName','firstName','extension','email','officeCode','reportsTo','jobTitle')
colnames(Employees_data) <- cols
Offices_data <- read_delim(file="./BIRTSample/birt-database-2_0_1/ClassicModels/mysql/datafiles/Offices.txt",
                           delim = ",", col_names = FALSE,quote='"',na = c("NULL"))  
cols <- c('officeCode','city','phone','addressLine1','addressLine2','state','country','postalCode','territory')
colnames(Offices_data) <- cols
OrderDetails_data <- read_delim(file="./BIRTSample/birt-database-2_0_1/ClassicModels/mysql/datafiles/OrderDetails.txt",
                                delim = ",", col_names = FALSE,quote='"',na = c("NULL"))  
cols<-c('orderNumber','productCode','quantityOrdered','priceEach','orderLineNumber')
colnames(OrderDetails_data)<-cols
Orders_data <- read_delim(file="./BIRTSample/birt-database-2_0_1/ClassicModels/mysql/datafiles/Orders.txt",
                          delim = ",", col_names = FALSE,quote='"',na = c("NULL"))  
cols<-c('orderNumber','orderDate','requiredDate','shippedDate','status','comments','customerNumber')
colnames(Orders_data)<-cols
Payments_data <- read_delim(file="./BIRTSample/birt-database-2_0_1/ClassicModels/mysql/datafiles/Payments.txt",
                            delim = ",", col_names = FALSE,quote='"',na = c("NULL")) 
cols<-c('customerNumber','checkNumber','paymentDate','amount')
colnames(Payments_data)<-cols
Products_data <- read_delim(file="./BIRTSample/birt-database-2_0_1/ClassicModels/mysql/datafiles/Products.txt",
                            delim = ",", col_names = FALSE,quote='"',na = c("NULL"), col_types = cols(X4 = col_character())) 
cols<-c('productCode','productName','productLine','productScale','productVendor','productDescription','quantityInStock','buyPrice','MSRP')
colnames(Products_data)<-cols
ProductLines_data <- read_delim(file="./BIRTSample/birt-database-2_0_1/ClassicModels/mysql/datafiles/ProductLines.txt",
                                delim = ",", col_names = FALSE,quote='"',na = c("NULL")) 
cols<-c('productLine','textDescription')
colnames(ProductLines_data)<-cols

# write into PostegreSQL
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, port=5432, host = "castle.ewi.utwente.nl", dbname ="dpv1b075", 
                 user = "dpv1b075", password="85+zw6e9", options = "-c search_path=project_data")

dbWriteTable(con, "Customers", value=Customers_data, overwrite=T, row.names=F)
dbWriteTable(con, "Employees", value=Employees_data, overwrite=T, row.names=F)
dbWriteTable(con, "Offices", value=Offices_data, overwrite=T, row.names=F)
dbWriteTable(con, "OrderDetails", value=OrderDetails_data, overwrite=T, row.names=F)
dbWriteTable(con, "Orders", value=Orders_data, overwrite=T, row.names=F)
dbWriteTable(con, "Payments", value=Payments_data, overwrite=T, row.names=F)
dbWriteTable(con, "Products", value=Products_data, overwrite=T, row.names=F)

# calculate turnAroundTime and Lateness
## find out payments by using quarters?????
Orders <- Orders_data %>%
  select(orderNumber, orderDate, requiredDate, shippedDate,customerNumber, comments, status)%>%
  left_join(select(Customers_data, 'customerNumber', 'country','salesRepEmployeeNumber')) %>%
  rename(orderStatus = 'status')%>%
  mutate(turnAroundTime = interval(Orders_data$orderDate,Orders_data$shippedDate)/ddays()) %>%
  mutate(Late = ifelse(country == 'Japan',
         ifelse(turnAroundTime>8,'Late','NotLate'),
         ifelse(turnAroundTime>6,'Late','NotLate' ))) %>%
  full_join(OrderDetails_data) %>%
  full_join(select(Products_data, 'productCode', 'productLine','buyPrice')) %>%
  full_join(select(Employees_data, 'employeeNumber', 'reportsTo','officeCode'), 
            by= c("salesRepEmployeeNumber"="employeeNumber")) %>%
  full_join(select(Offices_data,'officeCode', 'territory')) %>%
  full_join(Payments_data)

OrderDetails <- OrderDetails_data %>%
  left_join(select(Products_data, 'productCode', 'buyPrice'))
# fill in buyPrice with average  TODO: check if correct
OrderDetails$buyPrice[is.na(OrderDetails$buyPrice)] <- mean(OrderDetails$buyPrice, na.rm = TRUE)
OrderDetails <- OrderDetails %>%
  mutate(buyPrice = round(buyPrice,2)) %>%
  mutate(Profit = (priceEach - buyPrice)*quantityOrdered) %>%
  select(-c(orderLineNumber)) 

# use lubridate to find quarter starting month of fiscal year is October 
Payments <- Payments_data %>%
  mutate(paymentQuarter = quarter(paymentDate, with_year = TRUE, fiscal_start = 1)) 
