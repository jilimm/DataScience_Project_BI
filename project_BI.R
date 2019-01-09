# to push everything to tableau
library(DBI)
library(RPostgreSQL)
library(readr)
library(dplyr)
#importing data
# use the sql file to put in the column names????
Customers <- read_delim(file="./ClassicModels-MySQL/datafiles/Customers.txt",
                    delim = ",", col_names = FALSE, col_types = NULL,
                    locale = locale(encoding = "ISO-8859-1")) 
colnames( Customers ) <- c('customerNumber', 'customerName','contactLastName',
                       'contactFirstName','phone','addressLine1',
                       'addressLine2','city','state',
                       'postalCode','country','salesRepEmployeeNumber','creditLimit')
Employees <- read_delim(file="./ClassicModels-MySQL/datafiles/Employees.txt",
                        delim = ",", col_names = FALSE, col_types = NULL,
                        locale = locale(encoding = "ISO-8859-1")) 
colnames( Employees ) <- c('employeeNumber','lastName','firstName'
                           ,'extension','email','officeCode'
                           ,'reportsTo','jobTitle')
Offices <- read_delim(file="./ClassicModels-MySQL/datafiles/Offices.txt",
                        delim = ",", col_names = FALSE, col_types = NULL,
                        locale = locale(encoding = "ISO-8859-1")) 
colnames( Offices ) <- c('officeCode','city','phone'
                           ,'addressLine1','addressLine2','state'
                           ,'country','postalCode','territory')
OrderDetails<- read_delim(file="./ClassicModels-MySQL/datafiles/OrderDetails.txt",
                      delim = ",", col_names = FALSE, col_types = NULL,
                      locale = locale(encoding = "ISO-8859-1")) 
colnames( OrderDetails ) <- c('orderNumber','productCode','quantityOrdered'
                         ,'priceEach','orderLineNumber')
Orders <- read_delim(file="./ClassicModels-MySQL/datafiles/Orders.txt",
                      delim = ",", col_names = FALSE, col_types = NULL,
                      locale = locale(encoding = "ISO-8859-1")) 
colnames( Orders ) <- c('orderNumber','orderDate','requiredDate'
                        ,'shippedDate','status','comments'
                        ,'customerNumber')
Payments <- read_delim(file="./ClassicModels-MySQL/datafiles/Payments.txt",
                      delim = ",", col_names = FALSE, col_types = NULL,
                      locale = locale(encoding = "ISO-8859-1")) 
colnames( Payments ) <- c('customerNumber','checkNumber','paymentDate','amount')
Products <- read_delim(file="./ClassicModels-MySQL/datafiles/Products.txt",
                      delim = ",", col_names = FALSE, col_types = cols(X4 = col_character()),
                      locale = locale(encoding = "ISO-8859-1")) 
colnames( Products ) <- c('productCode','productName','productLine'
                          ,'productScale','productVendor','productDescription'
                          ,'quantityInStock','buyPrice','MSRP')
# write into PostegreSQL
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, port=5432, host = "castle.ewi.utwente.nl", dbname ="dpv1b075", 
                 user = "dpv1b075", password="85+zw6e9", options = "-c search_path=project")

dbWriteTable(con, "Customers", value=Customers, overwrite=T, row.names=F)
dbWriteTable(con, "Employees", value=Employees, overwrite=T, row.names=F)
dbWriteTable(con, "Offices", value=Offices, overwrite=T, row.names=F)
dbWriteTable(con, "OrderDetails", value=OrderDetails, overwrite=T, row.names=F)
dbWriteTable(con, "Orders", value=Orders, overwrite=T, row.names=F)
dbWriteTable(con, "Payments", value=Payments, overwrite=T, row.names=F)
dbWriteTable(con, "Products", value=Products, overwrite=T, row.names=F)
