install.packages('RPostgreSQL')
install.packages('DbDrivers')
dsn_database = "postgres"

dsn_hostname = "localhost"  
dsn_port = "5432"                # Specify your port number. e.g. 98939
dsn_uid = "postgres"         # Specify your username. e.g. "admin"
dsn_pwd = "password"        # Specify your password. e.g. "xxx"

tryCatch({  
    drv <- RPostgreSQL::postgresqlInitDriver()
    print("Connecting to Database…")
    connec <- RPostgreSQL::dbConnect(drv, 
                 dbname = dsn_database,
                 host = dsn_hostname, 
                 port = dsn_port,
                 user = dsn_uid, 
                 password = dsn_pwd)
    print("Database Connected!")
    },
    error=function(cond) {
            print("Unable to connect to Database.")
    })

dbSendQuery(connec, "DROP TABLE IF EXISTS Employees")
dbSendQuery(connec, "CREATE TABLE Employees(Id INTEGER PRIMARY KEY, Name VARCHAR(20))")
dbSendQuery(connec, "INSERT INTO Employees VALUES(1,'Aakash')")
dbSendQuery(connec, "INSERT INTO Employees VALUES(2,'Diksha')")
dbSendQuery(connec, "INSERT INTO Employees VALUES(3,'Jaskaran')")
dbSendQuery(connec, "INSERT INTO Employees VALUES(4,'Arsalan')")
dbSendQuery(connec, "INSERT INTO Employees VALUES(5,'Argha')")
dbSendQuery(connec, "INSERT INTO Employees VALUES(6,'Anuj')")
dbSendQuery(connec, "INSERT INTO Employees VALUES(7,'Noor')")
dbSendQuery(connec, "INSERT INTO Employees VALUES(8,'Anirudh')")

df <- dbGetQuery(connec, "SELECT * FROM Employees")
df