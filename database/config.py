import mysql.connector as mysql
from mysql.connector import Error
from mysql.connector import errorcode


DB_CONFIG = {
    "user": "root",
    "password": "",
    "host": "localhost",
    "raise_on_warnings": True,
}

class DatabaseConnection:
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.host = DB_CONFIG["host"]
        self.user = DB_CONFIG["user"]
        self.password = DB_CONFIG["password"]
        self.database = "CompanyData"
        self.raise_on_warnings = DB_CONFIG["raise_on_warnings"]
        self.get_connection()
        self.initDatabase()


    def initDatabase(self):
        self.create_database()
        self.create_table()
        
    def close_connection(self):
        if self.connection:
            self.connection.close()
            print("Conexión cerrada")
        else:
            print("No hay conexión para cerrar")

    
    
    def get_connection(self):
        
        if self.connection:
            return self.connection
        
        try:
            self.connection = mysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                raise_on_warnings=self.raise_on_warnings,
            )
            print("Conexión a la base de datos exitosa")
            return self.connection
        except Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Credenciales incorrectas")
            else:
                print(err)

  

    def create_database(self):

        try:
            self.cursor = self.connection.cursor()

            self.cursor.execute(
                """
                    DROP DATABASE IF EXISTS CompanyData;
                """
            )
            self.cursor.execute(
                """
                    CREATE DATABASE CompanyData;
                """
            )
            print("Base de datos creada correctamente")
        except Error as err:
            print(err)
        

    def create_table(self):
        cursor = self.connection.cursor()
        try:
            cursor.execute("USE CompanyData;")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS EmployeePerformance(
                    id int AUTO_INCREMENT PRIMARY KEY,
                    employee_id int,
                    department varchar(255),
                    performance_score double,
                    years_with_company int,
                    salary double
                )
                """
            )
            print("Tabla creada correctamente")
        except Error as err:
            cursor.close()
            self.connection.close()

            print(err)