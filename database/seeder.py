from mysql.connector import Error
import os
import csv

PATH_MOCK_DATA = "mock_data.csv"


class SeedDatabase:
    def __init__(self, connection):
        self.connection = connection
        self.read_data_from_csv_and_seed_database()

    def read_data_from_csv_and_seed_database(self):
        try:
            with open(PATH_MOCK_DATA, "r") as file:
                csv_reader = csv.reader(file)
                next(
                    csv_reader
                )  # Omitimos la primera fila que contiene los nombres de las columnas

                data = []
                for row in csv_reader:
                    data.append(
                        (
                            row[1],
                            row[2],
                            row[3],
                            row[4],
                            row[5],
                        )
                    )
                self.insert_data(data)
        except Error as err:
            print("read_data_from_csv_and_seed_database: " + err)

    def insert_data(self, data):
        try:
            cursor = self.connection.cursor()
            cursor.execute("USE CompanyData")
            cursor.executemany(
                """
                INSERT INTO EmployeePerformance(employee_id, department, performance_score, years_with_company, salary)
                VALUES(%s, %s, %s, %s, %s)
                """,
                data,
            )
            self.connection.commit()
            print(cursor.rowcount, "Registros insertados correctamente")
        except Error as err:

            print("insert_data: " + err)
