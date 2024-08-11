import mysql.connector as mysql
from mysql.connector import Error
from mysql.connector import errorcode

import pandas as pd
import matplotlib.pyplot as plt

import os
import csv

DB_CONFIG = {
    "user": "root",
    "password": "",
    "host": "localhost",
    "raise_on_warnings": True,
}

PATH_MOCK_DATA = "mock_data.csv"


def get_connection():
    connection = None
    try:
        connection = mysql.connect(**DB_CONFIG)
        print("Conexión a la base de datos exitosa")
        return connection
    except Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Credenciales incorrectas")
        else:
            print(err)
    else:
        connection.close()


def create_database(connection):
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
                DROP DATABASE IF EXISTS CompanyData;
            """
        )
        cursor.execute(
            """
                CREATE DATABASE CompanyData;
            """
        )
        print("Base de datos creada correctamente")
    except Error as err:
        print(err)


def create_table(connection):
    cursor = connection.cursor()
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
        connection.close()

        print(err)


def insert_data(connection, data):
    try:
        cursor = connection.cursor()
        cursor.execute("USE CompanyData")
        cursor.executemany(
            """
            INSERT INTO EmployeePerformance(employee_id, department, performance_score, years_with_company, salary)
            VALUES(%s, %s, %s, %s, %s)
            """,
            data,
        )
        connection.commit()
        print(cursor.rowcount, "Registros insertados correctamente")
    except Error as err:
        cursor.close()
        connection.close()
        print('insert_data: ' + err)


def read_data_from_csv_and_seed_database(connection):
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
            insert_data(connection, data)
    except Error as err:
        print('read_data_from_csv_and_seed_database: ' + err)


def get_employee_performance(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("USE CompanyData")
        cursor.execute(
            """
            SELECT *
            FROM EmployeePerformance
            """
        )
        return cursor.fetchall()
    except Error as err:
        print('get_employee_performance: ' + err)


def statistics(df, column):

    data = pd.DataFrame()
    
    data['Media'] = df.groupby([2])[column].mean()
    data['Mediana'] = df.groupby([2])[column].median()
    data['DE'] = df.groupby([2])[column].std()
    return data

def employee_quantity_per_department(df):
    return df.groupby([2])[1].count()

def main():
    connection = get_connection()
    create_database(connection)
    create_table(connection)
    read_data_from_csv_and_seed_database(connection)

    
    connection.close()

if __name__ == "__main__":
    main()
