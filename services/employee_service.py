from mysql.connector import Error
import matplotlib.pyplot as plt
import pandas as pd


class EmployeeService:
    def __init__(self, connection):
        self.connection = connection

    def get_employee_performance(self):
        try:
            cursor = self.connection.cursor()
            cursor.execute("USE CompanyData")
            cursor.execute(
                """
                SELECT *
                FROM EmployeePerformance
                """
            )
            return cursor.fetchall()
        except Error as err:
            print("get_employee_performance: " + err)

    def statistics(self, df, column):

        data = pd.DataFrame()

        data["Media"] = df.groupby([2])[column].mean()
        data["Mediana"] = df.groupby([2])[column].median()
        data["DE"] = df.groupby([2])[column].std()
        return data

    def employee_quantity_per_department(self, df):
        return df.groupby([2])[1].count()

    def histogram_performance_score_by_department(self, df):
        df.hist(column=3, by=2, bins=10, figsize=(10, 10))
        plt.suptitle("Histograma de performance_score por departamento")
        plt.show()

    def dispersion_graph_years_with_company_vs_performance_score(self, df):
        df.plot.scatter(x=4, y=3)
        plt.title("Gráfica de dispersión de years_with_company vs performance_score")
        plt.show()

    def dispersion_graph_salary_vs_performance_score(self, df):
        df.plot.scatter(x=5, y=3)
        plt.title("Gráfica de dispersión de salary vs performance_score")
        plt.show()

    def correlation(self, df, column1, column2):
        return df[[column1, column2]].corr(numeric_only=True)
