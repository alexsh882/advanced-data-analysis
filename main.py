import pandas as pd

from database.config import DatabaseConnection
from database.seeder import SeedDatabase
from services.employee_service import EmployeeService


def main():

    connection = DatabaseConnection().get_connection()

    SeedDatabase(connection)

    employee_service = EmployeeService(connection)

    data = employee_service.get_employee_performance()
    df = pd.DataFrame(data)

    # Media, mediana y desviación estándar del performance_score.
    print("\n")
    print("Media, mediana y desviación estándar del performance_score: ")
    print(employee_service.statistics(df, 3))

    # Media, mediana y desviación estándar del salary.
    print("\n")
    print("Media, mediana y desviación estándar del salary: ")
    print(employee_service.statistics(df, 5))

    # Número total de empleados por departamento.
    print("\n")
    print("Número total de empleados por departamento: ")
    print(employee_service.employee_quantity_per_department(df))

    # Correlación entre years_with_company y performance_score.
    print("\n")
    print("Correlación entre years_with_company y performance_score: ")
    print(employee_service.correlation(df, 4, 3))

    # Correlación entre salary y performance_score.
    print("\n")
    print("Correlación entre salary y performance_score: ")
    print(df[[5, 3]].corr(numeric_only=True))
    print(employee_service.correlation(df, 5, 3))

    # Histograma del performance_score.
    employee_service.histogram_performance_score_by_department(df)

    # Gráfica de dispersión de years_with_company vs performance_score.
    employee_service.dispersion_graph_years_with_company_vs_performance_score(df)

    # Gráfica de dispersión de salary vs performance_score.
    employee_service.dispersion_graph_salary_vs_performance_score(df)

    connection.close()


if __name__ == "__main__":
    main()
