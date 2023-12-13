import pandas as pd
import duckdb
import os
import pyarrow.parquet as pq


def create_duckdb_table():
    file_path = './data/Electric_Vehicle_Population_Data.csv'
    df = pd.read_csv(file_path, nrows=1)
    column_types = df.dtypes.to_dict()

    ddl = "CREATE TABLE electric_vehicles ("
    for column, dtype in column_types.items():
        column_name = column.replace(' ', '_').replace('(', '_').replace(')', '_')

        duckdb_type = None
        if 'int' in str(dtype) and column_name != '2020_Census_Tract':
            duckdb_type = "INTEGER"
        elif 'float' in str(dtype):
            duckdb_type = "DECIMAL"
        else:
            duckdb_type = f"VARCHAR({df[column].apply(lambda x: len(str(x))).max()})"

        ddl += f'"{column_name}" {duckdb_type}, '

    ddl = ddl[:-2] + ")"

    connection = duckdb.connect(database=':memory:')
    cursor = connection.cursor()
    cursor.execute(ddl)

    return connection, file_path


def import_data_to_duckdb(connection, file_path):
    cursor = connection.cursor()
    cursor.execute(f"COPY electric_vehicles FROM '{file_path}' (DELIMITER ',', HEADER)")


def analyze_data(connection):
    task_queries = [
        "SELECT City, Model_Year, COUNT(*) as count FROM electric_vehicles GROUP BY City, Model_Year",
        "SELECT Model, Model_Year, COUNT(*) as count FROM electric_vehicles GROUP BY Model, Model_Year ORDER BY count DESC LIMIT 3",
        "SELECT Postal_Code, Model, Model_Year, COUNT(*) as count FROM electric_vehicles GROUP BY Postal_Code, Model, Model_Year ORDER BY count DESC",
        "SELECT Model_Year, COUNT(*) as count FROM electric_vehicles GROUP BY Model_Year"
    ]

    results = []
    for i, query in enumerate(task_queries):
        result = pd.read_sql_query(query, connection)
        result['Analysis_Type'] = f"result{i+1}"
        results.append(result)

    return pd.concat(results, ignore_index=True)


def write_results_to_parquet(results):
    output_dir = './output'
    os.makedirs(output_dir, exist_ok=True)

    for year in results['Model_Year'].unique():
        year_results = results[results['Model_Year'] == year]

        for analysis_type in results['Analysis_Type'].unique():
            analysis_results = year_results[year_results['Analysis_Type'] == analysis_type]
            output_file = os.path.join(output_dir, f"{analysis_type}_{year}.parquet")
            analysis_results.to_parquet(output_file, index=False)


def read_and_output_parquet_files():
    parquet_dir = './output'
    parquet_files = [f for f in os.listdir(parquet_dir) if f.endswith('.parquet')]

    for parquet_file in parquet_files:
        file_path = os.path.join(parquet_dir, parquet_file)
        table = pq.read_table(file_path)
        df = table.to_pandas()
        print(f"\nContents of {parquet_file}:\n{df}")


def main():
    connection, file_path = create_duckdb_table()
    import_data_to_duckdb(connection, file_path)
    analysis_results = analyze_data(connection)
    write_results_to_parquet(analysis_results)
    read_and_output_parquet_files()


if __name__ == "__main__":
    main()
