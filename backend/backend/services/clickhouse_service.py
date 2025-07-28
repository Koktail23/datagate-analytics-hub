from clickhouse_driver import Client
import pandas as pd
from typing import List, Dict, Any
import os
from dotenv import load_dotenv

load_dotenv()


class ClickHouseService:
    def __init__(self):
        self.client = Client(
            host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
            port=int(os.getenv('CLICKHOUSE_PORT', 9000)),
            user=os.getenv('CLICKHOUSE_USER', 'admin'),
            password=os.getenv('CLICKHOUSE_PASSWORD', 'admin123'),
            database=os.getenv('CLICKHOUSE_DB', 'datagate'),
        )

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Выполнить запрос и вернуть результат."""
        try:
            result = self.client.execute(query, with_column_types=True)
            if result:
                columns = [col[0] for col in result[1]]
                data = result[0]
                return [dict(zip(columns, row)) for row in data]
            return []
        except Exception as e:
            print(f"Error executing query: {e}")
            return []

    def insert_dataframe(self, df: pd.DataFrame, table_name: str):
        """Вставить DataFrame в таблицу ClickHouse."""
        try:
            self.client.insert_dataframe(f'INSERT INTO {table_name} VALUES', df)
            return True
        except Exception as e:
            print(f"Error inserting data: {e}")
            return False

    def create_tables(self):
        """Создать необходимые таблицы."""
        queries = [
            """
            CREATE TABLE IF NOT EXISTS data_quality_checks (
                check_id UUID DEFAULT generateUUIDv4(),
                table_name String,
                column_name String,
                check_type String,
                check_result String,
                details String,
                created_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (created_at, table_name)
            """,
            """
            CREATE TABLE IF NOT EXISTS kpi_metrics (
                metric_id UUID DEFAULT generateUUIDv4(),
                metric_name String,
                metric_value Float64,
                metric_date Date,
                category String,
                created_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (metric_date, metric_name)
            """
        ]

        for query in queries:
            try:
                self.client.execute(query)
                print(f"Table created successfully")
            except Exception as e:
                print(f"Error creating table: {e}")


# Singleton экземпляр
clickhouse_service = ClickHouseService()