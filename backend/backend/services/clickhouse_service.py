from clickhouse_driver import Client
import pandas as pd
from typing import List, Dict, Any, Optional
import os
from dotenv import load_dotenv
import uuid
from datetime import datetime

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
        self.init_database()

    def init_database(self):
        """Создать базу данных если не существует."""
        try:
            self.client.execute(f"CREATE DATABASE IF NOT EXISTS {os.getenv('CLICKHOUSE_DB', 'datagate')}")
            self.create_tables()
        except Exception as e:
            print(f"Error initializing database: {e}")

    def create_tables(self):
        """Создать необходимые таблицы."""
        queries = [
            """
            CREATE TABLE IF NOT EXISTS datagate.data_quality_checks (
                check_id UUID DEFAULT generateUUIDv4(),
                dataset_name String,
                table_name String,
                column_name String,
                check_type String,
                check_status String,
                error_count UInt32,
                total_count UInt32,
                error_percentage Float32,
                details String,
                created_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (created_at, dataset_name, table_name)
            """,
            """
            CREATE TABLE IF NOT EXISTS datagate.uploaded_datasets (
                dataset_id UUID DEFAULT generateUUIDv4(),
                dataset_name String,
                file_path String,
                columns Array(String),
                row_count UInt32,
                file_size UInt64,
                upload_status String,
                created_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY created_at
            """,
            """
            CREATE TABLE IF NOT EXISTS datagate.validation_rules (
                rule_id UUID DEFAULT generateUUIDv4(),
                rule_name String,
                rule_type String,
                column_pattern String,
                validation_logic String,
                is_active UInt8,
                created_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (rule_type, created_at)
            """
        ]

        for query in queries:
            try:
                self.client.execute(query)
                print(f"Table created/verified successfully")
            except Exception as e:
                print(f"Error creating table: {e}")

    def execute_query(self, query: str, params: Dict = None) -> List[Dict[str, Any]]:
        """Выполнить запрос и вернуть результат."""
        try:
            result = self.client.execute(query, params or {})
            if result and isinstance(result, list):
                return result
            return []
        except Exception as e:
            print(f"Error executing query: {e}")
            return []

    def insert_quality_check(self, check_data: Dict[str, Any]) -> bool:
        """Вставить результат проверки качества."""
        try:
            query = """
                INSERT INTO datagate.data_quality_checks 
                (dataset_name, table_name, column_name, check_type, check_status, 
                 error_count, total_count, error_percentage, details) 
                VALUES
            """
            self.client.execute(query + " (%(dataset_name)s, %(table_name)s, %(column_name)s, "
                                        "%(check_type)s, %(check_status)s, %(error_count)s, %(total_count)s, "
                                        "%(error_percentage)s, %(details)s)", check_data)
            return True
        except Exception as e:
            print(f"Error inserting quality check: {e}")
            return False

    def get_recent_checks(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Получить последние проверки качества."""
        query = """
            SELECT 
                check_id,
                dataset_name,
                table_name,
                column_name,
                check_type,
                check_status,
                error_count,
                total_count,
                error_percentage,
                details,
                toString(created_at) as created_at
            FROM datagate.data_quality_checks
            ORDER BY created_at DESC
            LIMIT %(limit)s
        """
        return self.execute_query(query, {'limit': limit})


# Singleton экземпляр
clickhouse_service = ClickHouseService()