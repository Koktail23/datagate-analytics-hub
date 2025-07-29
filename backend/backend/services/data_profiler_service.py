# backend/backend/services/data_profiler_service.py
from typing import Dict, List, Any, Optional, Tuple
import clickhouse_connect
from clickhouse_connect.driver import Client
import pandas as pd
import numpy as np
from datetime import datetime
import re
from collections import Counter


class DataProfilerService:
    """Сервис для профилирования данных и анализа датасетов"""

    def __init__(self):
        self.client: Optional[Client] = None
        self._connect()

    def _connect(self):
        """Подключение к ClickHouse"""
        try:
            # Используем подключение без пароля для пользователя default
            self.client = clickhouse_connect.get_client(
                host='localhost',
                port=8123,
                username='default',
                password='',
                database='datagate'
            )
        except Exception as e:
            print(f"Ошибка подключения к ClickHouse: {e}")

    def get_tables_list(self) -> List[Dict[str, str]]:
        """Получить список всех таблиц в базе"""
        query = """
        SELECT 
            database,
            name as table_name,
            total_rows,
            total_bytes,
            formatReadableSize(total_bytes) as size_readable
        FROM system.tables
        WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
        ORDER BY database, name
        """
        try:
            result = self.client.query(query)
            return [
                {
                    'database': row[0],
                    'table_name': row[1],
                    'total_rows': row[2],
                    'total_bytes': row[3],
                    'size_readable': row[4]
                }
                for row in result.result_rows
            ]
        except Exception as e:
            print(f"Ошибка получения списка таблиц: {e}")
            return []

    def profile_table(self, database: str, table_name: str, sample_size: int = 10000) -> Dict[str, Any]:
        """Полное профилирование таблицы"""
        try:
            # Получаем информацию о структуре таблицы
            table_info = self._get_table_structure(database, table_name)

            # Получаем общую статистику
            general_stats = self._get_general_stats(database, table_name)

            # Получаем детальную статистику по колонкам
            column_stats = self._get_column_stats(database, table_name, sample_size)

            # Определяем типы данных и паттерны
            data_patterns = self._analyze_data_patterns(database, table_name, sample_size)

            return {
                'table_info': table_info,
                'general_stats': general_stats,
                'column_stats': column_stats,
                'data_patterns': data_patterns,
                'profiled_at': datetime.now().isoformat()
            }
        except Exception as e:
            print(f"Ошибка профилирования таблицы: {e}")
            return {'error': str(e)}

    def _get_table_structure(self, database: str, table_name: str) -> Dict[str, Any]:
        """Получить структуру таблицы"""
        query = f"""
        SELECT 
            name,
            type,
            default_type,
            default_expression,
            comment
        FROM system.columns
        WHERE database = '{database}' AND table = '{table_name}'
        ORDER BY position
        """
        result = self.client.query(query)

        return {
            'columns': [
                {
                    'name': row[0],
                    'type': row[1],
                    'default_type': row[2],
                    'default_expression': row[3],
                    'comment': row[4]
                }
                for row in result.result_rows
            ]
        }

    def _get_general_stats(self, database: str, table_name: str) -> Dict[str, Any]:
        """Получить общую статистику по таблице"""
        # Количество строк
        count_query = f"SELECT count() FROM {database}.{table_name}"
        row_count = self.client.query(count_query).result_rows[0][0]

        # Размер таблицы
        size_query = f"""
        SELECT 
            sum(bytes_on_disk) as total_bytes,
            formatReadableSize(sum(bytes_on_disk)) as size_readable
        FROM system.parts
        WHERE database = '{database}' AND table = '{table_name}' AND active
        """
        size_result = self.client.query(size_query).result_rows[0]

        # Количество колонок
        columns_query = f"""
        SELECT count() 
        FROM system.columns 
        WHERE database = '{database}' AND table = '{table_name}'
        """
        column_count = self.client.query(columns_query).result_rows[0][0]

        return {
            'row_count': row_count,
            'column_count': column_count,
            'total_bytes': size_result[0] if size_result[0] else 0,
            'size_readable': size_result[1] if size_result[1] else '0 B'
        }

    def _get_column_stats(self, database: str, table_name: str, sample_size: int) -> List[Dict[str, Any]]:
        """Получить статистику по каждой колонке"""
        # Получаем список колонок
        columns_query = f"""
        SELECT name, type 
        FROM system.columns 
        WHERE database = '{database}' AND table = '{table_name}'
        ORDER BY position
        """
        columns = self.client.query(columns_query).result_rows

        column_stats = []
        for col_name, col_type in columns:
            stats = self._analyze_column(database, table_name, col_name, col_type, sample_size)
            column_stats.append(stats)

        return column_stats

    def _analyze_column(self, database: str, table_name: str, col_name: str, col_type: str, sample_size: int) -> Dict[
        str, Any]:
        """Анализ отдельной колонки"""
        stats = {
            'column_name': col_name,
            'data_type': col_type,
            'inferred_type': self._infer_data_type(col_type)
        }

        # Базовые метрики
        try:
            # NULL значения
            null_query = f"""
            SELECT 
                countIf(isNull({col_name})) as null_count,
                count() as total_count
            FROM {database}.{table_name}
            """
            null_result = self.client.query(null_query).result_rows[0]
            stats['null_count'] = null_result[0]
            stats['null_percentage'] = round((null_result[0] / null_result[1] * 100) if null_result[1] > 0 else 0, 2)

            # Уникальные значения
            unique_query = f"""
            SELECT 
                uniqExact({col_name}) as unique_count,
                count() as total_count
            FROM {database}.{table_name}
            WHERE {col_name} IS NOT NULL
            """
            unique_result = self.client.query(unique_query).result_rows[0]
            stats['unique_count'] = unique_result[0]
            stats['unique_percentage'] = round(
                (unique_result[0] / unique_result[1] * 100) if unique_result[1] > 0 else 0, 2)

            # Для числовых типов
            if self._is_numeric_type(col_type):
                numeric_stats = self._get_numeric_stats(database, table_name, col_name)
                stats.update(numeric_stats)

            # Для строковых типов
            elif self._is_string_type(col_type):
                string_stats = self._get_string_stats(database, table_name, col_name, sample_size)
                stats.update(string_stats)

            # Для дат
            elif self._is_date_type(col_type):
                date_stats = self._get_date_stats(database, table_name, col_name)
                stats.update(date_stats)

            # Топ значения
            top_values = self._get_top_values(database, table_name, col_name, 10)
            stats['top_values'] = top_values

        except Exception as e:
            stats['error'] = str(e)

        return stats

    def _is_numeric_type(self, col_type: str) -> bool:
        """Проверка, является ли тип числовым"""
        numeric_types = ['Int', 'UInt', 'Float', 'Decimal']
        return any(t in col_type for t in numeric_types)

    def _is_string_type(self, col_type: str) -> bool:
        """Проверка, является ли тип строковым"""
        return 'String' in col_type or 'FixedString' in col_type

    def _is_date_type(self, col_type: str) -> bool:
        """Проверка, является ли тип датой"""
        return 'Date' in col_type

    def _infer_data_type(self, col_type: str) -> str:
        """Определение категории типа данных"""
        if self._is_numeric_type(col_type):
            return 'numeric'
        elif self._is_string_type(col_type):
            return 'string'
        elif self._is_date_type(col_type):
            return 'date'
        elif 'Bool' in col_type:
            return 'boolean'
        elif 'Array' in col_type:
            return 'array'
        else:
            return 'other'

    def _get_numeric_stats(self, database: str, table_name: str, col_name: str) -> Dict[str, Any]:
        """Статистика для числовых колонок"""
        query = f"""
        SELECT 
            min({col_name}) as min_val,
            max({col_name}) as max_val,
            avg({col_name}) as avg_val,
            median({col_name}) as median_val,
            quantile(0.25)({col_name}) as q1,
            quantile(0.75)({col_name}) as q3,
            stddevPop({col_name}) as std_dev,
            varPop({col_name}) as variance
        FROM {database}.{table_name}
        WHERE {col_name} IS NOT NULL
        """
        result = self.client.query(query).result_rows[0]

        return {
            'min': float(result[0]) if result[0] is not None else None,
            'max': float(result[1]) if result[1] is not None else None,
            'mean': float(result[2]) if result[2] is not None else None,
            'median': float(result[3]) if result[3] is not None else None,
            'q1': float(result[4]) if result[4] is not None else None,
            'q3': float(result[5]) if result[5] is not None else None,
            'std_dev': float(result[6]) if result[6] is not None else None,
            'variance': float(result[7]) if result[7] is not None else None
        }

    def _get_string_stats(self, database: str, table_name: str, col_name: str, sample_size: int) -> Dict[str, Any]:
        """Статистика для строковых колонок"""
        # Длина строк
        length_query = f"""
        SELECT 
            min(length({col_name})) as min_length,
            max(length({col_name})) as max_length,
            avg(length({col_name})) as avg_length
        FROM {database}.{table_name}
        WHERE {col_name} IS NOT NULL
        """
        length_result = self.client.query(length_query).result_rows[0]

        # Паттерны (на сэмпле)
        sample_query = f"""
        SELECT {col_name}
        FROM {database}.{table_name}
        WHERE {col_name} IS NOT NULL
        LIMIT {sample_size}
        """
        samples = [row[0] for row in self.client.query(sample_query).result_rows]

        # Определяем паттерны
        patterns = self._detect_string_patterns(samples)

        return {
            'min_length': int(length_result[0]) if length_result[0] is not None else None,
            'max_length': int(length_result[1]) if length_result[1] is not None else None,
            'avg_length': float(length_result[2]) if length_result[2] is not None else None,
            'patterns': patterns
        }

    def _get_date_stats(self, database: str, table_name: str, col_name: str) -> Dict[str, Any]:
        """Статистика для дат"""
        query = f"""
        SELECT 
            min({col_name}) as min_date,
            max({col_name}) as max_date,
            dateDiff('day', min({col_name}), max({col_name})) as range_days
        FROM {database}.{table_name}
        WHERE {col_name} IS NOT NULL
        """
        result = self.client.query(query).result_rows[0]

        return {
            'min_date': str(result[0]) if result[0] else None,
            'max_date': str(result[1]) if result[1] else None,
            'range_days': int(result[2]) if result[2] is not None else None
        }

    def _get_top_values(self, database: str, table_name: str, col_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Получить топ значений колонки"""
        query = f"""
        SELECT 
            {col_name} as value,
            count() as count,
            count() * 100.0 / sum(count()) OVER () as percentage
        FROM {database}.{table_name}
        GROUP BY {col_name}
        ORDER BY count DESC
        LIMIT {limit}
        """

        try:
            result = self.client.query(query).result_rows
            return [
                {
                    'value': str(row[0]) if row[0] is not None else 'NULL',
                    'count': row[1],
                    'percentage': round(float(row[2]), 2)
                }
                for row in result
            ]
        except:
            return []

    def _detect_string_patterns(self, samples: List[str]) -> Dict[str, Any]:
        """Определение паттернов в строковых данных"""
        if not samples:
            return {}

        patterns = {
            'email': 0,
            'phone': 0,
            'url': 0,
            'numeric': 0,
            'alphanumeric': 0,
            'uuid': 0
        }

        # Регулярные выражения для паттернов
        email_pattern = re.compile(r'^[\w\.-]+@[\w\.-]+\.\w+$')
        phone_pattern = re.compile(r'^[\+\d\s\-\(\)]+$')
        url_pattern = re.compile(r'^https?://|www\.')
        numeric_pattern = re.compile(r'^\d+$')
        alphanumeric_pattern = re.compile(r'^[a-zA-Z0-9]+$')
        uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)

        for sample in samples[:1000]:  # Проверяем первые 1000 значений
            if email_pattern.match(sample):
                patterns['email'] += 1
            elif phone_pattern.match(sample) and len(sample) >= 7:
                patterns['phone'] += 1
            elif url_pattern.match(sample):
                patterns['url'] += 1
            elif uuid_pattern.match(sample):
                patterns['uuid'] += 1
            elif numeric_pattern.match(sample):
                patterns['numeric'] += 1
            elif alphanumeric_pattern.match(sample):
                patterns['alphanumeric'] += 1

        # Конвертируем в проценты
        total = len(samples[:1000])
        for key in patterns:
            patterns[key] = round((patterns[key] / total * 100), 2) if total > 0 else 0

        # Оставляем только значимые паттерны (> 10%)
        return {k: v for k, v in patterns.items() if v > 10}

    def _analyze_data_patterns(self, database: str, table_name: str, sample_size: int) -> Dict[str, Any]:
        """Анализ паттернов данных в таблице"""
        # Получаем сэмпл данных
        sample_query = f"""
        SELECT *
        FROM {database}.{table_name}
        LIMIT {sample_size}
        """

        try:
            result = self.client.query(sample_query)
            if not result.result_rows:
                return {}

            # Анализируем корреляции для числовых колонок
            numeric_columns = [
                col for col, dtype in zip(result.column_names, result.column_types)
                if self._is_numeric_type(dtype)
            ]

            correlations = {}
            if len(numeric_columns) > 1:
                # Простая корреляция между парами числовых колонок
                for i, col1 in enumerate(numeric_columns):
                    for col2 in numeric_columns[i + 1:]:
                        corr_query = f"""
                        SELECT corr({col1}, {col2}) as correlation
                        FROM {database}.{table_name}
                        WHERE {col1} IS NOT NULL AND {col2} IS NOT NULL
                        """
                        try:
                            corr_result = self.client.query(corr_query).result_rows[0][0]
                            if corr_result is not None and abs(corr_result) > 0.5:
                                correlations[f"{col1}_vs_{col2}"] = round(float(corr_result), 3)
                        except:
                            pass

            return {
                'high_correlations': correlations,
                'numeric_columns_count': len(numeric_columns)
            }
        except Exception as e:
            return {'error': str(e)}

    def get_column_distribution(self, database: str, table_name: str, column_name: str, bins: int = 20) -> Dict[
        str, Any]:
        """Получить распределение значений для визуализации"""
        # Проверяем тип колонки
        type_query = f"""
        SELECT type 
        FROM system.columns 
        WHERE database = '{database}' AND table = '{table_name}' AND name = '{column_name}'
        """
        col_type = self.client.query(type_query).result_rows[0][0]

        if self._is_numeric_type(col_type):
            return self._get_numeric_distribution(database, table_name, column_name, bins)
        else:
            return self._get_categorical_distribution(database, table_name, column_name, bins)

    def _get_numeric_distribution(self, database: str, table_name: str, column_name: str, bins: int) -> Dict[str, Any]:
        """Распределение для числовых данных"""
        # Получаем min/max для определения интервалов
        range_query = f"""
        SELECT 
            min({column_name}) as min_val,
            max({column_name}) as max_val
        FROM {database}.{table_name}
        WHERE {column_name} IS NOT NULL
        """
        range_result = self.client.query(range_query).result_rows[0]
        min_val, max_val = float(range_result[0]), float(range_result[1])

        if min_val == max_val:
            return {
                'type': 'numeric',
                'bins': [str(min_val)],
                'counts': [1],
                'percentages': [100.0]
            }

        # Создаем гистограмму
        bin_width = (max_val - min_val) / bins
        histogram_query = f"""
        SELECT 
            floor(({column_name} - {min_val}) / {bin_width}) as bin_index,
            count() as count
        FROM {database}.{table_name}
        WHERE {column_name} IS NOT NULL
        GROUP BY bin_index
        ORDER BY bin_index
        """

        histogram_result = self.client.query(histogram_query).result_rows

        # Формируем данные для визуализации
        bin_labels = []
        counts = []

        for i in range(bins):
            bin_start = min_val + i * bin_width
            bin_end = bin_start + bin_width
            bin_labels.append(f"{bin_start:.2f}-{bin_end:.2f}")

            # Находим count для этого бина
            count = 0
            for row in histogram_result:
                if row[0] == i:
                    count = row[1]
                    break
            counts.append(count)

        total = sum(counts)
        percentages = [round(c / total * 100, 2) if total > 0 else 0 for c in counts]

        return {
            'type': 'numeric',
            'bins': bin_labels,
            'counts': counts,
            'percentages': percentages
        }

    def _get_categorical_distribution(self, database: str, table_name: str, column_name: str, limit: int) -> Dict[
        str, Any]:
        """Распределение для категориальных данных"""
        query = f"""
        SELECT 
            {column_name} as value,
            count() as count
        FROM {database}.{table_name}
        GROUP BY value
        ORDER BY count DESC
        LIMIT {limit}
        """

        result = self.client.query(query).result_rows

        values = []
        counts = []

        for row in result:
            values.append(str(row[0]) if row[0] is not None else 'NULL')
            counts.append(row[1])

        total = sum(counts)
        percentages = [round(c / total * 100, 2) if total > 0 else 0 for c in counts]

        return {
            'type': 'categorical',
            'values': values,
            'counts': counts,
            'percentages': percentages
        }