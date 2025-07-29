-- Создание тестовой таблицы для Data Profiler
-- Выполните этот скрипт в ClickHouse для создания тестовых данных

-- Создаем таблицу с различными типами данных
CREATE TABLE IF NOT EXISTS datagate.test_user_analytics
(
    user_id UInt64,
    username String,
    email String,
    phone String,
    age UInt8,
    registration_date Date,
    last_login DateTime,
    account_balance Decimal(10, 2),
    is_active UInt8,
    city String,
    country String,
    user_score Float32,
    tags Array(String),
    profile_completion UInt8,
    subscription_type Enum('free' = 1, 'basic' = 2, 'premium' = 3, 'enterprise' = 4),
    utm_source String,
    device_type String,
    session_duration UInt32,
    page_views UInt32
)
ENGINE = MergeTree()
ORDER BY (registration_date, user_id);

-- Вставляем тестовые данные с различными паттернами
INSERT INTO datagate.test_user_analytics
SELECT
    number AS user_id,
    concat('user_', toString(number)) AS username,
    CASE 
        WHEN rand() % 100 < 70 THEN concat('user', toString(number), '@example.com')
        WHEN rand() % 100 < 85 THEN concat('test', toString(number), '@gmail.com')
        WHEN rand() % 100 < 95 THEN concat('demo', toString(number), '@company.io')
        ELSE NULL
    END AS email,
    CASE 
        WHEN rand() % 100 < 60 THEN concat('+1-555-', toString(1000 + rand() % 9000), '-', toString(1000 + rand() % 9000))
        WHEN rand() % 100 < 80 THEN concat('+44-20-', toString(10000000 + rand() % 90000000))
        ELSE NULL
    END AS phone,
    18 + rand() % 52 AS age,
    today() - (rand() % 730) AS registration_date,
    now() - (rand() % 86400) AS last_login,
    round(rand() * 10000, 2) AS account_balance,
    rand() % 100 < 85 AS is_active,
    arrayElement(['New York', 'London', 'Paris', 'Tokyo', 'Berlin', 'Moscow', 'Sydney', 'Toronto', 'Dubai', 'Singapore'], 1 + rand() % 10) AS city,
    arrayElement(['USA', 'UK', 'France', 'Japan', 'Germany', 'Russia', 'Australia', 'Canada', 'UAE', 'Singapore'], 1 + rand() % 10) AS country,
    round(rand() * 100, 2) AS user_score,
    arrayFilter(x -> rand() % 2 = 0, ['tech', 'business', 'travel', 'food', 'sports', 'music', 'gaming']) AS tags,
    CASE 
        WHEN rand() % 100 < 20 THEN 100
        WHEN rand() % 100 < 50 THEN 80 + rand() % 20
        WHEN rand() % 100 < 80 THEN 60 + rand() % 20
        ELSE 40 + rand() % 20
    END AS profile_completion,
    arrayElement(['free', 'basic', 'premium', 'enterprise'], 1 + rand() % 4) AS subscription_type,
    CASE 
        WHEN rand() % 100 < 30 THEN 'google'
        WHEN rand() % 100 < 50 THEN 'facebook'
        WHEN rand() % 100 < 70 THEN 'organic'
        WHEN rand() % 100 < 85 THEN 'email'
        ELSE NULL
    END AS utm_source,
    arrayElement(['desktop', 'mobile', 'tablet'], 1 + rand() % 3) AS device_type,
    rand() % 3600 AS session_duration,
    1 + rand() % 50 AS page_views
FROM numbers(10000);

-- Добавим некоторые аномалии и интересные паттерны
INSERT INTO datagate.test_user_analytics
SELECT
    1000000 + number AS user_id,
    concat('premium_user_', toString(number)) AS username,
    concat('premium', toString(number), '@vip.com') AS email,
    concat('+1-800-VIP-', toString(1000 + number)) AS phone,
    35 + rand() % 20 AS age,
    today() - (rand() % 90) AS registration_date,
    now() - (rand() % 3600) AS last_login,
    50000 + round(rand() * 50000, 2) AS account_balance,
    1 AS is_active,
    'New York' AS city,
    'USA' AS country,
    90 + round(rand() * 10, 2) AS user_score,
    ['premium', 'vip', 'exclusive'] AS tags,
    100 AS profile_completion,
    'enterprise' AS subscription_type,
    'direct' AS utm_source,
    'desktop' AS device_type,
    3000 + rand() % 3000 AS session_duration,
    50 + rand() % 100 AS page_views
FROM numbers(100);

-- Создадим еще одну таблицу с транзакциями для демонстрации
CREATE TABLE IF NOT EXISTS datagate.test_transactions
(
    transaction_id UInt64,
    user_id UInt64,
    transaction_date DateTime,
    amount Decimal(10, 2),
    currency FixedString(3),
    status Enum('pending' = 1, 'completed' = 2, 'failed' = 3, 'refunded' = 4),
    payment_method String,
    merchant_name String,
    category String,
    fraud_score Float32
)
ENGINE = MergeTree()
ORDER BY (transaction_date, transaction_id);

-- Заполняем транзакции
INSERT INTO datagate.test_transactions
SELECT
    number AS transaction_id,
    rand() % 10100 AS user_id,
    now() - (rand() % (86400 * 30)) AS transaction_date,
    round(10 + rand() * 990, 2) AS amount,
    arrayElement(['USD', 'EUR', 'GBP', 'JPY'], 1 + rand() % 4) AS currency,
    arrayElement(['pending', 'completed', 'failed', 'refunded'], 1 + rand() % 4) AS status,
    arrayElement(['credit_card', 'debit_card', 'paypal', 'bank_transfer', 'crypto'], 1 + rand() % 5) AS payment_method,
    arrayElement(['Amazon', 'Walmart', 'Apple', 'Google', 'Netflix', 'Spotify', 'Uber', 'Airbnb'], 1 + rand() % 8) AS merchant_name,
    arrayElement(['shopping', 'entertainment', 'transport', 'food', 'utilities', 'travel'], 1 + rand() % 6) AS category,
    round(rand() * 100, 2) AS fraud_score
FROM numbers(50000);

-- Создадим таблицу с продуктовыми метриками
CREATE TABLE IF NOT EXISTS datagate.test_product_metrics
(
    date Date,
    product_id UInt32,
    product_name String,
    category String,
    views UInt64,
    clicks UInt64,
    conversions UInt64,
    revenue Decimal(10, 2),
    avg_rating Float32,
    reviews_count UInt32,
    return_rate Float32,
    stock_level Int32
)
ENGINE = MergeTree()
ORDER BY (date, product_id);

-- Заполняем продуктовые метрики
INSERT INTO datagate.test_product_metrics
SELECT
    today() - (number % 30) AS date,
    (number % 1000) + 1 AS product_id,
    concat('Product_', toString((number % 1000) + 1)) AS product_name,
    arrayElement(['Electronics', 'Clothing', 'Home', 'Sports', 'Books', 'Toys'], 1 + (number % 6)) AS category,
    100 + rand() % 10000 AS views,
    10 + rand() % 1000 AS clicks,
    1 + rand() % 100 AS conversions,
    round((1 + rand() % 100) * 9.99, 2) AS revenue,
    round(3 + rand() * 2, 1) AS avg_rating,
    rand() % 500 AS reviews_count,
    round(rand() * 0.2, 3) AS return_rate,
    CASE 
        WHEN rand() % 100 < 10 THEN -10 + rand() % 10
        ELSE rand() % 1000
    END AS stock_level
FROM numbers(30000);

-- Проверяем результаты
SELECT 'test_user_analytics' as table_name, count() as row_count FROM datagate.test_user_analytics
UNION ALL
SELECT 'test_transactions' as table_name, count() as row_count FROM datagate.test_transactions
UNION ALL
SELECT 'test_product_metrics' as table_name, count() as row_count FROM datagate.test_product_metrics;