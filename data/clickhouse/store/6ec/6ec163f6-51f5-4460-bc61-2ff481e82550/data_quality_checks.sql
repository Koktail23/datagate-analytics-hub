ATTACH TABLE _ UUID 'f7cf8f5f-5a73-4e8a-9a4f-a31426b6a372'
(
    `check_id` UUID DEFAULT generateUUIDv4(),
    `dataset_name` String,
    `table_name` String,
    `column_name` String,
    `check_type` String,
    `check_status` String,
    `error_count` UInt32,
    `total_count` UInt32,
    `error_percentage` Float32,
    `details` String,
    `created_at` DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (created_at, dataset_name, table_name)
SETTINGS index_granularity = 8192
