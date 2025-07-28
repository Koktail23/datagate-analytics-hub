ATTACH TABLE _ UUID '7f5ef628-5b36-42bf-a90a-eaae3a526ce4'
(
    `dataset_id` UUID DEFAULT generateUUIDv4(),
    `dataset_name` String,
    `file_path` String,
    `columns` Array(String),
    `row_count` UInt32,
    `file_size` UInt64,
    `upload_status` String,
    `created_at` DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY created_at
SETTINGS index_granularity = 8192
