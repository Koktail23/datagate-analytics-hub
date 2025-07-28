ATTACH TABLE _ UUID '34ad3ed6-1303-4fbf-a618-37842b025932'
(
    `rule_id` UUID DEFAULT generateUUIDv4(),
    `rule_name` String,
    `rule_type` String,
    `column_pattern` String,
    `validation_logic` String,
    `is_active` UInt8,
    `created_at` DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (rule_type, created_at)
SETTINGS index_granularity = 8192
