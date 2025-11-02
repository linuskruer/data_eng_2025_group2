ATTACH TABLE _ UUID '92139244-d6fd-4a9f-91c3-1675d2a8b772'
(
    `test` String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192
