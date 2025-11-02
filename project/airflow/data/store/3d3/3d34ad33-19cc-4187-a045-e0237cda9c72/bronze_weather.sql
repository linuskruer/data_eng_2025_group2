ATTACH TABLE _ UUID '42541ea9-4bf4-4cdc-ae19-57088512e838'
(
    `time` DateTime,
    `weather_code` UInt16,
    `temperature_2m` Float32,
    `relative_humidity_2m` Float32,
    `cloudcover` Float32,
    `rain` Float32,
    `sunshine_duration` Float32,
    `windspeed_10m` Float32,
    `city` String,
    `postal_prefix` String,
    `file_version` String,
    `ingestion_timestamp` DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (time, city)
SETTINGS index_granularity = 8192
