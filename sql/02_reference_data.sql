-- psql-only (uses \copy). The Python pipeline loads the same CSVs itself.
-- Run from the project root.
TRUNCATE etl.country_map, etl.status_map, etl.channel_map;
\copy etl.country_map FROM 'reference/country_map.csv' WITH (FORMAT csv, HEADER true)
\copy etl.status_map  FROM 'reference/status_map.csv'  WITH (FORMAT csv, HEADER true)
\copy etl.channel_map FROM 'reference/channel_map.csv' WITH (FORMAT csv, HEADER true)
