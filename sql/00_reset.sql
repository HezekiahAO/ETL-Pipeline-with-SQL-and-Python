-- Drops everything the pipeline owns. Demo / test use only.
DROP SCHEMA IF EXISTS analysis    CASCADE;
DROP SCHEMA IF EXISTS dw          CASCADE;
DROP SCHEMA IF EXISTS transformed CASCADE;
DROP SCHEMA IF EXISTS cleaned     CASCADE;
DROP SCHEMA IF EXISTS staging     CASCADE;
DROP SCHEMA IF EXISTS raw         CASCADE;
DROP SCHEMA IF EXISTS etl         CASCADE;
