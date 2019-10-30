# SnowflakeSupplyCollector
A supply collector designed to connect to Snowflake

## Build
Run `dotnet build`

## Tests
Run `gitlab-runner exec docker test`

Requires following environment variables set:

*  SNOWFLAKE_ACCOUNT
*  SNOWFLAKE_REGION
*  SNOWFLAKE_DB
*  SNOWFLAKE_USER
*  SNOWFLAKE_PASS

Database must be created. Use data loader to fill data for unit tests.

## Known issues

Extended attributes - primary key, foreign key, unique index - not supported at the moment.