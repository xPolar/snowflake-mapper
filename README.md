# snowflake mapper

a rust tool to fetch and map snowflake database schemas. this tool connects to your snowflake instance and generates detailed json files containing information about your databases, tables, and columns.

## features

- asynchronous snowflake operations using tokio
- functional programming approach with immutable data structures
- strong typing with rust's type system
- json output for each database
- environment variable configuration
- detailed error handling and logging

## prerequisites

- rust (latest stable version)
- snowflake account with appropriate permissions

## configuration

create a `.env` file in the project root with the following variables:

```env
snowflake_account=your_account
snowflake_username=your_username
snowflake_password=your_password
snowflake_warehouse=your_warehouse
snowflake_database=optional_specific_database
snowflake_role=optional_role_defaults_to_sales
```

## building

```bash
cargo build --release
```

## running

```bash
cargo run --release
```

the tool will:

1. connect to your snowflake instance
2. fetch database information
3. for each database, fetch table and column information
4. generate json files in the `output` directory

## output format

the tool generates json files with the following structure:

```json
[
  {
    "database_name": "string",
    "schema_name": "string",
    "table_name": "string",
    "columns": [
      {
        "name": "string",
        "data_type": "string",
        "is_nullable": boolean,
        "character_maximum_length": number | null,
        "numeric_precision": number | null,
        "numeric_scale": number | null
      }
    ]
  }
]
```

## error handling

the tool uses the `anyhow` crate for error handling and provides detailed error messages. all errors are properly propagated and logged using the `tracing` crate.

## development

this project follows rust best practices:

- strong typing with clear struct definitions
- trait-based abstractions for snowflake operations
- async/await for efficient i/o operations
- proper error handling and propagation
- functional programming patterns where appropriate
