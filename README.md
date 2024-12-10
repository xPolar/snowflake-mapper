# snowflake-mapper

a typescript utility to fetch and analyze snowflake database schemas and tables

## what does it do?

this tool connects to your snowflake instance and:
- fetches information about all databases
- retrieves detailed table schemas
- collects warehouse information
- generates structured json output with comprehensive metadata

## prerequisites

- node.js (v16 or higher)
- pnpm
- snowflake account with appropriate access permissions

## setup

1. clone the repository
2. install dependencies:
```bash
pnpm install
```

3. create a `.env` file in the root directory with your snowflake credentials:
```env
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USERNAME=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=your-warehouse
SNOWFLAKE_ROLE=your-role  # optional, defaults to SALES
SNOWFLAKE_DATABASE=your-database  # optional
```

## usage

build and run the project:
```bash
pnpm build
pnpm start
```

for debug output, add the `--debug` flag:
```bash
pnpm start -- --debug
```

## type definitions

all typescript interfaces and types are located in `typings/index.d.ts`. the main types include:

### snowflake config
configuration for connecting to snowflake:
```typescript
interface SnowflakeConfig {
    account: string;    // snowflake account identifier
    username: string;   // snowflake username
    password: string;   // snowflake password
    warehouse: string;  // warehouse to use
    database?: string;  // optional database
    role?: string;     // optional role (defaults to SALES)
}
```

### table info
detailed table metadata:
```typescript
interface TableInfo {
    database: string;
    schema: string;
    name: string;
    type: string;
    rowCount: number;
    bytes: number;
    retentionTime: number;
    created: string;
    lastAltered: string;
    comment: string | null;
    columns: readonly ColumnInfo[];
}
```

for complete type definitions, see `typings/index.d.ts`.

## output format

the tool generates json files in the `output` directory:
- `databases.json`: list of all accessible databases
- `warehouses.json`: list of all available warehouses
- `[database]/metadata.json`: metadata for each database
- `[database]/tables.json`: detailed table information for each database

## project structure

```
.
├── src/
│   └── snowflake-tables.ts  # main implementation
├── typings/
│   └── index.d.ts          # type definitions
├── biome.json             # code style configuration
├── package.json
└── tsconfig.json
```

## debugging

- check the `snowflake.log` file for detailed execution logs
- use the `--debug` flag for verbose output
- ensure your snowflake credentials are correct in the `.env` file
- verify your network connection to snowflake

## dependencies

- `snowflake-sdk`: official snowflake node.js driver
- `dotenv`: environment variable management
- `typescript`: for type safety and better development experience
- `biome`: for code formatting and linting

## license

mit
