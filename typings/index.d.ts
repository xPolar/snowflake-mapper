import type { Connection } from 'snowflake-sdk';

/** Configuration for Snowflake connection */
export interface SnowflakeConfig {
    /** Snowflake account identifier (e.g., 'your-account.snowflakecomputing.com') */
    account: string;
    /** Snowflake username for authentication */
    username: string;
    /** Snowflake password for authentication */
    password: string;
    /** Warehouse to use for query execution */
    warehouse: string;
    /** Optional database to connect to */
    database?: string;
    /** Role to use for executing queries (defaults to SALES) */
    role?: string;
}

/** Column metadata information */
export interface ColumnInfo {
    /** Name of the column */
    columnName: string;
    /** Snowflake data type of the column */
    dataType: string;
    /** Whether the column accepts NULL values ('YES' or 'NO') */
    isNullable: string;
    /** Maximum length for character data types */
    characterMaximumLength: number | null;
    /** Precision for numeric data types */
    numericPrecision: number | null;
    /** Scale for numeric data types */
    numericScale: number | null;
}

/** Table metadata information */
export interface TableInfo {
    /** Database name */
    database: string;
    /** Schema name */
    schema: string;
    /** Table name */
    name: string;
    /** Table type (e.g., 'TABLE', 'VIEW') */
    type: string;
    /** Number of rows in the table */
    rowCount: number;
    /** Size of the table in bytes */
    bytes: number;
    /** Time travel retention period in days */
    retentionTime: number;
    /** Creation timestamp */
    created: string;
    /** Last modification timestamp */
    lastAltered: string;
    /** Optional table comment */
    comment: string | null;
    /** Array of column information */
    columns: readonly ColumnInfo[];
}

/** Database metadata information */
export interface DatabaseInfo {
    /** Database name */
    name: string;
    /** Creation timestamp */
    created: string;
    /** Origin of the database */
    origin: string;
    /** Database owner */
    owner: string;
    /** Optional database comment */
    comment: string | null;
    /** Whether this is the current active database */
    isCurrent: boolean;
}

/** Warehouse metadata information */
export interface WarehouseInfo {
    /** Warehouse name */
    name: string;
    /** Current state of the warehouse */
    state: string;
    /** Warehouse type */
    type: string;
    /** Size of the warehouse */
    size: string;
    /** Whether this is the default warehouse */
    isDefault: boolean;
    /** Whether this is the current active warehouse */
    isCurrent: boolean;
    /** Warehouse owner */
    owner: string;
    /** Optional warehouse comment */
    comment: string | null;
}

/** Raw Snowflake warehouse query result */
export interface SnowflakeWarehouseResult {
    name: string;
    state: string;
    type: string;
    size: string;
    is_default: string;
    is_current: string;
    owner: string;
    comment: string | null;
}

/** Raw Snowflake database query result */
export interface SnowflakeDatabaseResult {
    name: string;
    created_on: string;
    owner: string;
    comment: string | null;
    is_current: string;
    is_default: string;
    origin: string;
}

/** Raw Snowflake table query result */
export interface SnowflakeTableResult {
    TABLE_SCHEMA: string;
    TABLE_NAME: string;
    TABLE_TYPE: string;
    ROW_COUNT: number;
    BYTES: number;
    RETENTION_TIME: number;
    CREATED: string;
    LAST_ALTERED: string;
    COMMENT: string | null;
}

/** Custom error for Snowflake-specific errors */
export class SnowflakeError extends Error {
    constructor(message: string, public readonly details?: unknown) {
        super(message);
        this.name = 'SnowflakeError';
    }
}
