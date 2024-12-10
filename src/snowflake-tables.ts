import type { Connection } from 'snowflake-sdk';
import snowflake from 'snowflake-sdk';
import { config } from 'dotenv';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { writeFile, mkdir, rm } from 'node:fs/promises';
import {
    type SnowflakeConfig,
    type ColumnInfo,
    type TableInfo,
    type DatabaseInfo,
    type WarehouseInfo,
    type SnowflakeWarehouseResult,
    type SnowflakeDatabaseResult,
    type SnowflakeTableResult,
    SnowflakeError
} from '../typings';

// Load environment variables from .env file
const __dirname = fileURLToPath(new URL('.', import.meta.url));
config({ path: join(__dirname, '..', '.env') });

/** Global debug flag controlled by --debug argument */
const DEBUG = process.argv.includes('--debug');

/** Logging utility that only logs when debug mode is enabled */
const logger = {
    debug: (...args: unknown[]): void => {
        if (DEBUG) {
            console.debug(...args);
        }
    },
    info: (...args: unknown[]): void => console.log(...args),
    error: (...args: unknown[]): void => console.error(...args)
};

/**
 * Creates a Snowflake connection with the provided configuration
 * @param config - Snowflake connection configuration
 * @returns Snowflake connection instance
 * @throws {SnowflakeError} If connection creation fails
 */
function createConnection(config: SnowflakeConfig): Connection {
    return snowflake.createConnection({
        account: config.account,
        username: config.username,
        password: config.password,
        warehouse: config.warehouse,
        database: config.database,
        role: config.role || 'SALES'
    });
}

/**
 * Establishes a connection to Snowflake
 * @param connection - Snowflake connection instance
 * @returns Connected Snowflake connection instance
 * @throws {SnowflakeError} If connection fails
 */
async function connectToSnowflake(connection: Connection): Promise<Connection> {
    return new Promise((resolve, reject) => {
        connection.connect((err, conn) => {
            if (err) {
                logger.error(`Unable to connect: ${err.message}`);
                reject(err);
            } else {
                logger.info('Successfully connected to Snowflake!');
                resolve(conn);
            }
        });
    });
}

/**
 * Executes a SQL query on the Snowflake connection
 * @param connection - Active Snowflake connection
 * @param query - SQL query to execute
 * @returns Array of query results
 * @throws {SnowflakeError} If query execution fails
 */
async function executeQuery<T>(connection: Connection, query: string): Promise<readonly T[]> {
    logger.debug('Executing query:', query);
    
    return new Promise((resolve, reject) => {
        connection.execute({
            sqlText: query,
            complete: (err: Error | undefined, _stmt: any, rows: T[] | undefined) => {
                if (err) {
                    reject(new SnowflakeError('Failed to execute query', { query, error: err }));
                    return;
                }
                
                logger.debug(`Query completed successfully. Rows returned: ${rows?.length || 0}`);
                if (rows?.[0]) {
                    logger.debug('Sample row:', rows[0]);
                }
                
                resolve(Object.freeze(rows || []));
            },
            streamResult: false
        });
    });
}

/**
 * Sets the active warehouse for the Snowflake session
 * @param connection - Active Snowflake connection
 * @param warehouse - Name of the warehouse to use
 * @throws {SnowflakeError} If warehouse change fails
 */
async function setWarehouse(connection: Connection, warehouse: string): Promise<void> {
    logger.debug(`Setting active warehouse to: ${warehouse}`);
    await executeQuery(connection, `USE WAREHOUSE "${warehouse}"`);
}

/**
 * Sets the active role for the Snowflake session
 * @param connection - Active Snowflake connection
 * @param role - Name of the role to use
 * @throws {SnowflakeError} If role change fails
 */
async function setRole(connection: Connection, role: string): Promise<void> {
    logger.debug(`Setting active role to: ${role}`);
    await executeQuery(connection, `USE ROLE "${role}"`);
}

/**
 * Retrieves information about all accessible databases
 * @param connection - Active Snowflake connection
 * @returns Array of database information
 * @throws {SnowflakeError} If database retrieval fails
 */
async function getAllDatabases(connection: Connection): Promise<readonly DatabaseInfo[]> {
    logger.debug('Getting all databases...');
    const query = 'SHOW DATABASES';
    const results = await executeQuery<SnowflakeDatabaseResult>(connection, query);
    
    if (results.length > 0) {
        logger.debug('Sample database:', results[0]);
    }
    
    return Object.freeze(results.map((row: SnowflakeDatabaseResult): DatabaseInfo => ({
        name: row.name,
        created: row.created_on,
        origin: row.origin || 'snowflake', // Provide default if origin is not available
        owner: row.owner,
        comment: row.comment,
        isCurrent: row.is_current === 'Y'
    })));
}

/**
 * Retrieves detailed information about all tables in a specific database
 * @param connection - Active Snowflake connection
 * @param database - Name of the database to query
 * @returns Array of table information including columns
 * @throws {SnowflakeError} If table information retrieval fails
 */
async function getTablesForDatabase(connection: Connection, database: string): Promise<readonly TableInfo[]> {
    logger.debug(`Getting tables for database: ${database}`);
    
    // First switch to the database
    await executeQuery(connection, `USE DATABASE "${database}"`);
    
    // Get all schemas using SHOW SCHEMAS instead of INFORMATION_SCHEMA
    const schemasQuery = `SHOW SCHEMAS IN DATABASE "${database}"`;
    const schemasResult = await executeQuery<{ name: string }>(connection, schemasQuery);
    
    // Then get tables for each schema
    const allTables: TableInfo[] = [];
    
    for (const schema of schemasResult) {
        const schemaName = schema.name;
        logger.debug(`Getting tables for schema: ${schemaName}`);
        
        try {
            // Try to use the schema first
            await executeQuery(connection, `USE SCHEMA "${database}"."${schemaName}"`);
            
            const query = `
                SELECT 
                    TABLE_SCHEMA,
                    TABLE_NAME,
                    TABLE_TYPE,
                    ROW_COUNT,
                    BYTES,
                    RETENTION_TIME,
                    CREATED,
                    LAST_ALTERED,
                    COMMENT
                FROM "${database}".INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '${schemaName}'
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            `;
            
            const results = await executeQuery<SnowflakeTableResult>(connection, query);
            
            if (results.length > 0) {
                logger.debug(`Found ${results.length} tables in schema ${schemaName}`);
                logger.debug('Sample table:', results[0]);
            }
            
            for (const row of results) {
                const tableInfo: TableInfo = {
                    database,
                    schema: row.TABLE_SCHEMA,
                    name: row.TABLE_NAME,
                    type: row.TABLE_TYPE,
                    rowCount: row.ROW_COUNT,
                    bytes: row.BYTES,
                    retentionTime: row.RETENTION_TIME,
                    created: row.CREATED,
                    lastAltered: row.LAST_ALTERED,
                    comment: row.COMMENT,
                    columns: []
                };
                allTables.push(tableInfo);
                
                // Write individual table files using the exact case from Snowflake
                const tableFileName = `${row.TABLE_SCHEMA}.${row.TABLE_NAME}.json`;
                const dbPath = database.toLowerCase();
                await writeFormattedOutput(`${dbPath}/${tableFileName}`, tableInfo);
            }
        } catch (error) {
            logger.debug(`Error accessing schema ${schemaName}: ${error}`);
        }
    }
    
    return Object.freeze(allTables);
}

/**
 * Writes formatted JSON data to a file, creating directories if needed
 * @param path - Relative path for the output file
 * @param data - Data to write to the file
 * @throws {Error} If file writing fails
 */
async function writeFormattedOutput(path: string, data: unknown): Promise<void> {
    const outputDir = join(__dirname, '..', 'output');
    await mkdir(outputDir, { recursive: true });
    
    // If path contains slashes, ensure those directories exist
    if (path.includes('/')) {
        const dirPath = join(outputDir, dirname(path));
        await mkdir(dirPath, { recursive: true });
    }
    
    const fullPath = join(outputDir, path);
    await writeFile(fullPath, JSON.stringify(data, null, 2));
    logger.debug(`Written output to ${fullPath}`);
}

/**
 * Safely disconnects from the Snowflake connection
 * @param connection - Active Snowflake connection to close
 * @throws {SnowflakeError} If disconnection fails
 */
async function disconnectFromSnowflake(connection: Connection): Promise<void> {
    return new Promise((resolve, reject) => {
        connection.destroy((err) => {
            if (err) {
                reject(err);
                return;
            }
            
            logger.info('Disconnected from Snowflake');
            
            resolve();
        });
    });
}

/**
 * Lists all accessible warehouses and their details
 * @param connection - Active Snowflake connection
 * @returns Array of warehouse information
 * @throws {SnowflakeError} If warehouse listing fails
 */
async function listWarehouses(connection: Connection): Promise<readonly WarehouseInfo[]> {
    logger.debug('Listing available warehouses...');
    const query = 'SHOW WAREHOUSES';
    const results = await executeQuery<SnowflakeWarehouseResult>(connection, query);
    logger.debug('Query completed successfully. Rows returned:', results.length);
    
    if (results.length > 0) {
        logger.debug('Sample row:', results[0]);
    }
    
    const warehouses = results.map((row: SnowflakeWarehouseResult): WarehouseInfo => ({
        name: row.name,
        state: row.state,
        type: row.type,
        size: row.size,
        isDefault: row.is_default === 'Y',
        isCurrent: row.is_current === 'Y',
        owner: row.owner,
        comment: row.comment
    }));
    
    logger.debug('Available warehouses:', warehouses);
    return Object.freeze(warehouses);
}

/**
 * Retrieves comprehensive information about tables across databases
 * @param config - Snowflake configuration (database is optional)
 * @returns Array of table information
 * @throws {SnowflakeError} If table information retrieval fails
 * @remarks If no database is specified, retrieves tables from all accessible databases
 */
async function getSnowflakeTables(
    config: Omit<SnowflakeConfig, 'database'> & { database?: string }
): Promise<readonly TableInfo[]> {
    logger.info('Starting Snowflake table extraction...');
    
    // Purge output directory before starting
    const outputDir = join(__dirname, '..', 'output');
    try {
        await rm(outputDir, { recursive: true, force: true });
        logger.debug('Purged output directory');
    } catch (error) {
        logger.debug('Error purging output directory:', error);
    }
    await mkdir(outputDir, { recursive: true });
    
    logger.debug('Creating Snowflake connection with config:', {
        account: config.account,
        username: config.username,
        warehouse: 'COMPUTE_WH',
        database: config.database || 'SNOWFLAKE',
        role: config.role || 'SALES'
    });
    
    const connection = createConnection({
        ...config,
        warehouse: 'COMPUTE_WH',
        database: config.database || 'SNOWFLAKE'
    } as SnowflakeConfig);
    
    try {
        await connectToSnowflake(connection);
        
        // Set role first before any other operations
        await setRole(connection, config.role || 'SALES');
        
        const warehouses = await listWarehouses(connection);
        await writeFormattedOutput('warehouses.json', warehouses);
        
        await setWarehouse(connection, 'COMPUTE_WH');
        
        const tables: TableInfo[] = [];
        
        if (!config.database) {
            const databases = await getAllDatabases(connection);
            await writeFormattedOutput('databases.json', databases);
            
            await Promise.all(databases.map(async (database) => {
                try {
                    const dbTables = await getTablesForDatabase(connection, database.name);
                    tables.push(...dbTables);
                    
                    const dbDir = database.name.toLowerCase();
                    
                    await writeFormattedOutput(
                        join(dbDir, 'metadata.json'),
                        {
                            name: database.name,
                            created: database.created,
                            owner: database.owner,
                            comment: database.comment
                        }
                    );
                    
                    await Promise.all(dbTables.map(table => 
                        writeFormattedOutput(
                            join(dbDir, `${table.schema.toLowerCase()}.${table.name.toLowerCase()}.json`),
                            table
                        )
                    ));
                } catch (err) {
                    logger.error(
                        new SnowflakeError(
                            `Error getting tables for database ${database.name}`,
                            err
                        )
                    );
                }
            }));
        } else {
            const dbTables = await getTablesForDatabase(connection, config.database);
            tables.push(...dbTables);
            
            const dbDir = config.database.toLowerCase();
            
            await Promise.all(dbTables.map(table => 
                writeFormattedOutput(
                    join(dbDir, `${table.schema.toLowerCase()}.${table.name.toLowerCase()}.json`),
                    table
                )
            ));
        }
        
        return Object.freeze(tables);
    } catch (error) {
        throw new SnowflakeError('Error in getSnowflakeTables', error);
    } finally {
        await disconnectFromSnowflake(connection);
    }
};

/**
 * Main entry point for the Snowflake table information retrieval
 * @throws {Error} If environment variables are missing or if any operation fails
 */
async function main(): Promise<void> {
    logger.info('Starting Snowflake table extraction...');
    
    const requiredEnvVars = [
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_USERNAME',
        'SNOWFLAKE_PASSWORD'
    ] as const;
    
    const missingEnvVars = requiredEnvVars.filter(varName => !process.env[varName]);
    
    if (missingEnvVars.length > 0) {
        logger.error('Missing required environment variables:', missingEnvVars.join(', '));
        process.exit(1);
    }
    
    try {
        const tables = await getSnowflakeTables({
            account: process.env.SNOWFLAKE_ACCOUNT!,
            username: process.env.SNOWFLAKE_USERNAME!,
            password: process.env.SNOWFLAKE_PASSWORD!,
            warehouse: process.env.SNOWFLAKE_WAREHOUSE || 'COMPUTE_WH',
            database: process.env.SNOWFLAKE_DATABASE,
            role: process.env.SNOWFLAKE_ROLE || 'SALES'
        });
        
        logger.info('Successfully fetched tables:', tables.length);
        process.exit(0);
    } catch (error) {
        logger.error('Error:', error);
        process.exit(1);
    }
};

// Run main if this is the entry point
if (import.meta.url === `file://${process.argv[1]}`) {
    main();
}

export {
    getSnowflakeTables,
    type SnowflakeConfig,
    type DatabaseInfo,
    type TableInfo,
    type ColumnInfo,
    type WarehouseInfo
};
