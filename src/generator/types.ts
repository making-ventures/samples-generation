// Column generator types
export type ColumnType =
  | "integer"
  | "bigint"
  | "float"
  | "string"
  | "boolean"
  | "datetime"
  | "date";

export interface SequenceGenerator {
  kind: "sequence";
  start?: number;
  step?: number;
}

export interface RandomIntGenerator {
  kind: "randomInt";
  min: number;
  max: number;
}

export interface RandomFloatGenerator {
  kind: "randomFloat";
  min: number;
  max: number;
  precision?: number;
}

export interface RandomStringGenerator {
  kind: "randomString";
  length: number;
}

export interface ChoiceGenerator<T = unknown> {
  kind: "choice";
  values: T[];
}

export interface ConstantGenerator<T = unknown> {
  kind: "constant";
  value: T;
}

export interface DatetimeGenerator {
  kind: "datetime";
  from?: Date;
  to?: Date;
}

export interface UuidGenerator {
  kind: "uuid";
}

export interface ChoiceByLookupGenerator {
  kind: "choiceByLookup";
  /** Array of values to choose from (will be stored in a lookup table) */
  values: string[];
}

export type GeneratorConfig =
  | SequenceGenerator
  | RandomIntGenerator
  | RandomFloatGenerator
  | RandomStringGenerator
  | ChoiceGenerator
  | ConstantGenerator
  | DatetimeGenerator
  | UuidGenerator
  | ChoiceByLookupGenerator;

// Post-generation transformation types

/**
 * Template transformation - construct a string from other columns
 * Use {column_name} to reference other columns
 */
export interface TemplateTransformation {
  kind: "template";
  /** Column to update */
  column: string;
  /** Template string, e.g., "{first_name}.{last_name}@example.com" */
  template: string;
  /** If true, convert to lowercase (default: false) */
  lowercase?: boolean;
}

/**
 * Mutation operation types
 */
export type MutationOperation = "replace" | "delete" | "insert";

/**
 * Mutate transformation - randomly modify characters in a string
 */
export interface MutateTransformation {
  kind: "mutate";
  /** Column to mutate */
  column: string;
  /** Probability of mutation (0-1) */
  probability: number;
  /** Operations to apply randomly */
  operations: MutationOperation[];
}

/**
 * Lookup transformation - set column value from another table via join.
 *
 * **Execution order note (ClickHouse)**: Due to ClickHouse limitations with
 * correlated subqueries in ALTER TABLE UPDATE, lookup transformations are
 * applied using a table swap approach and execute **before** template/mutate
 * transformations in the same batch. If transformation order matters, place
 * lookups in a separate postTransformations batch.
 */
export interface LookupTransformation {
  kind: "lookup";
  /** Column to update */
  column: string;
  /** Lookup table name */
  fromTable: string;
  /** Column in lookup table to get value from */
  fromColumn: string;
  /** Join condition */
  joinOn: {
    /** Column in target table to match */
    targetColumn: string;
    /** Column in lookup table to match */
    lookupColumn: string;
  };
}

/**
 * Swap transformation - swap values of two columns with probability.
 * Useful for simulating data entry errors where values are entered in wrong fields.
 */
export interface SwapTransformation {
  kind: "swap";
  /** First column to swap */
  column1: string;
  /** Second column to swap */
  column2: string;
  /** Probability of swap (0-1) */
  probability: number;
}

export type Transformation =
  | TemplateTransformation
  | MutateTransformation
  | LookupTransformation
  | SwapTransformation;

/**
 * A batch of transformations with optional description.
 */
export interface TransformationBatch {
  /** Optional description for logging and debugging */
  description?: string;
  /** Transformations to apply in this batch */
  transformations: Transformation[];
}

export interface ColumnConfig {
  name: string;
  type: ColumnType;
  generator: GeneratorConfig;
  nullable?: boolean;
  /**
   * Probability of NULL values for this column (0-1).
   * Requires nullable: true to take effect.
   */
  nullProbability?: number;
}

export interface TableConfig {
  name: string;
  /** Optional description for logging */
  description?: string;
  columns: ColumnConfig[];
}

/**
 * Common options shared between generate() and runScenario()
 */
export interface CommonGenerateOptions {
  /** Create tables if not exists (default: true) */
  createTable?: boolean;
  /** Truncate tables before generating (default: false) */
  truncateFirst?: boolean;
  /** Drop tables before generating (default: false) */
  dropFirst?: boolean;
  /**
   * If true, queries the table for max values of sequence columns
   * and continues from there. Default: true.
   */
  resumeSequences?: boolean;
  /**
   * If true, runs database-specific optimization after insert
   * (VACUUM, OPTIMIZE TABLE, etc.). Default: true.
   */
  optimize?: boolean;
}

export interface GenerateOptions extends CommonGenerateOptions {
  table: TableConfig;
  rowCount: number;
}

export type GeneratedRow = Record<string, unknown>;

export interface GenerateResult {
  rowsInserted: number;
  /** Total duration including optimization */
  durationMs: number;
  /** Duration of data generation only */
  generateMs: number;
  /** Duration of optimization (0 if skipped) */
  optimizeMs: number;
}

export interface TransformResult {
  /** Total duration of transformations */
  durationMs: number;
  /** Number of batches applied */
  batchesApplied: number;
}

/**
 * A single step in a scenario.
 *
 * Steps can be:
 * 1. **Generate + Transform**: Provide `table`, `rowCount`, and optionally `transformations`
 * 2. **Transform only**: Provide `tableName` and `transformations` (table must already exist)
 */
export type ScenarioStep = ScenarioGenerateStep | ScenarioTransformStep;

/**
 * A step that generates data into a table and optionally transforms it.
 */
export interface ScenarioGenerateStep {
  /** Table configuration (schema + generators) */
  table: TableConfig;
  /** Number of rows to generate for this table */
  rowCount: number;
  /** Optional transformations to apply after generation */
  transformations?: TransformationBatch[];
}

/**
 * A step that only applies transformations to an existing table.
 * Useful for cross-table lookups or applying transformations after all tables are generated.
 */
export interface ScenarioTransformStep {
  /** Name of the existing table to transform */
  tableName: string;
  /** Transformations to apply */
  transformations: TransformationBatch[];
}

/**
 * A complete scenario with multiple tables and transformations.
 * Use with runScenario() for a single-call multi-table data generation workflow.
 *
 * @example
 * const scenario: Scenario = {
 *   name: "E-commerce data",
 *   steps: [
 *     // Generate tables first
 *     { table: usersTable, rowCount: 1000 },
 *     { table: productsTable, rowCount: 500 },
 *     { table: ordersTable, rowCount: 5000 },
 *     // Then apply cross-table lookups
 *     {
 *       tableName: "orders",
 *       transformations: [
 *         { transformations: [{ kind: "lookup", column: "user_name", fromTable: "users", ... }] }
 *       ]
 *     },
 *   ],
 * };
 */
export interface Scenario {
  /** Optional name for logging */
  name?: string;
  /** Optional description */
  description?: string;
  /** Steps to execute in order (each step = one table) */
  steps: ScenarioStep[];
}

/**
 * Options for running a scenario
 */
export interface ScenarioOptions extends CommonGenerateOptions {
  /** The scenario to run */
  scenario: Scenario;
}

/**
 * Result of a single step in a scenario
 */
export interface ScenarioStepResult {
  /** Table name */
  tableName: string;
  /** Generation result (undefined for transform-only steps) */
  generate?: GenerateResult;
  /** Transformation result (if transformations were defined) */
  transform?: TransformResult;
}

/**
 * Result of running a complete scenario
 */
export interface ScenarioResult {
  /** Results for each step */
  steps: ScenarioStepResult[];
  /** Total rows inserted across all tables */
  totalRowsInserted: number;
  /** Total duration of the entire scenario */
  durationMs: number;
}

// Main interface that all database implementations must follow
export interface DataGenerator {
  readonly name: string;

  /**
   * Connect to the database
   */
  connect(): Promise<void>;

  /**
   * Disconnect from the database
   */
  disconnect(): Promise<void>;

  /**
   * Create a table based on the configuration
   */
  createTable(table: TableConfig): Promise<void>;

  /**
   * Truncate/clear a table
   */
  truncateTable(tableName: string): Promise<void>;

  /**
   * Drop a table if it exists
   */
  dropTable(tableName: string): Promise<void>;

  /**
   * Generate and insert rows into the database
   */
  generate(options: GenerateOptions): Promise<GenerateResult>;

  /**
   * Apply transformations to an existing table
   */
  transform(
    tableName: string,
    batches: TransformationBatch[]
  ): Promise<TransformResult>;

  /**
   * Query rows from a table (for verification)
   */
  queryRows(tableName: string, limit?: number): Promise<GeneratedRow[]>;

  /**
   * Count rows in a table
   */
  countRows(tableName: string): Promise<number>;

  /**
   * Get the maximum value of a column (for resuming sequences)
   */
  getMaxValue(tableName: string, columnName: string): Promise<number | null>;

  /**
   * Get the total size of a table in bytes (including indexes if applicable)
   */
  getTableSize(tableName: string): Promise<number | null>;

  /**
   * Get the total size of a table as a human-readable string
   */
  getTableSizeForHuman(tableName: string): Promise<string | null>;

  /**
   * Run database-specific optimization after large inserts.
   * - PostgreSQL: VACUUM ANALYZE
   * - ClickHouse: OPTIMIZE TABLE FINAL
   * - SQLite: VACUUM + ANALYZE
   * - Trino/Iceberg: rewrite_data_files, expire_snapshots
   */
  optimize(tableName: string): Promise<void>;

  /**
   * Run a complete scenario: generate data and apply transformations.
   * Combines generate() + transform() into a single call.
   */
  runScenario(options: ScenarioOptions): Promise<ScenarioResult>;
}
