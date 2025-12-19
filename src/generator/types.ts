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

export interface GenerateOptions {
  table: TableConfig;
  rowCount: number;
  createTable?: boolean;
  truncateFirst?: boolean;
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
}
