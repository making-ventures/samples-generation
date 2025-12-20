import { parseArgs } from "node:util";
import {
  PostgresDataGenerator,
  ClickHouseDataGenerator,
  SQLiteDataGenerator,
  TrinoDataGenerator,
  formatDuration,
  type DataGenerator,
  type Scenario,
} from "../src/generator/index.js";

// Usage:
//   npx tsx scripts/generate-all.ts
//   npx tsx scripts/generate-all.ts --rows 1000
//   npx tsx scripts/generate-all.ts -r 1000 --postgres
//   npx tsx scripts/generate-all.ts --clickhouse --trino
//   npx tsx scripts/generate-all.ts --scenario english-names
//   npx tsx scripts/generate-all.ts --help

const SCENARIO_NAMES = ["simple", "english-names", "russian-names"] as const;
type ScenarioName = (typeof SCENARIO_NAMES)[number];

const { values } = parseArgs({
  options: {
    rows: { type: "string", short: "r", default: "1000" },
    scenario: { type: "string", short: "s", default: "simple" },
    sqlite: { type: "boolean", default: false },
    postgres: { type: "boolean", default: false },
    clickhouse: { type: "boolean", default: false },
    trino: { type: "boolean", default: false },
    help: { type: "boolean", short: "h", default: false },
  },
});

if (values.help) {
  console.log(`
Usage: npx tsx scripts/generate-all.ts [options]

Options:
  -r, --rows <count>     Number of rows to generate (default: 1000, supports 1_000_000 format)
  -s, --scenario <name>  Scenario to run: ${SCENARIO_NAMES.join(", ")} (default: simple)
  --sqlite               Generate for SQLite only
  --postgres             Generate for PostgreSQL only
  --clickhouse           Generate for ClickHouse only
  --trino                Generate for Trino only
  -h, --help             Show this help message

Scenarios:
  simple         Random strings and values (5 columns)
  english-names  English first/last names with email template (7 columns)
  russian-names  Russian first/last names with email template (7 columns)

If no database is specified, all databases are generated.

Examples:
  npx tsx scripts/generate-all.ts --rows 1000
  npx tsx scripts/generate-all.ts -r 10000 --postgres
  npx tsx scripts/generate-all.ts --scenario english-names --clickhouse
  npx tsx scripts/generate-all.ts -s russian-names --trino
`);
  process.exit(0);
}

const ROW_COUNT = parseInt(values.rows.replace(/_/g, ""), 10);
const SCENARIO = values.scenario as ScenarioName;

if (!SCENARIO_NAMES.includes(SCENARIO)) {
  console.error(
    `Invalid scenario: ${SCENARIO}. Valid options: ${SCENARIO_NAMES.join(", ")}`
  );
  process.exit(1);
}

// English names dataset
const ENGLISH_FIRST_NAMES = [
  "James",
  "Mary",
  "Robert",
  "Patricia",
  "John",
  "Jennifer",
  "Michael",
  "Linda",
  "David",
  "Elizabeth",
  "William",
  "Barbara",
  "Richard",
  "Susan",
  "Joseph",
  "Jessica",
  "Thomas",
  "Sarah",
  "Christopher",
  "Karen",
  "Charles",
  "Lisa",
  "Daniel",
  "Nancy",
  "Matthew",
  "Betty",
  "Anthony",
  "Margaret",
  "Mark",
  "Sandra",
  "Donald",
  "Ashley",
  "Steven",
  "Kimberly",
  "Paul",
  "Emily",
  "Andrew",
  "Donna",
  "Joshua",
  "Michelle",
  "Kenneth",
  "Dorothy",
  "Kevin",
  "Carol",
  "Brian",
  "Amanda",
  "George",
  "Melissa",
  "Timothy",
  "Deborah",
  "Ronald",
  "Stephanie",
  "Edward",
  "Rebecca",
  "Jason",
  "Sharon",
  "Jeffrey",
  "Laura",
  "Ryan",
  "Cynthia",
  "Jacob",
  "Kathleen",
  "Gary",
  "Amy",
];

const ENGLISH_LAST_NAMES = [
  "Smith",
  "Johnson",
  "Williams",
  "Brown",
  "Jones",
  "Garcia",
  "Miller",
  "Davis",
  "Rodriguez",
  "Martinez",
  "Hernandez",
  "Lopez",
  "Gonzalez",
  "Wilson",
  "Anderson",
  "Thomas",
  "Taylor",
  "Moore",
  "Jackson",
  "Martin",
  "Lee",
  "Perez",
  "Thompson",
  "White",
  "Harris",
  "Sanchez",
  "Clark",
  "Ramirez",
  "Lewis",
  "Robinson",
  "Walker",
  "Young",
  "Allen",
  "King",
  "Wright",
  "Scott",
  "Torres",
  "Nguyen",
  "Hill",
  "Flores",
  "Green",
  "Adams",
  "Nelson",
  "Baker",
  "Hall",
  "Rivera",
  "Campbell",
  "Mitchell",
  "Carter",
  "Roberts",
  "Gomez",
  "Phillips",
  "Evans",
  "Turner",
  "Diaz",
  "Parker",
  "Cruz",
  "Edwards",
  "Collins",
  "Reyes",
  "Stewart",
  "Morris",
  "Morales",
  "Murphy",
];

// Russian names dataset (transliterated)
const RUSSIAN_FIRST_NAMES = [
  "Aleksandr",
  "Dmitriy",
  "Maksim",
  "Sergey",
  "Andrey",
  "Aleksey",
  "Artyom",
  "Ilya",
  "Kirill",
  "Mikhail",
  "Nikita",
  "Matvey",
  "Roman",
  "Yegor",
  "Arseniy",
  "Ivan",
  "Denis",
  "Yevgeniy",
  "Daniil",
  "Timofey",
  "Vladislav",
  "Igor",
  "Vladimir",
  "Pavel",
  "Ruslan",
  "Mark",
  "Konstantin",
  "Timur",
  "Oleg",
  "Yaroslav",
  "Anton",
  "Nikolay",
  "Anna",
  "Mariya",
  "Yelena",
  "Darya",
  "Alina",
  "Irina",
  "Yekaterina",
  "Anastasiya",
  "Polina",
  "Viktoriya",
  "Yelizaveta",
  "Kseniya",
  "Sofiya",
  "Varvara",
  "Aleksandra",
  "Valeriya",
  "Ulyana",
  "Milana",
  "Yeva",
  "Alisa",
  "Taisia",
  "Kira",
  "Diana",
  "Natalya",
  "Olga",
  "Tatyana",
  "Svetlana",
  "Marina",
  "Lyudmila",
  "Galina",
  "Nina",
];

const RUSSIAN_LAST_NAMES = [
  "Ivanov",
  "Smirnov",
  "Kuznetsov",
  "Popov",
  "Vasilyev",
  "Petrov",
  "Sokolov",
  "Mikhaylov",
  "Novikov",
  "Fyodorov",
  "Morozov",
  "Volkov",
  "Alekseyev",
  "Lebedev",
  "Semyonov",
  "Yegorov",
  "Pavlov",
  "Kozlov",
  "Stepanov",
  "Nikolayev",
  "Orlov",
  "Andreyev",
  "Makarov",
  "Nikitin",
  "Zakharov",
  "Zaytsev",
  "Solovyov",
  "Borisov",
  "Yakovlev",
  "Grigoryev",
  "Romanov",
  "Vorobyov",
  "Sergeyev",
  "Kuzmin",
  "Frolov",
  "Aleksandrov",
  "Dmitriyev",
  "Korolev",
  "Gusev",
  "Kiselev",
  "Ilyin",
  "Maksimov",
  "Polyakov",
  "Sorokin",
  "Vinogradov",
  "Kovalyov",
  "Belov",
  "Medvedev",
  "Antonov",
  "Tarasov",
  "Zhukov",
  "Baranov",
  "Filippov",
  "Komarov",
  "Davydov",
  "Belyayev",
  "Gerasimov",
  "Bogdanov",
  "Osipov",
  "Sidorov",
  "Matveev",
  "Titov",
  "Markov",
  "Mironov",
];

function getScenarioConfig(scenario: ScenarioName, rowCount: number): Scenario {
  switch (scenario) {
    case "simple":
      return {
        name: "Simple benchmark",
        steps: [
          {
            table: {
              name: "samples",
              columns: [
                {
                  name: "id",
                  type: "bigint",
                  generator: { kind: "sequence", start: 1 },
                },
                {
                  name: "name",
                  type: "string",
                  generator: { kind: "randomString", length: 10 },
                },
                {
                  name: "value",
                  type: "float",
                  generator: { kind: "randomFloat", min: 0, max: 1000 },
                },
                {
                  name: "status",
                  type: "string",
                  generator: {
                    kind: "choice",
                    values: ["active", "pending", "inactive"],
                  },
                },
                {
                  name: "created_at",
                  type: "datetime",
                  generator: { kind: "datetime" },
                },
              ],
            },
            rowCount,
          },
        ],
      };

    case "english-names":
      return {
        name: "English names",
        steps: [
          {
            table: {
              name: "samples",
              columns: [
                {
                  name: "id",
                  type: "bigint",
                  generator: { kind: "sequence", start: 1 },
                },
                {
                  name: "first_name",
                  type: "string",
                  generator: {
                    kind: "choiceByLookup",
                    values: ENGLISH_FIRST_NAMES,
                  },
                },
                {
                  name: "last_name",
                  type: "string",
                  generator: {
                    kind: "choiceByLookup",
                    values: ENGLISH_LAST_NAMES,
                  },
                },
                {
                  name: "email",
                  type: "string",
                  generator: { kind: "randomString", length: 20 },
                },
                {
                  name: "score",
                  type: "float",
                  generator: { kind: "randomFloat", min: 0, max: 100 },
                },
                {
                  name: "status",
                  type: "string",
                  generator: {
                    kind: "choice",
                    values: ["active", "pending", "inactive"],
                  },
                },
                {
                  name: "created_at",
                  type: "datetime",
                  generator: { kind: "datetime" },
                },
              ],
            },
            rowCount,
            transformations: [
              {
                description: "Generate email from first and last name",
                transformations: [
                  {
                    kind: "template",
                    column: "email",
                    template: "{first_name}.{last_name}@example.com",
                    lowercase: true,
                  },
                ],
              },
            ],
          },
        ],
      };

    case "russian-names":
      return {
        name: "Russian names",
        steps: [
          {
            table: {
              name: "samples",
              columns: [
                {
                  name: "id",
                  type: "bigint",
                  generator: { kind: "sequence", start: 1 },
                },
                {
                  name: "first_name",
                  type: "string",
                  generator: {
                    kind: "choiceByLookup",
                    values: RUSSIAN_FIRST_NAMES,
                  },
                },
                {
                  name: "last_name",
                  type: "string",
                  generator: {
                    kind: "choiceByLookup",
                    values: RUSSIAN_LAST_NAMES,
                  },
                },
                {
                  name: "email",
                  type: "string",
                  generator: { kind: "randomString", length: 20 },
                },
                {
                  name: "score",
                  type: "float",
                  generator: { kind: "randomFloat", min: 0, max: 100 },
                },
                {
                  name: "status",
                  type: "string",
                  generator: {
                    kind: "choice",
                    values: ["active", "pending", "inactive"],
                  },
                },
                {
                  name: "created_at",
                  type: "datetime",
                  generator: { kind: "datetime" },
                },
              ],
            },
            rowCount,
            transformations: [
              {
                description: "Generate email from first and last name",
                transformations: [
                  {
                    kind: "template",
                    column: "email",
                    template: "{first_name}.{last_name}@example.com",
                    lowercase: true,
                  },
                ],
              },
            ],
          },
        ],
      };
  }
}

const scenarioConfig = getScenarioConfig(SCENARIO, ROW_COUNT);

interface GeneratorEntry {
  name: string;
  generator: DataGenerator;
  flag: keyof typeof values;
}

function createGenerators(): GeneratorEntry[] {
  return [
    {
      name: "SQLite",
      flag: "sqlite",
      generator: new SQLiteDataGenerator({ path: "data/samples.db" }),
    },
    {
      name: "PostgreSQL",
      flag: "postgres",
      generator: new PostgresDataGenerator({
        host: "localhost",
        port: 5432,
        database: "appdb",
        username: "postgres",
        password: "postgres",
      }),
    },
    {
      name: "ClickHouse",
      flag: "clickhouse",
      generator: new ClickHouseDataGenerator({
        host: "localhost",
        port: 8123,
        database: "default",
        username: "default",
        password: "clickhouse",
      }),
    },
    {
      name: "Trino",
      flag: "trino",
      generator: new TrinoDataGenerator({
        host: "localhost",
        port: 8080,
        user: "trino",
        catalog: "iceberg",
        schema: "warehouse",
      }),
    },
  ];
}

async function generateForDatabase(entry: GeneratorEntry): Promise<void> {
  const { name, generator } = entry;
  console.log(`\n=== ${name} ===`);

  try {
    await generator.connect();
    console.log(`Connected to ${name}`);

    const result = await generator.runScenario({
      scenario: scenarioConfig,
      dropFirst: true,
    });

    // Log results for each step
    for (const step of result.steps) {
      if (step.generate) {
        console.log(
          `[${step.tableName}] Generated ${step.generate.rowsInserted.toLocaleString()} rows in ${formatDuration(step.generate.generateMs)} (optimize: ${formatDuration(step.generate.optimizeMs)})`
        );
      }
      if (step.transform) {
        console.log(
          `[${step.tableName}] Applied ${String(step.transform.batchesApplied)} transformation batch(es) in ${formatDuration(step.transform.durationMs)}`
        );
      }
    }

    console.log(
      `Total: ${result.totalRowsInserted.toLocaleString()} rows in ${formatDuration(result.durationMs)}`
    );

    // Verify row counts for all tables
    for (const step of result.steps) {
      const count = await generator.countRows(step.tableName);
      const size = await generator.getTableSizeForHuman(step.tableName);
      console.log(
        `[${step.tableName}] Verified: ${count.toLocaleString()} rows${size ? `, ${size}` : ""}`
      );
    }

    console.log(`Disconnected from ${name}`);
  } catch (err) {
    const message = err instanceof Error ? err.message : "Unknown error";
    console.error(`Error with ${name}: ${message}`);
  } finally {
    await generator.disconnect();
  }
}

async function main(): Promise<void> {
  const generators = createGenerators();
  console.log(`Scenario: ${SCENARIO}`);
  console.log(
    `Generating ${ROW_COUNT.toLocaleString()} rows in each database...`
  );

  // Check which databases to generate for
  const anyDbSelected = generators.some((g) => values[g.flag]);
  const enableAll = !anyDbSelected;

  for (const entry of generators) {
    const enabled = enableAll || values[entry.flag];
    if (enabled) {
      await generateForDatabase(entry);
    } else {
      console.log(`\n=== ${entry.name} === (skipped)`);
    }
  }

  console.log("\n=== Done ===");
}

main().catch(console.error);
