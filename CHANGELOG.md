# Changelog

## [0.4.0](https://github.com/making-ventures/samples-generation/compare/v0.3.0...v0.4.0) (2025-12-22)

### Features

- show last batch duration in progress output ([041a4ba](https://github.com/making-ventures/samples-generation/commit/041a4ba1c0249214572720d4b41531dc71f7f147))

### Documentation

- update ClickHouse simple 1B benchmark results ([de747a4](https://github.com/making-ventures/samples-generation/commit/de747a4a7e636148da13c5729d5b27ca85335fe9))

## [0.3.0](https://github.com/making-ventures/samples-generation/compare/v0.2.0...v0.3.0) (2025-12-21)

### Features

- add batch size option for large dataset generation ([dbb9091](https://github.com/making-ventures/samples-generation/commit/dbb9091d4872a58a4bb83596a6bf496f69f7439a))
- add ETA display for batch progress ([d2641e8](https://github.com/making-ventures/samples-generation/commit/d2641e8f6fc3d141a82b3486c9fb8b3aaede88ee))
- add individual database compose commands ([df1acec](https://github.com/making-ventures/samples-generation/commit/df1acec8fd7d83e5ba988ee397e9bfda0b17e804))
- add lookup-demo scenario demonstrating LookupTransformation ([7d58c01](https://github.com/making-ventures/samples-generation/commit/7d58c01ee192271cf0737a64eb83d07bf9b81b1e))
- add lookup-demo scenario with ScenarioResult timing breakdown ([272f66f](https://github.com/making-ventures/samples-generation/commit/272f66f369a9ca1329a10159c99cd4069d53bd2a))
- add trino-fte configuration (16GB + fault-tolerant execution) ([6d01100](https://github.com/making-ventures/samples-generation/commit/6d01100836a50619876eb4f95abfdc8c7eb688a5))
- show sample row from each table in generate-all script ([52e3e4e](https://github.com/making-ventures/samples-generation/commit/52e3e4e3062485ae30c8ea294b642bc3ebe3c90a))
- use S3-based exchange manager for trino-fte ([af5ecf1](https://github.com/making-ventures/samples-generation/commit/af5ecf1480c2ab3b8700df965720e7a87f51645f))

### Bug Fixes

- adjust Trino memory config to fit within 8GB container ([91fd26e](https://github.com/making-ventures/samples-generation/commit/91fd26e622f6a3736a562a01489cdbd664c558aa))
- properly catch and display Trino transformation errors ([d9477d4](https://github.com/making-ventures/samples-generation/commit/d9477d45f059375b3d2cf6578dcb3339d00aba2f))
- use correct Trino optimize procedure names ([301d372](https://github.com/making-ventures/samples-generation/commit/301d372f9504b0a2f1eb94864e92f0a8895e4e5f))

### Refactoring

- extract executeQuery helper for Trino error handling ([c300cb8](https://github.com/making-ventures/samples-generation/commit/c300cb88f5f57fee17a3d3fa42f700c3c9f58764))

### Documentation

- add reproduction commands to measurements section ([1bf9ff2](https://github.com/making-ventures/samples-generation/commit/1bf9ff28f133899c63c1ae2bc1a855cfa106668a))
- add Trino FTE 16GB benchmark results ([7920e3e](https://github.com/making-ventures/samples-generation/commit/7920e3e58f1faca6ced18d974e248dcf60806f6c))
- update benchmark results for Trino 16GB FTE with batching ([cab530b](https://github.com/making-ventures/samples-generation/commit/cab530b8ba2082ac18c60d12e5a0c7f62d3fbcd6))
- update note about equalized resource configuration ([3dafa24](https://github.com/making-ventures/samples-generation/commit/3dafa247c7a548ee3bf91a221650f1d8c8d81bb8))
- update README with multi-step scenarios and script options ([c46ef5a](https://github.com/making-ventures/samples-generation/commit/c46ef5acd405bc130d166da2e3c1b349bb86c2f9))

## 0.2.0 (2025-12-21)

### Features

- add choiceFromTable generator for large value sets ([896f45f](https://github.com/making-ventures/samples-generation/commit/896f45f895ec81b348ffa3b046164da2890d18d0))
- add nullProbability option for generating NULL values ([cdc2162](https://github.com/making-ventures/samples-generation/commit/cdc21623f3f060fc5ce7afda517f6f9263436661))
- add optional description to TableConfig for logging ([18c0478](https://github.com/making-ventures/samples-generation/commit/18c04786424e0092da1e2fccebf7f61d937a274c))
- add optional description to transformation batches ([238cc8a](https://github.com/making-ventures/samples-generation/commit/238cc8ac1a0f1c73974b58fcbe3c7d715f0686ad))
- add ROW_COUNT env var to generate-all.ts script ([089a6ca](https://github.com/making-ventures/samples-generation/commit/089a6ca9aa1cd7caf316a3a41c26379b3dad1d04))
- add Scenario API with multi-step and transform-only support ([fcef70a](https://github.com/making-ventures/samples-generation/commit/fcef70a0b80aeb5b442841c367aab28bac71b799))
- add swap transformation for all 4 databases ([081ffef](https://github.com/making-ventures/samples-generation/commit/081ffef7f0ad692604edc1790aacab23dcdab2af))
- **clickhouse:** add unique suffix to lookup temp tables for concurrency safety ([d7c02eb](https://github.com/making-ventures/samples-generation/commit/d7c02ebecd27cfe52ea5817270eaffdd9b89e796))
- multi-database data generator with PostgreSQL, ClickHouse, SQLite, and Trino support ([c509b19](https://github.com/making-ventures/samples-generation/commit/c509b1927fec4b7bf49fe9af930631b5d0e123dc))
- support underscore format in --rows (e.g., 10_000) ([699fd36](https://github.com/making-ventures/samples-generation/commit/699fd367c852917fdfd8b53f65752a74ecbc4a82))

### Bug Fixes

- **clickhouse:** swap transformation - unescape SHOW CREATE output and use table alias to avoid column name resolution issue ([bc7e313](https://github.com/making-ventures/samples-generation/commit/bc7e3138a699fa86c45c116ee03de93c098087bd))
- correct choiceByLookup SQL syntax for ClickHouse and Trino ([8457f82](https://github.com/making-ventures/samples-generation/commit/8457f82600057422084d309aba11fcb193902c8d))
- format row count with thousand separators in log output ([1747cd7](https://github.com/making-ventures/samples-generation/commit/1747cd7718fa5206c84776354d1975c6a776f618))
- increase ClickHouse timeout to 2 hours for 10B+ rows ([592f909](https://github.com/making-ventures/samples-generation/commit/592f9097a0807979cbae4bbd978ee0fd1f521aa4))
- increase ClickHouse timeout to 6 hours for 10B+ rows ([85880a1](https://github.com/making-ventures/samples-generation/commit/85880a175c1c0a67df6cef86930e8158656da040))
- increase test timeouts for slow SQLite tests ([cf6badf](https://github.com/making-ventures/samples-generation/commit/cf6badf7d24b4b306b2295b6de8a9a9179bc8641))
- PostgreSQL optimize uses wrong identifier escaping ([1a3855f](https://github.com/making-ventures/samples-generation/commit/1a3855f1b4997c37b5ee59c9d9ef1475e02098b6))
- scale Trino and ClickHouse for 1B+ row generation ([09fe1cc](https://github.com/making-ventures/samples-generation/commit/09fe1ccb5173e43ca3066ab68b0f3871a11de049))
- test-all-dbs.sh now runs e2e tests instead of unit tests ([a26f9b9](https://github.com/making-ventures/samples-generation/commit/a26f9b9d2eed244d005efaca50b00d92101e5e49))
- **trino:** cast sequence expression to BIGINT for >2B rows ([c7fbbd3](https://github.com/making-ventures/samples-generation/commit/c7fbbd3c4e9d8b203707448ef0ff5aa1799f3ca0))
- **trino:** use BIGINT literals for large numeric constants ([de59f58](https://github.com/making-ventures/samples-generation/commit/de59f586fde93ca696c81f0936b26b022db5c544))
- use DOUBLE PRECISION instead of NUMERIC for PostgreSQL float type ([b00e5ba](https://github.com/making-ventures/samples-generation/commit/b00e5ba69c7b7ce7756eb9f53286bcafcdde50b8))

### Performance

- add production-safe PostgreSQL optimizations for bulk operations ([c69236e](https://github.com/making-ventures/samples-generation/commit/c69236ed7e9d25d3d53979867cb463ef81462fb7))
- tune PostgreSQL for bulk inserts in docker-compose ([0150d7c](https://github.com/making-ventures/samples-generation/commit/0150d7cfe5a5c15cabc0d0c73ad22a2243221887))

### Refactoring

- extract e2e tests to generator.e2e.test.ts with global 30s timeout ([29faec2](https://github.com/making-ventures/samples-generation/commit/29faec2a1d93fc7b3e30e105b7cd35a7d737664d))
- optimize tables once at end of scenario, use name dictionaries ([8a966d1](https://github.com/making-ventures/samples-generation/commit/8a966d1c5b7c57a847f4117cef2023b2c468a52e))
- remove backward compatibility for transformation batches ([3faf6a7](https://github.com/making-ventures/samples-generation/commit/3faf6a755b21dc09f8c9e09de13ff8b6299720f4))
- rename choiceFromTable to choiceByLookup ([eeb9e46](https://github.com/making-ventures/samples-generation/commit/eeb9e46af6e1e98222721b53131c6c1fa2f1e62c))
- separate e2e tests from unit tests ([30bcad3](https://github.com/making-ventures/samples-generation/commit/30bcad34de24205c3f6f5a8a009b5bd320ce870d))
- separate transform() from generate() ([35d427c](https://github.com/making-ventures/samples-generation/commit/35d427c1372324bd250d7416afd738dab130b958))
- use CLI options instead of env vars in generate-all.ts ([bac7244](https://github.com/making-ventures/samples-generation/commit/bac7244a5541be4d718d26aa2622a8b39f7b766c))

### Documentation

- add execution order note for ClickHouse lookup transformations ([3cf1feb](https://github.com/making-ventures/samples-generation/commit/3cf1febe7d82df52edab1fa0bac2caed17685caf))
- add PostgreSQL 1B rows benchmark results ([6ffacc3](https://github.com/making-ventures/samples-generation/commit/6ffacc367e96684a1ab3fa35dd53ef795ba51845))
- add Trino 10B generation benchmark (1h 4m, 158.62 GB) ([5115474](https://github.com/making-ventures/samples-generation/commit/5115474e73cfdfdb09de1265360ffc4130ccb80f))
- clarify randomFloat and datetime descriptions, document default precision ([66b1d09](https://github.com/making-ventures/samples-generation/commit/66b1d09aaec86c17e65376e18edf95ddde4b90bc))
- clarify Trino writes to Iceberg tables ([f3052f7](https://github.com/making-ventures/samples-generation/commit/f3052f783f3e3375e6f262371e8aca06eb1f08c0))
- clarify unit vs e2e tests in README ([07708a8](https://github.com/making-ventures/samples-generation/commit/07708a8bef1b1af298d3c7ab96948cc722f8b98b))
- clean up README ([55531f1](https://github.com/making-ventures/samples-generation/commit/55531f1f2b8720076e98fa10f2f1328deb9c5dd3))
- fix default row count in README (1000, not 1 billion) ([4b3474a](https://github.com/making-ventures/samples-generation/commit/4b3474a426aea5ea16d89d575f48a6a96428df94))
- fix typos in README objective section ([27239cb](https://github.com/making-ventures/samples-generation/commit/27239cb4df6b099f0bd5ecbc3aa2eace86883452))
- move formatBytes to Table Size section, add test-all-dbs.sh reference ([89e8954](https://github.com/making-ventures/samples-generation/commit/89e8954deed453d19fb43de95b6418e275920965))
- update Trino 1B benchmark timing (6m 48s) ([ef08a6b](https://github.com/making-ventures/samples-generation/commit/ef08a6b048d06e424b4bb08bc25b4301ec2ddf1c))

All notable changes to this project will be documented in this file.
