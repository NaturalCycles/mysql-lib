## [3.5.1](https://github.com/NaturalCycles/mysql-lib/compare/v3.5.0...v3.5.1) (2019-11-20)


### Bug Fixes

* correctly type-cast booleans with value=NULL ([e208b4c](https://github.com/NaturalCycles/mysql-lib/commit/e208b4ca16f700b6ed7bf95c872f68f934894992))

# [3.5.0](https://github.com/NaturalCycles/mysql-lib/compare/v3.4.0...v3.5.0) (2019-11-09)


### Features

* implement getDBAdapter. Allow empty (default) configuration ([bcf12dc](https://github.com/NaturalCycles/mysql-lib/commit/bcf12dc9360f525749f9bb10965046b4c56d849a))

# [3.4.0](https://github.com/NaturalCycles/mysql-lib/compare/v3.3.1...v3.4.0) (2019-11-09)


### Features

* auto JSON.stringify objects (and arrays) ([0d9aaed](https://github.com/NaturalCycles/mysql-lib/commit/0d9aaedf8f2920a51dacf13b6723eb98c61b34c3))

## [3.3.1](https://github.com/NaturalCycles/mysql-lib/compare/v3.3.0...v3.3.1) (2019-11-09)


### Bug Fixes

* use LONGBLOB for binary columns ([ae4facc](https://github.com/NaturalCycles/mysql-lib/commit/ae4facc76bc03b8aafa5141199a98bef530f6913))

# [3.3.0](https://github.com/NaturalCycles/mysql-lib/compare/v3.2.1...v3.3.0) (2019-11-09)


### Features

* mapNameToMySQL ([c4bf40c](https://github.com/NaturalCycles/mysql-lib/commit/c4bf40cd6aefaf78219a2588ec803cf74004bc07))

## [3.2.1](https://github.com/NaturalCycles/mysql-lib/compare/v3.2.0...v3.2.1) (2019-11-09)


### Bug Fixes

* createTable() ([7983085](https://github.com/NaturalCycles/mysql-lib/commit/7983085017562a4e4a8f7dd38eb41ae6b9fc30ed))

# [3.2.0](https://github.com/NaturalCycles/mysql-lib/compare/v3.1.0...v3.2.0) (2019-11-09)


### Bug Fixes

* emoji support ([1262fc4](https://github.com/NaturalCycles/mysql-lib/commit/1262fc4d9ad39c818833b5cfc31b3a65979cdea0))


### Features

* auto split long sql queries ([c7c780f](https://github.com/NaturalCycles/mysql-lib/commit/c7c780f295d7ded9f418509f3160a512ec54c7e7))

# [3.1.0](https://github.com/NaturalCycles/mysql-lib/compare/v3.0.0...v3.1.0) (2019-11-08)


### Features

* commonSchemaToMySQLDDL ([09168c5](https://github.com/NaturalCycles/mysql-lib/commit/09168c563cbb629ca47121a1aac6a125dd23c144))
* implement getTables(), getTableSchema() ([20a7746](https://github.com/NaturalCycles/mysql-lib/commit/20a7746c8c4c88dfca8365d1e8d2decce3f47efa))

# [3.0.0](https://github.com/NaturalCycles/mysql-lib/compare/v2.0.2...v3.0.0) (2019-11-02)


### Features

* adapt to db-lib@3 ([7d13f54](https://github.com/NaturalCycles/mysql-lib/commit/7d13f540c92dd59b014d1a502c1bff937fad567f))


### BREAKING CHANGES

* ^^^

## [2.0.2](https://github.com/NaturalCycles/mysql-lib/compare/v2.0.1...v2.0.2) (2019-10-20)


### Bug Fixes

* adopt to db-lib ([6da7d78](https://github.com/NaturalCycles/mysql-lib/commit/6da7d78))

## [2.0.1](https://github.com/NaturalCycles/mysql-lib/compare/v2.0.0...v2.0.1) (2019-10-19)


### Bug Fixes

* use Readable ([a0d09c1](https://github.com/NaturalCycles/mysql-lib/commit/a0d09c1))

# [2.0.0](https://github.com/NaturalCycles/mysql-lib/compare/v1.7.1...v2.0.0) (2019-10-18)


### Features

* implement CommonDB 2.0 ([9de0e9f](https://github.com/NaturalCycles/mysql-lib/commit/9de0e9f))


### BREAKING CHANGES

* ^^^

## [1.7.1](https://github.com/NaturalCycles/mysql-lib/compare/v1.7.0...v1.7.1) (2019-10-18)


### Bug Fixes

* pin @types/hapi__joi ([3e3cf5a](https://github.com/NaturalCycles/mysql-lib/commit/3e3cf5a))

# [1.7.0](https://github.com/NaturalCycles/mysql-lib/compare/v1.6.0...v1.7.0) (2019-09-30)


### Features

* tiny change to use pool.query inst of getConnection() ([6ffe1b4](https://github.com/NaturalCycles/mysql-lib/commit/6ffe1b4))

# [1.6.0](https://github.com/NaturalCycles/mysql-lib/compare/v1.5.0...v1.6.0) (2019-09-30)


### Features

* offset ([78710a1](https://github.com/NaturalCycles/mysql-lib/commit/78710a1))

# [1.5.0](https://github.com/NaturalCycles/mysql-lib/compare/v1.4.0...v1.5.0) (2019-09-30)


### Features

* testing simpler streamSQL impl ([0887aa3](https://github.com/NaturalCycles/mysql-lib/commit/0887aa3))

# [1.4.0](https://github.com/NaturalCycles/mysql-lib/compare/v1.3.0...v1.4.0) (2019-09-30)


### Features

* stream to use it's own separate connection ([43615f3](https://github.com/NaturalCycles/mysql-lib/commit/43615f3))

# [1.3.0](https://github.com/NaturalCycles/mysql-lib/compare/v1.2.0...v1.3.0) (2019-09-30)


### Features

* cfg.debugConnections ([70218ba](https://github.com/NaturalCycles/mysql-lib/commit/70218ba))

# [1.2.0](https://github.com/NaturalCycles/mysql-lib/compare/v1.1.0...v1.2.0) (2019-09-28)


### Features

* add BIT cast to boolean ([899cce4](https://github.com/NaturalCycles/mysql-lib/commit/899cce4))

# [1.1.0](https://github.com/NaturalCycles/mysql-lib/compare/v1.0.0...v1.1.0) (2019-09-21)


### Features

* modernize, implement saveBatch ([5595b6a](https://github.com/NaturalCycles/mysql-lib/commit/5595b6a))

# 1.0.0 (2019-06-03)


### Features

* first version ([02620fb](https://github.com/NaturalCycles/mysql-lib/commit/02620fb))
* init project by create-module ([eee50b7](https://github.com/NaturalCycles/mysql-lib/commit/eee50b7))
