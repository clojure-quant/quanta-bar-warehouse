# quanta-bar-warehouse [![GitHub Actions status |clojure-quant/quanta-bar-warehouse](https://github.com/clojure-quant/quanta-bar-warehouse/workflows/CI/badge.svg)](https://github.com/clojure-quant/quanta-bar-warehouse/actions?workflow=CI)[![Clojars Project](https://img.shields.io/clojars/v/io.github.clojure-quant/quanta-bar-warehouse.svg)](https://clojars.org/io.github.clojure-quant/quanta-bar-warehouse)

bar-warehouse 
  - allows to store bars locally in a persistent way
  - this makes sense because bars can be big, so having them locally 
    speeds things up
  - storages are nippy and duckdb


bar-engine 
  - uses the bar-warehouse and bar-importers
  - allows to use various different strategies to get bars 
    (a middleware)  

preloader
- imports data once to the warehouse using bar-engine


## for developers

### for development

 start a nrepl-connection to the dev project.
 in dev folder you will find plenty of namespaces to play with


*code linter*  `clj -M:lint`

*code formatter `clj -M:cljfmt-fix`

*unit tests* `clj -M:test`


## bardb types
- duckdb
- nippy



## transform types
- append-only
- compress
- dynamic
- shuffle



## todo
- overviewdb uses datahike, but this is not included here.
