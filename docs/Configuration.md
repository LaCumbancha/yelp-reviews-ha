## Configuración

Dentro del directorio `scripts` se podrá encontrar el archivo de configuración `system-config.yaml` con el que podrá variar la parametría del sistema. Estas configuraciones se dividen en 3 grandes categorías que se explicarán a continuación:

### Environment

#### Logging
* `log_level`: Nivel de log del sistema. Puede ser: **TRACE**, **DEBUG**, **INFO**, **WARN** o **ERROR**.
* `log_bulk_rate`: Para evitar loguear todos los bulks procesados (y volver el log ilegible) se puede configurar cada cuantos dejar registro. Los **WARN** y **ERROR** se loguearán siempre.

#### Flows
* `funbiz_logging`: Habilita el logging del flujo Top Funniest Cities.
* `weekday_logging`: Habilita el logging del flujo Weekday Histogram.
* `best_logging`: Habilita el logging del flujo Best Users.
* `bots_logging`: Habilita el logging del flujo Bots Users.
* `top_logging`: Habilita el logging del flujo Top Users.

#### Specs
* `funbiz_top_size`: Tamaño del top de Funniest Cities.
* `bots_reviews_threshold`: Mínimo de reviews de texto idéntico esperadas para considerar a un usuario dentro de los 'Bots Users'.
* `users_reviews_threshold`: Mínimo de reviews esperadas para considerar a un usuario dentro de los 'Top Users'.

### Nodes

#### System
* `common_pool_size`: Tamaño del pool de goroutines utilizadas por el Reviews Scatter.
* `reviews_streaming`: Envío de reviews desde el cliente de streaming activado o desactivado.

#### Bulks
* `input1_bulk_size`: Tamaño del bulk de reviews que envía el Businesses Scatter (I1).
* `joiner1_bulk_size`: Tamaño del bulk de reviews que envía el Funcit Joiner (J1).
* `aggregator1_bulk_size`: Tamaño del bulk de reviews que envía el Funbiz Aggregator (A1).
* `aggregator7_bulk_size`: Tamaño del bulk de reviews que envía el User Aggregator (A7).
* `aggregator8_bulk_size`: Tamaño del bulk de reviews que envía el Stars Aggregator (A8).

##### Instances

#### Mappers
* `mapper1_instances`: Instancias del Citbiz Mapper (M1).
* `mapper2_instances`: Instancias del Funbiz Mapper (M2).
* `mapper3_instances`: Instancias del Weekday Mapper (M3).
* `mapper4_instances`: Instancias del Hash Mapper (M4).
* `mapper5_instances`: Instancias del User Mapper (M5).
* `mapper6_instances`: Instancias del Stars Mapper (M6).

#### Filters
* `filter1_instances`: Instancias del Funbiz Filter (F1).
* `filter4_instances`: Instancias del User Filter (F4).
* `filter5_instances`: Instancias del Stars Filter (F5).

#### Monitors
* `monitor_instances`: Instancias de los monitores que controlan el sistema ante fallos.
