### Configuración

Dentro del directorio `scripts` se podrá encontrar el archivo de configuración `system-config.yaml` con el que podrá jugar con las distintas partes del sistema. Por un lado, dentro de las secciones de cada flujo podrá editarse la cantidad de nodos de cada uno de los componentes. Por otro lado, en las 5 primeras secciones se podrán editar las siguientes configuraciones del sistema:

#### Testing

* `testing_mode`: Define si el sistema correrá con el set de datos productivo o con uno de testing.
* `test_file_size`: En caso de que se corra en modo test, se podrá especificar el tamaño del set de datos que se generará con el script `test-builder`.

#### System configuration
* `reviews_streaming`: Para activar o desactivar el envío de reviews desde el cliente de streaming.
* `reviews_pool_size`: Tamaño del pool de goroutines para el ReviewsScatter.
* `business_pool_size`: Tamaño del pool de goroutines para el BusinessesScatter

#### Bulks
* `log_bulk_rate`: Cantidad de bulks entre los que se registrá un log.
* `reviews_bulk_size`: Tamaño del bulk de reviews que envía el ReviewsScatter.
* `business_bulk_size`: Tamaño del bulk de reviews que envía el BusinessesScatter.
* `funbizagg_bulk_size`: Tamaño del bulk de reviews que envía el Funny-Business aggregator.
* `funcitjoin_bulk_size`: Tamaño del bulk de reviews que envía el Funny-City joiner.
* `funcitagg_bulk_size`: Tamaño del bulk de reviews que envía el Funny-City aggregator.
* `hashagg_bulk_size`: Tamaño del bulk de reviews que envía el Hash-Text aggregator.
* `dishashagg_bulk_size`: Tamaño del bulk de reviews que envía el Distinct-Hash aggregator.
* `useragg_bulk_size`: Tamaño del bulk de reviews que envía el User aggregator.
* `starsagg_bulk_size`: Tamaño del bulk de reviews que envía el Stars aggregator.

#### Logs
* `funniest_cities_logging`: Habilita el logging del flujo Funniest Cities.
* `weekday_histogram_logging`: Habilita el logging del flujo Weekday Histogram.
* `best_users_logging`: Habilita el logging del flujo Best Users.
* `bot_users_logging`: Habilita el logging del flujo Bot Users.
* `top_users_logging`: Habilita el logging del flujo Top Users.

#### System specs
* `users_min_reviews`: Mínimo de reviews esperadas para considerar a un usuario dentro de los 'Top Users'.
* `bots_min_reviews`: Mínimo de reviews de texto idéntico esperadas para considerar a un usuario dentro de los 'Bot Users'.
* `funniest_cities_top_size`: Tamaño del top de Funniest Cities.
