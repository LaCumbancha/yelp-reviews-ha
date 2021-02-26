# Review Analysis
TP2 | 75.74 - Sistemas Distribuidos I | 2C2020 | FIUBA

## Requerimientos 

### Funcionales

Se solicita un sistema distribuido que procese el detalle de críticas sobre el servicio de comercios. El sistema debe recibir la transmisión de los datos a ser procesados y retornar:
* Usuarios con 50 o más reviews.
* Usuarios con 50 o más reviews con comentarios 5 estrellas únicamente.
* Usuarios con 5 o más reviews que utilizan siempre el mismo texto.
A su vez, se desea obtener la siguiente información estadística:
* Histograma de cantidad de comentarios por día de la semana (Lu, Ma, ..., Do).
* Listado de las 10 ciudades más diverditas, es decir, con mayor cantidad de reviews "funny".
Como origen de datos se definen los archivos de ingreso registrados [en estos datasets](https://www.kaggle.com/pablodroca/yelp-review-analysis). El sistema debe soportar más de 1 consulta en secuencia del único cliente, sin
cerrarse.

### No Funcionales

Además del correcto funcionamiento del sistema, deben tenerse en cuenta las siguientes consideraciones:

* El sistema debe estar optimizado para entornos multicomputadoras.
* EL sistema debe ser invocado desde un nodo que transmite los datos a ser procesados.
* Se debe soportar el escalamiento de los elementos de cómputo.
* De ser necesaria una comunicación basada en grupos, se requiere la definición de un middleware.
* El diseño debe permitir la adaptación para eventuales procesamientos en streaming, es decir, a medida que se reciben los nuevos comentarios.
* El sistema debe mostrar alta disponibilidad hacia los clientes.
* El sistema debe ser tolerante a fallos como la caída de procesos.
* Debido a restricciones en el tiempo de implementación, se permite la construcción de un sistema acoplado al modelo de negocio. No es requerimiento la creación de una plataforma de procesamiento de datos.

## Desarrollo

### Set Up

Antes de poder levantar el sistema deberá colocarse el archivo de businesses en el siguiente directorio:

```bash
root
|-- data
|    |-- businesses 
|    |    |-- business.json
```

Para buildear todos los nodos del sistema, se utilizará el siguiente comando:

```bash
make system-build
```

Una vez construidos los nodos, para levantar y poder realizar un seguimiento del sistema a través de los logs deberá ejecutarse:

```bash
make system-soft-up
make system-logs
```

Otra forma alternativa para buildear y levantar el sistema directamente en un sólo paso es con:

```bash
make system-hard-up
```

Una vez que el sistema se encuentre esperando recibir mensajes, deberá levantarse el cliente externo que permitirá el envío de múltiples datasets. Esto podrá hacerse con el script `./scripts/inputs/reviews-scatter`, el cual permitirá seleccionar los archivos que se irán enviando al sistema. Previamente, deberá tenerse instalado Golang en el host y se deberá ejecutar el siguiente comando para instalar la librería de AMQP:

```bash
go get github.com/streadway/amqp
```

Finalmente, para poder usar el modo streaming, deberá previamente setearse en `true` la variable de configuración del `system-config.yaml`. Hecho esto y una vez levantado el sistema, deberá correrse el script `./scripts/inputs/reviews-streamer` con el cual podrán comenzar a streamearse nuevas reviews custom al sistema. Los requisitos del mismo en cuanto a dependencias son los mismos que para el script de procesamiento de datasets.

### Configuración adicional

Todas las distintas configuraciones con las que puede levantarse el sistema son customizables a través del archivo `./scripts/system-config.yaml`. Para más información de estas, se podrá consultar la sección de [Configuración](docs/Configuration.md)
