# Trabajo Práctico - Coordinación

En este trabajo se busca familiarizar a los estudiantes con los desafíos de la coordinación del trabajo y el control de la complejidad en sistemas distribuidos. Para tal fin se provee un esqueleto de un sistema de control de stock de una verdulería y un conjunto de escenarios de creciente grado de complejidad y distribución que demandarán mayor sofisticación en la comunicación de las partes involucradas.

## Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`:   Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.

## Elementos del sistema objetivo

![ ](./imgs/diagrama_de_robustez.jpg  "Diagrama de Robustez")
*Fig. 1: Diagrama de Robustez*

### Client

Lee un archivo de entrada y envía por TCP/IP pares (fruta, cantidad) al sistema.
Cuando finaliza el envío de datos, aguarda un top de pares (fruta, cantidad) y vuelca el resultado en un archivo de salida csv.
El criterio y tamaño del top dependen de la configuración del sistema. Por defecto se trata de un top 3 de frutas de acuerdo a la cantidad total almacenada.

### Gateway

Es el punto de entrada y salida del sistema. Intercambia mensajes con los clientes y las colas internas utilizando distintos protocolos.

### Sum
 
Recibe pares  (fruta, cantidad) y aplica la función Suma de la clase `FruitItem`. Por defecto esa suma es la canónica para los números enteros, ej:

`("manzana", 5) + ("manzana", 8) = ("manzana", 13)`

Pero su implementación podría modificarse.
Cuando se detecta el final de la ingesta de datos envía los pares (fruta, cantidad) totales a los Aggregators.

### Aggregator

Consolida los datos de las distintas instancias de Sum.
Cuando se detecta el final de la ingesta, se calcula un top parcial y se envía esa información al Joiner.

### Joiner

Recibe tops parciales de las instancias del Aggregator.
Cuando se detecta el final de la ingesta, se envía el top final hacia el gateway para ser entregado al cliente.

## Limitaciones del esqueleto provisto

La implementación base respeta la división de responsabilidades de los distintos controles y hace uso de la clase `FruitItem` como un elemento opaco, sin asumir la implementación de las funciones de Suma y Comparación.

No obstante, esta implementación no cubre los objetivos buscados tal y como es presentada. Entre sus falencias puede destactarse que:

 - No se implementa la interfaz del middleware. 
 - No se dividen los flujos de datos de los clientes más allá del Gateway, por lo que no se es capaz de resolver múltiples consultas concurrentemente.
 - No se implementan mecanismos de sincronización que permitan escalar los controles Sum y Aggregator. En particular:
   - Las instancias de Sum se dividen el trabajo, pero solo una de ellas recibe la notificación de finalización en la ingesta de datos.
   - Las instancias de Sum realizan _broadcast_ a todas las instancias de Aggregator, en lugar de agrupar los datos por algún criterio y evitar procesamiento redundante.
  - No se maneja la señal SIGTERM, con la salvedad de los clientes y el Gateway.

## Condiciones de Entrega

El código de este repositorio se agrupa en dos carpetas, una para Python y otra para Golang. Los estudiantes deberán elegir **sólo uno** de estos lenguajes y realizar una implementación que funcione correctamente ante cambios en la multiplicidad de los controles (archivo de docker compose), los archivos de entrada y las implementaciones de las funciones de Suma y Comparación del `FruitItem`.

![ ](./imgs/mutabilidad.jpg  "Mutabilidad de Elementos")
*Fig. 2: Elementos mutables e inmutables*

A modo de referencia, en la *Figura 2* se marcan en tonos oscuros los elementos que los estudiantes no deben alterar y en tonos claros aquellos sobre los que tienen libertad de decisión.
Al momento de la evaluación y ejecución de las pruebas se **descartarán** o **reemplazarán** :

- Los archivos de entrada de la carpeta `datasets`.
- El archivo docker compose principal y los de la carpeta `scenarios`.
- Todos los archivos Dockerfile.
- Todo el código del cliente.
- Todo el código del gateway, salvo `message_handler`.
- La implementación del protocolo de comunicación externo y `FruitItem`.

Redactar un breve informe explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes y a la cantidad de controles.

## Informe de Entrega

La solución implementada coordina las instancias de `Sum` y `Aggregation` usando mensajes internos con `request_id`, `type`, `sequence` y, cuando hace falta deduplicar por emisor, `origin`.

### Coordinación entre instancias de `Sum`

La comunicación `Gateway -> Sum` se mantiene como una work queue compartida. Esto permite repartir los mensajes de datos entre varias réplicas de `Sum`, pero introduce un problema con el EOF: el fin de una consulta puede llegar solo a una réplica, mientras que otras réplicas todavía tienen estado parcial de esa misma consulta.

Para resolverlo, cuando una instancia de `Sum` recibe el EOF original del cliente, reemite un mensaje interno de control `sum_eof` hacia todas las réplicas de `Sum`. Cada réplica mantiene estado local por `request_id`, de modo que:

- si procesó datos de esa consulta, cierra su estado local y emite sus parciales
- si no procesó datos de esa consulta, ignora el `sum_eof`

Además, se fijó `prefetch = 1` en la cola de entrada para evitar que RabbitMQ adelante varios mensajes a una réplica antes de que esta procese el cierre de la consulta. Esto reduce el riesgo de cerrar una consulta antes de haber drenado mensajes ya reservados por el broker.

Como intento de mejora de performance, se evaluó una variante donde `Gateway` agregaba a cada mensaje un `sequence` global o timestamp incremental, y `Sum` dejaba el EOF pendiente hasta observar un mensaje posterior con `sequence` más nueva. La idea era relajar la dependencia de `prefetch = 1` y permitir más trabajo en vuelo por consumidor. Ese approach se descartó porque introducía casos borde en el cierre local. Por eso se optó por la solución más conservadora: reemisión explícita del EOF entre réplicas y `prefetch = 1`.

Un contraejemplo simple muestra por qué ese enfoque podía fallar. Supongamos una cola con:

```text
p1, p2, p3, EOF
```

y dos réplicas:

```text
s1, s2
```

Si RabbitMQ permite adelantar varios mensajes, puede ocurrir que:

- `s1` tenga reservados `p1`, `p2` y `p3`
- `s2` reciba antes el `EOF`

En un approach basado en dejar el EOF pendiente hasta observar cualquier mensaje posterior con `sequence` más nueva, el problema aparece igual del lado de `s2`. Como `s2` no recibió datos de esa consulta, puede ocurrir que tampoco reciba luego ningún mensaje nuevo que le permita decir “ya vi algo más nuevo, ahora proceso este EOF”. Entonces `s2` podría quedar con un EOF pendiente que nunca procesa, aun cuando su caso debería ser el más trivial de todos: no tenía trabajo acumulado y solo necesitaba confirmar el cierre.

### Coordinación entre instancias de `Aggregation`

Los datos emitidos por `Sum` no se envían por broadcast a todos los `Aggregation`. En cambio, cada fruta acumulada se particiona de forma determinística usando una clave derivada de:

```text
request_id|fruit
```

Con esto, para una consulta concreta, cada fruta siempre cae en un único `Aggregation`, evitando procesamiento redundante.

Sin embargo, el EOF sí se envía a todos los `Aggregation`. Esto es necesario porque una partición puede no haber recibido datos de una consulta y, aun así, debe enterarse de que esa consulta terminó para no quedar esperando indefinidamente.

Cada `Aggregation` mantiene estado por `request_id` y espera un EOF de cada instancia de `Sum`. Para que el cierre sea robusto frente a duplicados, el EOF lleva `origin = sum_id` y `Aggregation` registra confirmaciones únicas por origen. Así, un mismo `Sum` no puede cerrar dos veces la misma consulta. Cuando una instancia de `Aggregation` recibió los EOFs de todos los `Sum`, construye su top parcial y lo envía a `Join`.

También se tuvo en cuenta un enfoque alternativo más orientado a coordinación entre `Aggregation`, donde la primera instancia en detectar cierto cierre actuara como disparador y propagara esa información al resto mediante comunicación entre `Aggregation`. Sin embargo, ese esquema agregaba una topología extra y requería implementar un protocolo adicional entre pares, con manejo de duplicados, carreras y posibles reenvíos. En comparación, el esquema actual resultó más ventajoso porque aprovecha directamente la configuración ya disponible: `SUM_AMOUNT` define cuántos cierres debe esperar cada `Aggregation`, y `AGGREGATION_AMOUNT` define cuántos parciales debe esperar `Join`. De esa manera, se redujo la complejidad del código y el tiempo de implementación, sin necesidad de introducir liderazgo o comunicación lateral adicional entre aggregators.

### Escalabilidad respecto a los clientes

El sistema escala respecto a los clientes porque cada consulta queda identificada por un `request_id` único, que viaja por todo el pipeline interno. Esto permite que varias consultas de distintos clientes se procesen en paralelo sin mezclar estado ni resultados.

Cada etapa interna mantiene su estado separado por `request_id`, por lo que:

- múltiples clientes pueden estar activos al mismo tiempo
- los parciales de una consulta no interfieren con los de otra
- el `Gateway` puede reenviar cada resultado final al cliente correcto

### Escalabilidad respecto a la cantidad de controles

En `Sum`, los datos se balancean naturalmente por la work queue compartida. Todas las réplicas pueden participar del procesamiento de una misma consulta, y el mecanismo de `sum_eof` garantiza que todas las que intervinieron puedan cerrar correctamente.

En `Aggregation`, el particionado por `(request_id, fruit)` reparte el dominio de trabajo entre instancias, de modo que cada una procesa solo la parte que le corresponde. Esto evita que el costo crezca linealmente con la cantidad de réplicas por duplicación de cómputo.

La sincronización final se apoya en cantidades esperadas de confirmaciones:

- cada `Aggregation` espera un cierre por cada `Sum`
- `Join` espera un parcial por cada `Aggregation`

Esto hace que, al aumentar la cantidad de controles, la lógica siga siendo la misma: lo que crece es la cantidad de mensajes de cierre o confirmación requeridos. Si hay más réplicas de `Sum`, cada `Aggregation` deberá recibir más EOFs para cerrar su partición. Si hay más `Aggregation`, `Join` deberá recibir más parciales para cerrar la consulta completa. Esa es también la principal desventaja de escalar horizontalmente este esquema: a mayor cantidad de nodos, mayor cantidad de confirmaciones necesarias antes de poder cerrar una consulta.

### Aclaraciones extra

- Se agregó una validación para forzar `prefetch = 1` en el middleware de cola:

```go
if err := m.ch.Qos(queuePrefetchCount, 0, false); err != nil {
	return ErrMessageMiddlewareMessage
}
```

- Se modificó la creación de exchanges y consumers con respecto a la entrega original de MOMs para que el lado consumidor se inicialice recién en `StartConsuming`, en vez de quedar suscripto automáticamente desde `CreateExchange` o `CreateQueue`. De esta manera, los productores no quedan registrados como consumidores de colas o exchanges que en realidad no van a consumir.
