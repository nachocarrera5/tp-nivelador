# Informe

La solucion esta formada por un servidor codeado en Python y seis clientes en Go. Cada uno de los componentes es ejecutado en su propio contenedor.
Docker Compose configura las variables de entorno y los volumenes para los archivos de entrada y salida.

Como exige la consigna, cada cliente (agencia) procesa el archivo de input que le corresponde linea por linea y utiliza batching para enviarle las apuestas al servidor. El tamaño de los batches se configura mediante la variable BATCH_SIZE. No es necesario cargar el archivo completo en memoria.

## Protocolo diseñado

Cada mensaje es enviado sobre sockets TCP con la siguiente estructura:

- Un byte para identificar el tipo de mensaje
- Ocho bytes ASCII que funcionan de encabezado para indicar el largo del payload
- El payload con el mensaje (la apuesta)

Los tipos de mensaje son:

- B: Batch de apuestas
- A: "ACK"
- F: Indica que la agencia termino de enviar sus apuestas
- W: Indica las apuestas que son ganadoras
- E: Para indicar que termino el envio de ganadores
- X: Error

Las apuestas se separan con saltos de linea y cada una tiene un identificador asociado a la agencia que corresponde. El servidor responde ACK despues de cada batch recibido correctamente. El encabezado permite saber cuanto hay que leer por mensaje ya que TCP funciona mediante flujo de bytes y por si mismo no separa los mensajes. 

Las funciones send_all y recv_all manejan correctamente los casos de short read y short write completando siempre la cantidad de bytes.

## Manejo de concurrencia

El servidor crea un thread por conexion, por ende puede seguir aceptando clientes mientras procesa de forma concurrente los mensajes de las agencias aceptadas.

Se protege con un lock el acceso al archivo compartido de apuestas, para evitar que dos threads escriban al mismo tiempo o que uno lea mientras el otro escribe.

Las agencias que ya enviaron todas sus apuestas se guardan en un Set, despues se usa una Condition para poder esperar el quorum pedido por la consigna. los threads esperan sobre esta condicion y se despiertan con un notify_all cuando se alcanza el quorum.

Como lo pide la consigna, cada cliente recibe unicamente los ganadores correspondientes a la misma agencia.

## Manejo de archivos y memoria

Los clientes utilizan un bufio.scanner para leer los archivos linea por linea. Se conserva en memoria la linea actual y una copia del lote con las apuestas, pero que como mucho tendria una longitud de maximo BATCH_SIZE.

La salida tambien se escribe de forma incremental y los archivos de entrada y salida estan en volumenes, se pueden modificar y persistir sin reconstruir las imagenes de Docker.

## Cierre graceful

El cliente y el servidor manejan SIGTERM.

El cliente recibe la señal por un channel. Cuando la recibe se cierra el socket para desbloquear cualquier operacion de comunicacion que haya quedado y espera a que termine la propia goroutine de ejecucion del cliente.

El servidor usa un Event para indicar el cierre, tambien se cierra el socket principal que desbloquea la llamada de accept, se despiertan los threads que estaban esperando el quorum, se cierran los sockets de clientes y se realiza un join de los threads al final. Asi se liberan todos los recursos utilizados antes de que terminen los procesos.