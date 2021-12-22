# Comandos y Procesos Titanlog

Colecciones:
------------

_IMPORTANTE: A excepción de 'titanLog' y 'updateLog' (en el que solo debieran eliminar los documentos revisados) ninguna de estas colecciones debe ser modificada manualmente. Solo las pueden utilizar en modo lectura._

  

**apiPlatforms:** Colección en donde se encuentra la información básica de cada plataforma, activa o inactiva en el API.

  

**apiPresence:** Colección en donde se muestran los contenidos unificados (con los episodios) de una plataforma diferenciándolos por tres _Status_ : active, inactive y new.

  

Cuando una tarea tiene el tag _titanlog_ debemos buscar los documentos que indiquen los errores en las siguientes colecciones:

  

*   **titanLog:** La mayoría de los errores generados en una plataforma se guardan acá. Puede tener errores de datos específicos o, si el script "se rompió", hay que revisar los logs completos usando el comando que se detalla más abajo.
*   **updateLog:** Cuando al hacer el _update_ encuentra nuevos errores. Hasta ahora trae dos tipos de errores: indicando duplicados o que supere el limite de nuevos inactivos permitido.
*   **updateDups:** Cuando en updateLog figura "Cantidad de UIDs duplicados supera el limite (n1, max n2)" se pueden chequear acá dichos duplicados.

  

**titanStats:** Colección que guarda un historial de todas las veces que se subió a Misato un scraping. Es de gran ayuda cuando queremos saber si ya subimos a Misato nuestro scraping o no. Cuando Imported = true, significa que los datos ya pasaron por el proceso de actualización y o bien ya están en el API o lanzaron algún error.

  

**updateStats:** Colección que muestra estadísticas de diferente tipo evaluados _durante_ el update/actualización de una plataforma. Podemos acudir a esta colección cuando queremos asegurarnos de que x plataforma terminó de actualizar o actualizó correctamente.

  

**last\_update:** Colección que se actualiza _cada_ 10 minutos indicando la última vez que actualizó (update) cierta plataforma

  

  

Servers:
--------

  

**Misato:** server de producción.

  

**Kaji:** server de prueba para el API y las actualizaciones. No se realizan scrapings en este server.

  

  

**Comandos:**
-------------

  

### **Upload:**

_Comando que se utiliza para que revise los datos con todas las validaciones independientemente de si se haya corrido o no ese día el scraping de la plataforma en cuestión._

  

python updates/upload.py --platformcode xx.xxxxx --createdat YYYY-MM-DD

  

_\*Si no se le pasa ningún parámetro adicional, se corre en modo_ _testing_

  

**_Parámetros adicionales:_**

  

**\--platformcode ó -p**

_Obligatorio. se pasa el platform code de la plataforma_

  

**\--createdat ó -c**

_Se pasa la fecha en la que se corrió el scraping. Si no se agrega este parámetro, por default toma la fecha actual_

  

**\--upload ó -u**

_para subir a Misato o Kaji_

  

**\--bypass**

_para 'avisar' al update que trae mas o menos contenidos de lo normal. Este parámetro solo debe ser agregado si fue requerido por Maribel o Miguel_

  

**\--noepisodes**

_cuando la plataforma trae series sin episodios_

  

**\--server 1**

_para subir a Misato. Por default, si no se agrega este parámetro pero si el de --upload, se sube a Misato igualmente._

  

**\--server 2** **ó -s**

_para subir a KAJI_

  

  

### **Logs**

_Para ver los logs de las plataformas en los servers cuando en titanlog les aparece este mensaje: "\[ROOT\] : An error has ocurred before the upload_"

  

\- Para mostrar las ultimas 100 lineas del ultimo log se usa:

<Platform> --c <country> --o log

  

\- Para mostrar las ultimas 100 lineas de una fecha determinada:

<Platform> --c <country> --o log --date <día en formato YYYYMMDD o YYYY-MM-DD>

  

\- Para descargar el ultimo log:

<Platform>--c <country> --o logd

  

\- Para descargar el log de una fecha determinada:

<Platform> --c <country> --o logd --date <día en formato YYYYMMDD o YYYY-MM-DD>

  

\*<Platform> : nombre de la clase del script. Ejemplos: Google , Playbrands, Rakuten, etc.

  

  

**Makefile**
------------

Es un archivo de texto que se encuentra en la carpeta _agentes_ el cual contiene una serie de '**targets**' que se lo utilizan para abreviar los comandos previos. Serían más que nada atajos.

Por lo general se puede usar en Linux/Mac teniendo **make** instalado en el Sistema. Para poder usarlo en Windows hay que descargar MSYS2 y luego agregar la ruta de los binarios en el **PATH**.

  

### **Algunos ejemplos****:**

Para realizar el scraping de una plataforma normalmente se escribe:

```
python main.py --o scraping <Platform> --c <country>
```

Pero se puede hacer lo mismo de forma más sencilla con:

```
make scraping <Platform> <country>
```

  

### Lista de otros comandos disponibles con ejemplos:

*   **testing:** _Para realizar las operaciones del scraping en modo testing_

```
make testing Netflix US
```

  

*   **return:** _Para continuar con el último scraping que no se llegó a terminar._

```
make return Netflix US
```

  

*   **pyracy:** _Obtiene datos de una plataforma que aloja contenido pirata._

```
make piracy <piracyProvider>
```

  

*   **comprobe:** Sirve para simular que lo sube a misato pero se lo utiliza para comprobar si el scraping se realizó de forma correcta y que los payloads no presentan inconsistencias.

```
make comprobe us.netflix 2020-11-25
```

  

*   **upload\_misato** ó **misato:** _Como su nombre lo indica, sube el scraping a misato._

_La forma típica de subirla a misato es:_

```
python updates/upload.py --platformcode us.netflix --createdat 2020-11-25 --upload
```

_Pero es más práctico:_

```
make misato us.netflix 2020-11-25
```

  

*   **upload\_kaji** ó **kaji:** _Subirá al server de pruebas._

```
make kaji us.netflix 2020-11-25
```

  

*   **bypass:** _Subirá a misato con este modo en los casos que el scraping haya traído una gran diferencia(mayor) de cantidad con respecto a lo que ya había._

```
make bypass us.netflix 2020-11-25
```

  

*   **log\_checker** ó **logs:** _Mostrará por terminal/consola el último log ó el de una fecha específica (opcional) de una plataforma determinada._

```
make logs Netflix US 2020-11-25
```

  

*   **log\_downloader** ó **logd:** _Descargará el último log o el que se le especifique por una fecha(opcional) de una plataforma determinada. Se descargará en la carpeta_ **_`log`_**_._

```
make logd Netflix US 2020-11-25
```