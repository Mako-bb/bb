<h1 align="center">
  <img align="center"; src="https://s3.invisionapp-cdn.com/storage.invisionapp.com/boards/files/183060432.png?x-amz-meta-iv=1&x-amz-meta-ck=cd20ea812f8ae161523111afa5aea5e8&AWSAccessKeyId=AKIAWCDCF6QSLTS7LRWT&Expires=1630454400&Signature=KwA0MwTV%2FpNqzz72M50hjiTA3%2B4%3D"; width="40"; height="30">
    Agentes - Guía de inicio y buenas prácticas
</h1>

## 😄 Introducción
  En este espacio contamos lo más breve posible, todo lo que debe saber un/a desarrollador/a que esté trabajando dentro de este repositorio.

  En **BB** hay varios proyectos y desarrollos en constante crecimiento, pero solo las personas que trabajamos diariamente en este repositorio, somos parte de un equipo llamado **"Team Scraping"**. Es así, una manera en la cual nos suelen identificar dentro de la empresa.

  El elegante y distinguido **Team Scraping** se organiza y se comunica de la siguiente manera:
  - Usamos metodología **Scrum** y la herramienta [**ClickUp**](https://app.clickup.com/) para organizar nuestros proyectos. [**Más información haciendo click aquí.**](#)
  - Usamos **Microsoft Teams**. Tenemos canales y chats para seguir los temas relacionados a nuestros proyectos y el trabajo diario. [**Más información haciendo click aquí.**](#)
  - Usamos **Microsoft Sharepoint** para realizar posteos de novedades, cosas que queramos comunicar al equipo o el resto de la empresa. [**Más información haciendo click aquí.**](https://businessbureau0.sharepoint.com/sites/IT2)
    
  **Básicamente, en este repositorio trabajamos teniendo en cuenta tres puntos importantes:**
  1) Scrapear contenidos de plataformas **"On Demand"** de todo el mundo.
  2) Analizar la metadata de estos contenidos que obtenemos.
  3) Finalizar y automatizar la carga de datos.

  ***Es decir, realizamos un proceso ETL (Extract, transform and load).***

  Para el **1er** punto, antes que nada, es necesario esta leer y entender esta **Guía de inicio y buenas prácticas**.
  
  Para el **2do** punto, es necesario leer y entender la **Documentación para analizar metadata**.

  Y por último, para el **3er** punto, es necesario leer y entender la sección **Últimos detalles que debemos saber y mejora continua**.

  Estos **tres puntos importantes**, los explicamos paso a paso en el siguiente [**Índice**](https://gitlab.com/dondeloveo-for-business/agentes#%C3%ADndice) que tenemos más abajo leyendo este [README.md](https://gitlab.com/dondeloveo-for-business/agentes#agentes-gu%C3%ADa-de-inicio-y-buenas-pr%C3%A1cticas)

  Pero si sos nuevo recomendamos ver cada uno de los tutoriales que tenemos [**aquí**](https://businessbureau0.sharepoint.com/sites/IT2/SitePages/Capacitaciones.aspx) para que estes super integrado al equipo🤩.

## 📖 Índice

**1 - Guía de inicio y buenas prácticas**

1) [Aclaraciones importantes](https://gitlab.com/dondeloveo-for-business/agentes#aclaraciones-importantes)
2) [Instalación de nustro entorno de trabajo](https://gitlab.com/dondeloveo-for-business/agentes#instalación-de-nustro-entorno-de-trabajo)
3) [Convenciones del equipo y buenas prácticas](https://gitlab.com/dondeloveo-for-business/agentes#convenciones-del-equipo-y-buenas-prácticas)
4) [Crear un nuevo script](https://gitlab.com/dondeloveo-for-business/agentes#crear-un-nuevo-script)
5) [Cómo documentar un script](https://gitlab.com/dondeloveo-for-business/agentes#cómo-documentar-un-script)
6) [Otros comandos](https://gitlab.com/dondeloveo-for-business/agentes/-/tree/master#otros-comandos)
7) [Reset trial de apps](https://gitlab.com/dondeloveo-for-business/agentes#reset-trial-para-nosqlbooster-y-studio3t)
<br>

**2 - Documentación para analizar metadata**

Aquí definimos las necesidades del negocio.
  1) [Scrapings](https://gitlab.com/dondeloveo-for-business/agentes/-/blob/master/docs/scrapings.md)
  2) [Revisión Metadata](https://gitlab.com/dondeloveo-for-business/agentes/-/blob/master/docs/revision_metadata.md)
  3) [Analisis de datos con Excel](https://gitlab.com/dondeloveo-for-business/agentes#analizar-datos-con-excel)
  4) [Analisis de datos con Pandas](https://gitlab.com/dondeloveo-for-business/agentes#analizar-datos-con-pandas)

**3 - Últimos detalles que debemos saber y mejora continua**
  1) [Comandos y Procesos Titanlog](https://gitlab.com/dondeloveo-for-business/agentes/-/blob/master/docs/comandos.md)
  2) [Cómo solicitar VPN's](https://businessbureau0.sharepoint.com/sites/IT2/SitePages/VPN.aspx)
  2) [Gitflow - ¿Por qué trabajamos sobre platforms-dev?](https://gitlab.com/dondeloveo-for-business/agentes/-/blob/master/docs/gitflow.md)
  3) [Ideas](https://app.clickup.com/3043480/docs/2ww4r-178/2ww4r-381)

## 👨‍⚖️ Aclaraciones importantes

El código fuente de este repositorio es **propiedad de BB-Business Bureau** y está prohibida su difusión y/o utilización por intereses ajenos a la empresa.

Sabemos que muchos de nosotros somos desarrolladores, nerds, entusiastas, nos gusta el animé y **siempre vemos muchas cosas para mejorar**. Pero hoy _agentes_ creció un montón, y un mínimo cambio por fuera de estos archivos en los cuales indicamos desarrollar nuestro código, puede generar problemas en la operación diaria.
Es por esto, que las ideas de mejora, las estamos volcando en este [**link**](https://app.clickup.com/3043480/docs/2ww4r-178/2ww4r-381), para luego planificarlas y matarializarlas.

## 👩‍💻 Instalación de nustro entorno de trabajo

1) Clonar el repositorio.

Recomendamos hacerlo en una carpeta dentro de nuestra computadora, en donde organicemos nuestros archivos de trabajo.
Para clonar solo la rama de desarrollo **platforms-dev** usamos el comando:

**Git clone https://gitlab.com/dondeloveo-for-business/agentes.git -b platforms-dev --single-branch**

2) Luego, nos ubicarnos dentro del repositorio con nuestra terminal predilecta y, a la altura donde está ubicado este mismo archivo, en la carpeta clonada llamda **agentes** creamos un entorno virtual.

Hay varios entornos virtuales, pero en este caso recomendamos el que ofrece la documentación oficial de Python: https://docs.python.org/3/library/venv.html

Para crear el entorno virtual, ejecutamos el siguiente comando:

```shell
mi-nombre@pc123:~/Desktop/agentes$ python -m venv env
```
Luego accedemos al entorno virtual, lo corrobramos cuando vemos la terminal de esta manera:

```shell
(env) mi-nombre@pc123:~/Desktop/agentes$
```

3) Ahora ubicados dentro del entorno virtual, instalar las dependencias:

**Para desarrollo:**
```shell
(env) mi-nombre@pc123:~/Desktop/agentes$ pip install -r requirements/local.txt
```

**Importante:** Todas las librerías locales deben ir en requirements/local.txt . En caso de necesitar nuevas librerías en los servidores, comunicarse con los líderes del equipo.

**Para producción:**
```shell
(env) mi-nombre@pc123:~/Desktop/agentes$ pip install -r requirements/production.txt
```

4) Hay que instalar MongoDB y crear una base de datos local que escuche en el **puerto 27017**. Tambien se puede instalar un contenedor de Docker con un volumen para resguardar los datos (Esto último recomendado para desarrolladores más experimentados).

Link para instalar según el sistema operativo: https://docs.mongodb.com/manual/installation/

5) Por último hay que instalar un cliente para consumir los datos de MongoDB en localhost. Recomendamos instalar Studio 3T, pero se puede utilizar el que el desarrollador considere.

Link para instalar según el sistema operativo: https://studio3t.com/download/

## 👩‍🏫 Convenciones del equipo y buenas prácticas
**Idioma**
- Nombre de variables, métodos y clases en inglés.
- Documentación en castellano.
- Logs en castellano.

**Buenas prácticas**
- Debemos seguir las buenas prácticas para escribir código de [PEP8](https://www.python.org/dev/peps/pep-0008/)
- Variables que hagan referencia a lo que son.
- Código modular, separar en métodos por partes.
- Que el código lo entienda cualquiera.
- Documentación eficaz. Que cualquiera pueda entender el código.
- Utilizar el config.yaml y evitar escribir urls, tags, queries y otras cosas no mantenibles en el código.
- Que los logs indiquen el progreso de scraping de la plataforma.
- Utilizamos los mejores principios y buenas prácticas de programación como DRY, KISS, YAGNI y SOLID.

**Commits**
- Se debe dejar un breve y explícito mensaje en el commit. Deben ser en inglés. Se recomienda hacer un commit cada vez que se modifica un archivo, o si se modifican varios, hacer un resumen en el mensaje del commit.

  - Por ejemplo si creé el archivo pepito.py indicamos: **git commit -m "Create pepito.py**
  - Por ejemplo si modifiqué el archivo pepito.py indicamos: **git commit -m "Update pepito.py**
  - Por ejemplo si eliminé el archivo pepito.py indicamos: **git commit -m "Delete pepito.py**

**Rama platforms-dev**

El equipo **solo puede trabajar en esta rama** y modificar **solo** los siguientes archivos:
- Archivos dentro de la **carpeta platforms-dev**
- config.yaml

**Rama master**
- Solo personas autorizadas pueden crear versiones en esta rama. El resto del equipo debe trabajar en la rama **platforms-dev**.

***IMPORTANTE:*** Cambios en cualquier otro archivo del repositorio no serán aceptados sin autorización de los líderes del equipó. Los nuevos features se organizan con líderes. Se escuchan muchas propuestas, pero es necesario evaluar la necesidad y factibilidad de nuevo código **(Principio YAGNI)**.

## 👨‍🍳 Crear un nuevo script

Nos toca scrapear una plataforma que se llama **Pepito**, entonces...

1) Indicamos en el archivo **config.yaml** los datos básicos de la plataforma a la altura de **ott_sites**, por ejemplo:

```yaml
ott_site:
  Pepito:
    countries:
      US: us.pepito # Es el PlatformCode -> Es muy importante validar este PlatformCode con los líderes del equipo.
    api_url: https://www.pepito.com/v3/api/
```

2) Crear un archivo nuevo en **agentes/platforms**. El nombre del archivo tiene que referir obviamente al sitio al cual vamos a scrapear. Por ejemplo: **pepito.py**.

**IMPORTANTE:** Si el nombre de la plataforma está compuesta **por más de una palabra**, no usar **snake case** espaciando con **"_"** y unimos todo.
Por ejemplo:

La plataforma se llama **"Pepito America"**
- Nombre correcto para el archivo: **pepitoamerica.py**
- Nombre **incorrecto** para el archivo: **pepito_america.py**

3) **Es muy importante** que el nombre de la clase sea igual al nombre del archivo sin **.py** ni  y que esté en **CamelCase**. Por ejemplo el nombre de la clase dentro del script **pepito.py** puede ser:

```python
class Pepito()
    pass
```

Ejemplo si la plataforma esta compuesta **por más de una palabra**:
Si la plataforma se llama **"Pepito America"**
- Nombre **correcto** para la clase: 
```python
class PepitoAmerica()
    pass
```
- Nombres **incorrectos** para la clase:
  - **pepitoAmerica**
  - **Pepito_America**
  - **PEPITOAMERICA**

4) Ahora debemos escribir las siguientes líneas de código.

```python
# -*- coding: utf-8 -*-
import json
import time
import requests
from common import config
from updates.upload import Upload
from handle.replace import _replace
from handle.mongo import mongo
# Traer solo las librerías necesarias.

class Pepito():
    """
    Pepito es una ott de Estados Unidos.
    """
    def __init__(self, ott_site_uid, ott_site_country, operation):
        self.config = config()['ott_sites'][ott_site_uid]
        self.created_at = time.strftime("%Y-%m-%d")
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes = config()['mongo']['collections']['episode']
        self.mongo = mongo()
        self.platform_code = self.config['countries'][ott_site_country].lower()        
        # Completar el constructor de la clase!!!

        if operation == 'return':
            params = {"PlatformCode" : self.platform_code}
            last_item = self.mongo.lastCretedAt(self.titanScraping, params)
            if last_item.count() > 0:
                for last_content in last_item:
                    self.created_at = last_content['CreatedAt']
            self._scraping()
        
        elif operation == 'scraping':
            self.scraping()

        elif operation == 'testing':
            self.scraping(testing=True)

    def scraping(self, testing=False):
        # Escribir el código aqui!!!

        print("¡Probando!")

        # Este objeto es importante para verificar la información que se almacene en
        # MongoDB local.
        Upload(self.platform_code, self.created_at, testing=testing)
```

Este script **pepito.py** se instancia en **main.py** con el nombre de su clase, pero
cambiada a **"lowercase"**.

5) Por último, para ejecutarlo, debemos realizar el siguiente comando:

```shell
(env) mi-nombre@pc123:~/Desktop/agentes$ python main.py Pepito --c US --o testing
```
Si luego de ejecutar vemos **¡Probando!** en la terminal, ¡Todo se instaló correctamente!

## 👩‍🔧 Cómo documentar un script

El código escrito en python en sí, ya es bastante legible, pero sin duda, documentar el código nos ahorrará responder muchas preguntas a futuro.

El equipo documenta el script dentro de la clase de la siguiente manera:

```python
# -*- coding: utf-8 -*-
import json
import time
import requests
from handle.mongo import mongo

class Pepito():
    """
    Pepito es una ott de Estados Unidos.

    DATOS IMPORTANTES:
    - Versión Final: Si/No (En desarrollo).
    - VPN: Si/No (Recomendación: Usar ExpressVPN).
    - ¿Usa Selenium?: Si/No.
    - ¿Tiene API?: Si/No.
    - ¿Usa BS4?: Si/No.
    - ¿Se relaciona con scripts TP? Si/No. ¿Cuales?
    - ¿Instanacia otro archivo de la carpeta "platforms"?: Si/No. ¿Cuales?
    - ¿Cuanto demoró la ultima vez? tiempo + fecha.
    - ¿Cuanto contenidos trajo la ultima vez? cantidad + fecha.

    OTROS COMENTARIOS:
    Con esta plataforma pasa lo siguiente...
    """
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config = config()['ott_sites'][ott_site_uid]
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
```
Documentamos los métodos de la siguiente manera:
```python
    def get_payloads(self, pais, cod_idioma, datos_contenido):
        """Obtiene el payload del contenido. # Indicar que hace el método.

        Args:
            # Indicar argumentos y tipo de datos.
            pais (str): Indica el país del contenido.
            cod_idioma (str): Indica el idioma del contenido.
            datos_contenido (dict): Diccionario con toda la información del contenido.

        Returns:
            # En caso de retornar algo, indicar qué retorna.
            list: Retorna el payload en una lista.
        """
        contenidos = []

        return contenidos
```

## 👨‍🔧 Otros comandos

Estos se utilizan en proyectos particulares adhoc.

* IMDb Match And Piracy Agents Update `python main.py Work --o adhoc --t allPiracy`
* IMDb Match Companies Only `python main.py Work --o adhoc --t PiracyM` 
* Scraping Piracy `python main.py Piracy --o piracy` 

## 👨‍💻 Reset trial de apps

Hay un script presente en agentes que _resetea_ el trial de los siguientes programas:
* Studio3T
* NoSQLBooster
* Datagrip

Este script funciona tanto para Windows, Linux y Mac.


## Uso:
El comando sería el siguiente:
```bash
python resetdb.py --p <program>
```

Donde ___\<program\>___ debe ser cualquiera de las siguientes opciones. La opción ___all___ elije todos.
  - studio3t
  - s3t
  - nosqlbooster
  - nsb
  - datagrip
  - dg
  - all


Por ejemplo, para reiniciar Studio3T, escribir lo siguiente:
```bash
python resetdb.py --p s3t
```

### NOTAS:
  - _**IMPORTANTE**_: Esto borrará las conexiones y configuraciones creadas en todos los programas. Exportar las conexiones existentes desde cada app antes de ejecutar el camando previo.
  - En Windows puede que se tenga que ejecutar 2 veces el script(1 como usuario normal y otro como administrador) en el caso que se elija Studio3T porque tiene alojado datos en el __registro de Windows__ que están disponibles en múltiples registros.
  - Con NoSQLBooster funciona temporalmente. Falta corregirlo.
  - El crack de Studio3T para Mac no funciona perfectamente en algunos casos. Falta corregirlo.
<br>

## 🕵️ Analizar datos con Excel

Antes de terminar una plataforma, es importante revisar la metadata. Para exportar un excel que tenga hojas con las colecciones **titanScraping**, **titanScrapingEpisodes** (que estan en mi localhost de mongo) y **apiPresence** (que está en misato), puedo ejecutar los comandos que veremos a continuación. Para ejecutar esto, es necesaro tener instalada la librería **pandas** en local.

Por ejemplo, si la plataforma se llama **Pepito** y el país a analizar es **US**:

**Debo ejecutar en la terminal:** `python main.py Pepito --c US --o excel`

Ahora, si quiero traer una fecha en particular, por ejemplo, datos que tengo en mi localhost de mongo del 01/03/2020...

**Debo ejecutar en la terminal:** `python main.py Pepito --c US --o excel --date 2020-03-01`

Si no indico fecha, me trae datos del día. De **apiPresence** trae la última actualización.

Una vez exportado el excel, puedo aplicar funciones, crear nuevas hojas, hacer tablas dinámicas, o cualquier cosa que se me ocurra para analizar y validar los datos de una plataforma. La extension **.xlsx** está ignorada en **.gitignore**.

**Por último**, Es importante que se agrege una hoja llamada **"análisis"** y aquí dar un detalle de la metadata que trae un script.

[Click aquí para ver un mini tutorial de análisis de datos con Excel](#)

## 🕵️ Analizar datos con Pandas

**Importante:** Si nunca creé un notebook, explicamos cómo instalar **Jupyter Notebook** haciendo click [aquí](https://gitlab.com/dondeloveo-for-business/agentes#instalar-jupyter-notebook). 

Aquí explicamos cómo crear **Dataframes** de mongo muy fácil, y analizarlos con **Jupyter**, **Jupyter Lab**, **Spyder** o con el *notebook/entorno* que más me guste.

Recomendamos ejecutar el *notebook/entorno* favorito sobre la carpeta **analysis**, que está dentro de este repositorio **agentes**.

Para traer un **DataFrame** de **localhost > titan** ejecuto:
```python
from utils import GetDataFrame
query = {"PlatformCode":"us.pepito", "CreatedAt": "2020-03-01"}
df = GetDataFrame.local(query, collection='titanScraping')
df
```
Si quiero traer un **DataFrame** de **kaji > bussines**, debo usar el método **kaji**:
```python
df = GetDataFrame.kaji(query, collection='titanScraping')
```
Traer un Dataframe de **misato > bussines**, debo usar el método **misato**:
```python
df = GetDataFrame.misato(query, collection='apiPresence')
```
Donde **query** debe ser un diccionario, y el argumento **collection** debe ser una colección de la base de datos de mongo.

Listo, ahora puedo analizar o comparar DataFrames. La extension **.ipynb** y las carpetas **.ipynb_checkpoints** están ignoradas en **.gitignore**.

**Tip:** Puedo usar regex para hacer consultas, por ejemplo, mi **query** puede ser:

```python
query = {'PlatformCode' : { '$regex' : '.pepito', '$options' : 'i' } }
```

Al finalizar el análisis, se puede importar a html con el comando: `jupyter nbconvert --to (formato)archivo.ipynb`

[Click aquí para ver un mini tutorial de análisis de datos con Pandas](#)

## 🐍 Instalar Jupyter Notebook

Antes que nada, debemos tener el entorno virtual activado y posicionarnos sobre la carpeta analysis:

```shell
(env) mi-nombre@pc123:~/Desktop/agentes/analysis$
```

Es importante tener instaladas las librerías **pandas** y **jupyter** en nuestro entorno virtual.

Ahora debo ejecutar **jupyter notebook** en la terminal:

```shell
(env) mi-nombre@pc123:~/Desktop/agentes/analysis$ jupyter notebook
```
Una vez levantado el servidor, debemos ingresamos al link que inicia con **http://localhost:8888/**.

Creamos un nuevo notebook:
<p>
  <img align="center"; src="https://www.programaenpython.com/wp-content/uploads/2019/03/jupyter_notebook_menu_new.png"; width="110"; height="120">
</p>

¡Ya podemos trabajar!
<p>
  <img align="center"; src="https://www.programaenpython.com/wp-content/uploads/2019/03/jupyter_notebook_blank-1024x190.png"; width="800"; height="150">
</p>