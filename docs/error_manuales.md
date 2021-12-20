# Error - Manuales

Se listan los criterios para definir el motivo por el cual vamos a completar el campo "titanLog - Error" en las tareas de Manuales que no pudieron ser ejecutadas en primera instancia sin fallas.

  

**Diferencias:**

_Ejemplo_: Mucha diferencia: Scraping 3907 / Presence 3234

_Ejemplo_: Cantidad que pasa a inactive supera el limite (133, max 83)

  

**VPN:**

_Ejemplo_: No esta disponible el país al que debemos conectarnos en ningún cliente VPN

_Ejemplo_: El país está disponible pero no esta tomando correctamente la ubicación el sitio

  

**Conexión:**

_Ejemplo_: el script se detiene informando error de TimeOut

  

**Packages:**

_Ejemplo_: doc\[Packages\] - valor incorrecto: \[\] -> Id: "680836"

_Ejemplo_: doc\[Packages\]\[1\]\[RentPrice\] - valor incorrecto: 0.0 -> Id: "1624870509"

_Ejemplo_: doc\[Packages\]\[1\] - falta el precio -> Id: "1624870509"

  

**HTTPS Error:**

_Ejemplo_: Error 502 al ingresar al sitio

_Ejemplo_: Error 1005 al ingresar al sitio

_Ejemplo_: Error 403 al realizar las requests

  

**JSON Error:**

_Ejemplo_: Exception/Error: KeyError: 'data' -> assets = resp.json()\['data'\]

_Ejemplo_: Exception/Error: AttributeError: 'NoneType' object has no attribute 'json' -> data = response.json()

  

**Script:**

_Ejemplo_: el script se detuvo por algún error en variables o en su lógica. También es valido cuando se detecta que el script entró en bucle infinito.