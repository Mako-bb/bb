# Amazon

## Explicacion General
La dificultad que conlleva scrapear Amazon es tal que hay que tomarlo como una app y no como  un scraping mas.

El codigo originalmente se fue pensado para que scrapeara todos los paises, empezando por USA y despues escalado a 
Japon, UK e India, es por eso que se usan variables globales, ya que la plataforma en sus version Amazon y PrimeVideo
tienen la mismas apis, HTML, distribucion, todo.

Siguiendo con lo dicho mas arriba, dado que el codigo es igual para todos los paises, salvo por un par que necesita
loguearse, pero salvo eso, todo funciona exactamente igual y lo hace bien, por lo que el mas minimo cambio va a tener
que testearse en todos los paises donde se corra.

Los links usados para el scraping quedan en el yaml que esta dentro de este mismo path.

Las variables globales que se usan son para la moneda, el dominio web y el pais ya que como explique mas arriba, la 
logica es la misma para todos, asi que esto hace que no sea necesario hardcodear absolutamente nada.

PD: debido a que el codigo original de Amazon no traia todo, lo que se hizo fue implementar un scraping de JW para 
conseguir mas links, dado por resultado los +200K links que consigue. Actualmente esa parte funciona unicamente para 
USA, por lo que hay que escalar eso nomas para que funcione para el resto de los paises

## Amazon sin login
El scraping arranca por el home, obteniendo una api y token que contiene casi todo el contenido de la pagina (no de la 
plataforma) y tambien scrollea la misma para asegurarse de conseguir lo maximo posible.

Eso no es todo, ya que como se puede apreciar [aca](https://www.amazon.com/gp/video/storefront/ref=topnav_storetab_atv?node=2858778011)
hay un monton de colecciones a las que se pueden entrar tocando el boton See More. Una vez conseguidas todas, o la mayor
cantidad posible, se ingresa a dicha coleccion, se obtiene un token y llama a la api correspondiente y empieza a iterar
para conseguir absolutamente todo el contenido de los mismos. Un ejemplo de coleccion es 
[este](https://www.amazon.com/gp/video/search/ref=atv_hm_hom_1_c_innhq1_5_smr?queryToken=eyJ0eXBlIjoicXVlcnkiLCJuYXYiOnRydWUsInBpIjoiZGVmYXVsdCIsInNlYyI6ImNlbnRlciIsInN0eXBlIjoic2VhcmNoIiwicXJ5IjoiYXZfa2lkX2luX3RlcnJpdG9yeT1VUyZmaWVsZC13YXlzX3RvX3dhdGNoPTEyMDA3ODY1MDExJnBfbl9lbnRpdHlfdHlwZT0xNDA2OTE4NTAxMSZhZHVsdC1wcm9kdWN0PTAmYnE9KGFuZCAoYW5kIGZpZWxkLWlzX3ByaW1lX2JlbmVmaXQ6JzEnIChub3QgKG9yIGdlbnJlOidhdl9nZW5yZV9lcm90aWMnIGF2X3ByaW1hcnlfZ2VucmU6J2F2X2dlbnJlX2Vyb3RpYycpKSkgKGFuZCAob3IgZ2VucmU6J2F2X2dlbnJlX2tpZHMnIGF2X3ByaW1hcnlfZ2VucmU6J2F2X2dlbnJlX2tpZHMnKSBlbnRpdHlfdHlwZTonVFYgU2hvdycpKSZmaWVsZC1pc19wcmltZV9iZW5lZml0PTEmc2VhcmNoLWFsaWFzPWluc3RhbnQtdmlkZW8mcXMtYXZfcmVxdWVzdF90eXBlPTQmcXMtaXMtcHJpbWUtY3VzdG9tZXI9MiIsInJ0IjoiaU5OaHExc21yIiwidHh0IjoiS2lkcyBhbmQgZmFtaWx5IFRWIiwib2Zmc2V0IjowLCJucHNpIjowLCJvcmVxIjoiMWNjOTk2YjctMjZmNC00MGMwLWJlOGItZThiMDhjNWNiMWU0OjE2MjgyNTcxMDcwMDAiLCJzdHJpZCI6IjE6MTFHWEhRV0dLQUU2VDEjI01aUVdHWkxVTVZTRUdZTFNONTJYR1pMTSIsIm9yZXFrIjoiTGk2K28vZ3NoMGhHQ2NUZ2FUZ0tMem1iQXp0em5nb3NvZUkwNnphaGZkST0iLCJvcmVxa3YiOjF9&pageId=default&queryPageType=browse&ie=UTF8).

El proceso de arriba tambien se repite con el Store ya que la logica es exactamente la misma.

Con los canales, debido a que son muchos, hay una funcion que los consigue a todos desde el HTML y en base a eso logra
entrar a los mismos y, de nuevo, consigue todas las colecciones de cada canal, ingresa a los mismos, consigue la api y
obtiene todos los contenidos. A diferencia del store y home, aca scrapea las colecciones un canal a la vez.

Este proceso tarda muchas horas y lo que hace es conseguir la mayor cantidad de links posibles. Dado que no se puede
usar BS4, porque la pagina te detecta como bot, lo hace todo por Selenium. Solo en USA, estamos hablando de arriba de
200K links y la ultima vez que se corrio, tardo dos semanas en hacerlo.

## Amazon con login
El login tiene que hacerse de una cuenta especifica por pais porque sino te muestra el contenido totalmente distinto al
que deberia. Esto quiere decir que si se va a usar en India, tiene que usarse una cuenta de dicho pais.

A diferencia de Amazon sin login, en PrimeVideo lo que se hace es scrollear todas las colecciones en vez de llamar a las
apis. El resto del codigo para conseguir la metadata es el mismo.


## Metadata
Los requerimientos originales eran que todos los contenidos tenian que tener todos los platformcodes correspondientes.
Eso quiere decir que si una pelicula como [esta](https://www.amazon.com/gp/video/detail/B08P7ZR4HR/ref=atv_hm_hom_1_c_D4dtpS_4_3)
tiene que tener como platformcode us.amazon-prime y us.amazon. Esto significa que por cada link que scrapea, 
hace ese analisis y lo ingresa a la DB asi como viene. No importa si se vuelve a repetir mas adelante, la cantidad de 
informacion que se maneja en esta plataforma es tan absurdamente grande que ni vale la pena fijarse mientras se scrapea.

La metadata se consigue desde un diccionario que esta en el HTML tanto para series y peliculas como capitulos, aunque
el diccionario es distinto

En el caso de las series, tienen precio por capitulo y temporada, y dado que nunca se decidio que precio tienen que 
llevar no lleva ninguno, por lo que el package queda como transaction-vod. Tambien hay que aclarar que en muchos casos
puede pasar de que una serie sea por suscripcion pero tenga un par de capitulos gratis o para comprar, por lo que en 
esas situaciones lo que se hace es repetir la serie con distinto platformcode y que tengan unicamente los capitulos con
distinto package. 

Ejemplo, supongamos que tenemos Vikings. Toda la serie es por suscripcion a PrimeVideo y tiene 3 capitulos de la primer 
temporada que estan gratis. En casos como este, la serie completa tendria platformcode us.amazon-prime y us.amazon 
unicamente con esos 3 capitulos.