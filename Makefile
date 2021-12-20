SHELL=/bin/bash
PYTHON=python
MAIN=main.py
RESET=handle/resettrial.py
UPLOAD=updates/upload.py
DEPLOY_DOCKER=build/deploy_docker.sh
ROOT=root/root.py
DATE := $(shell date +"%Y-%m-%d")
COMPOSE_FILE=build/docker-compose.yml
CONTAINER_NAME=agentes
ARGS=$(filter-out $@,$(MAKECMDGOALS))
ARG3=$(word 3,$(ARGS))


.PHONY: scraping testing return justwatch piracy comprobe upload_misato upload_kaji bypass root logs logd misato kaji

default: help

help:
	@echo "USAGE:"
	@echo -e "  make \033[1m[TARGET]\033[0m \033[1m[ARGS]\033[0m\n"
	@echo
	@echo "TARGETS:"
	@echo -e "  scraping\t\tRealiza el scraping de una plataforma. \t\t\tmake scraping PLATFORM COUNTRY"
	@echo -e "  testing\t\tRealiza el scraping en modo testing. \t\t\tmake testing PLATFORM COUNTRY"
	@echo -e "  return\t\tContinúa el scraping desde una fecha anterior. \t\tmake return PLATFORM COUNTRY"
	@echo -e "  jwscraping\t\tRealiza el scraping de una plataforma usando JW. \tmake jwscraping PROVIDER COUNTRY"
	@echo -e "  jwtesting\t\tRealiza el scraping en modo testing. \t\t\tmake jwstesting PROVIDER COUNTRY"
	@echo -e "  jwreturn\t\tContinúa el scraping desde una fecha anterior. \t\tmake jwsreturn PROVIDER COUNTRY"
	@echo -e "  comprobe\t\tSimula que sube para comprobar los payloads. \t\tmake comprobe PLATFORMCODE CREATEDAT"
	@echo -e "  misato, upload_misato Sube el scraping a Misato. \t\t\t\tmake upload_misato PLATFORMCODE CREATEDAT"
	@echo -e "  kaji, upload_kaji \tSube el scraping a Kaji. \t\t\t\tmake upload_kaji PLATFORMCODE CREATEDAT"
	@echo -e "  revision\t\tSube a Kaji pero para una revisión. \t\t\tmake revision PLATFORMCODE CREATEDAT"
	@echo -e "  bypass\t\tSube a Misato pero con el flag de bypass. \t\tmake bypass PLATFORMCODE CREATEDAT"
	@echo -e "  logs, logchecker\tMuestra las últimas línea del log de una plataforma. \tmake logs PLATFORM COUNTRY CREATEDAT"
	@echo -e "  logd, logdownload\tDescarga los logs de una plataforma. \t\t\tmake logd PLATFORM COUNTRY CREATEDAT"
	@echo -e "  root\t\t\tScript principal para ejecutar plataformas. \t\tmake root"
	@echo -e "  root-since\t\tEjecuta el root desde un país específico. \t\tmake root-since COUNTRY"
	@echo -e "  reset\t\tEjecuta el script que reinicia el trial de algunas apps. \t\tmake reset APP"
	@echo
	@echo "ARGS:"
	@echo -e "  PLATFORM\t\tNombre de la clase de la plataforma"
	@echo -e "  PLATFORMCODE\t\tPlatformCode de una plataforma"
	@echo -e "  PROVIDER\t\tParámetro utilizado para obtener el scraping de una plataforma en JustWatch"
	@echo -e "  COUNTRY\t\tPaís para realizar el scraping. Ej: US, AR, DE, etc."
	@echo -e "  CREATEDAT\t\tFecha del scraping."
	@echo -e "  APP\t\t\tEn el caso de usar el script que reinicia el trial. Valores posibles: s3t, nsb, dg"
	@echo 
	@echo "EXAMPLES:"
	@echo "  make"
	@echo "  make help"
	@echo "  make scraping Itunes US"
	@echo "  make comprobe tr.blutv 2021-05-01"
	@echo "  make upload_misato tr.blutv 2021-05-01"
	@echo "  make bypass tr.blutv 2021-05-01"
	@echo "  make revision us.netflix 2021-05-01"
	@echo "  make logs Microsoft MX"
	@echo "  make root"

# Uso: make scraping <platform> <country>
scraping: $(MAIN)
	python $(MAIN) --o scraping $(word 1,$(ARGS)) --c $(word 2,$(ARGS))

# Uso: make testing <platform> <country>
testing: $(MAIN)
	python $(MAIN) --o testing $(word 1,$(ARGS)) --c $(word 2,$(ARGS))

# Uso: make return <platform> <country>
return: $(MAIN)
	python $(MAIN) --o return $(word 1,$(ARGS)) --c $(word 2,$(ARGS))

# Exportar un excel con los datos de una plataforma
# make excel <platform> <country>
# Exportar un excel con los datos de una plataforma obtenidos en una fecha determinada
# make excel <platform> <country> <YYYY-MM-DD>
excel: $(MAIN)
	@if [[ ! -z "$(ARG3)" ]]; then \
		echo python $(MAIN) --o excel $(word 1,$(ARGS)) --c $(word 2,$(ARGS)) --date $(ARG3); \
		python $(MAIN) --o excel $(word 1,$(ARGS)) --c $(word 2,$(ARGS)) --date $(ARG3); \
	else \
		echo python $(MAIN) --o excel $(word 1,$(ARGS)) --c $(word 2,$(ARGS)); \
		python $(MAIN) --o excel $(word 1,$(ARGS)) --c $(word 2,$(ARGS)); \
	fi;

# Uso: make jwscraping <provider> <country>
jwscraping: $(MAIN)
	python $(MAIN) JustWatch --o jwscraping --provider $(word 1,$(ARGS)) --c $(word 2,$(ARGS))

# Uso: make jwtesting <provider> <country>
jwtesting: $(MAIN)
	python $(MAIN) JustWatch --o jwtesting --provider $(word 1,$(ARGS)) --c $(word 2,$(ARGS))

# Uso: make jwreturn <provider> <country>
jwreturn: $(MAIN)
	python $(MAIN) JustWatch --o jwreturn --provider $(word 1,$(ARGS)) --c $(word 2,$(ARGS))

# Uso: make piracy <name>
piracy: $(MAIN)
	python $(MAIN) --o piracy $(word 1,$(ARGS))

# Uso: make comprobe xx.xxxx 9999-99-99
# Ej: make comprobe us.netflix 2020-03-25
comprobe: $(UPLOAD)
	python updates/upload.py --platformcode $(word 1,$(ARGS)) --createdat $(word 2,$(ARGS))

# Subirá por defecto a Misato
# Ej: make misato us.netflix 2020-03-25
misato upload_misato: $(UPLOAD)
	python updates/upload.py --platformcode $(word 1,$(ARGS)) --createdat $(word 2,$(ARGS)) --upload

# Subirá al server de Kaji
# Ej: make kaji us.netflix 2020-03-25
kaji upload_kaji: $(UPLOAD)
	python updates/upload.py --platformcode $(word 1,$(ARGS)) --createdat $(word 2,$(ARGS)) --upload --server 2

# Subirá al server de Kaji para una revisión
# Ej: make revision us.netflix 2020-03-25
revision review : $(UPLOAD)
	python updates/upload.py --platformcode $(word 1,$(ARGS)) --createdat $(word 2,$(ARGS)) --upload --server 2 --titan

# Ej: make bypass us.netflix 2020-03-25
# Sube a misato con el el flag de bypass
bypass: $(UPLOAD)
	python updates/upload.py --platformcode $(word 1,$(ARGS)) --createdat $(word 2,$(ARGS)) --upload --bypass

# Se asume que el nombre del root está 
# definido en la variable de entorno DLV_ROOT_NAME
# make root
# make root GB2
root: $(ROOT)
	@if [[ ! -z "$(word 1,$(ARGS))" ]]; then \
		echo python root.py --l $(word 1,$(ARGS)) --date $(DATE); \
		python root.py --l $(word 1,$(ARGS)) --date $(DATE); \
	else \
		echo python root.py --date $(DATE); \
		python root.py --date $(DATE); \
	fi;

# Ej: make root-since AR
root-since: $(ROOT)
	@if [[ ! -z "$(word 2,$(ARGS))" ]]; then \
		echo python root.py --l $(word 1,$(ARGS)) --s $(word 2,$(ARGS)) --date $(DATE); \
		python root.py --l $(word 1,$(ARGS)) --s $(word 2,$(ARGS)) --date $(DATE); \
	else \
		echo python root.py --s $(word 1,$(ARGS)) --date $(DATE); \
		python root.py --s $(word 1,$(ARGS)) --date $(DATE); \
	fi;

# Ver las última 100 líneas del último log de una plataforma:
# make logs Netflix US
# Ver las última 100 líneas de una fecha determinada:
# make logs Netflix US 2020-11-25
logchecker logs: $(MAIN)
	@if [[ ! -z "$(ARG3)" ]]; then \
		echo python $(MAIN) --o log $(word 1,$(ARGS)) --c $(word 2,$(ARGS)) --date $(ARG3); \
		python $(MAIN) --o log $(word 1,$(ARGS)) --c $(word 2,$(ARGS)) --date $(ARG3); \
	else \
		echo python $(MAIN) --o log $(word 1,$(ARGS)) --c $(word 2,$(ARGS)); \
		python $(MAIN) --o log $(word 1,$(ARGS)) --c $(word 2,$(ARGS)); \
	fi;

# Para descargar el último log de una plataforma:
# make logdownload Netflix US
# Para logd de una fecha determinada:
# make logd Netflix US 2020-11-25
logdownload logd: $(MAIN)
	@if [[ ! -z "$(ARG3)" ]]; then \
		echo python $(MAIN) --o logd $(word 1,$(ARGS)) --c $(word 2,$(ARGS)) --date $(ARG3); \
		python $(MAIN) --o logd $(word 1,$(ARGS)) --c $(word 2,$(ARGS)) --date $(ARG3); \
	else \
		echo python $(MAIN) --o logd $(word 1,$(ARGS)) --c $(word 2,$(ARGS)); \
		python $(MAIN) --o logd $(word 1,$(ARGS)) --c $(word 2,$(ARGS)); \
	fi;

# Reinicia el trial de algunas apps:
# make reset s3t
# make reset nsb
reset: $(RESET)
	python $(RESET) --p $(word 1,$(ARGS))

# #####################
#    DOCKER TARGETS   #
# #####################
# Crea las imágenes de Docker
# docker-compose build
build: $(COMPOSE_FILE)
	$(SHELL) $(DEPLOY_DOCKER) --build

# Genera los procesos de las imágenes creadas
create: $(COMPOSE_FILE)
	$(SHELL) $(DEPLOY_DOCKER) --create

run: $(COMPOSE_FILE)
	$(SHELL) $(DEPLOY_DOCKER) --up $(word 1,$(ARGS))

enter: $(COMPOSE_FILE)
	$(SHELL) $(DEPLOY_DOCKER) --enter $(word 1,$(ARGS))

docker-regions:
	@echo -e "\033[1mREGION-1\033[0m: $(DOCKER_REGION_1)"
	@echo -e "\033[1mREGION-2\033[0m: $(DOCKER_REGION_2)"
	@echo

# all: $(COMPOSE_FILE)
# 	docker-compose up
# 	docker exec -i -t $(CONTAINER_NAME) $(SHELL) -c "su bb"

stop: $(COMPOSE_FILE)
	@docker stop $(docker ps -qa) 2>/dev/null

remove: $(COMPOSE_FILE)
	@docker rm $(docker ps -qa) 2>/dev/null

%:
	@:
