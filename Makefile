PYTHON=python
MAIN=main.py
UPLOAD=updates/upload.py
ARGS=$(filter-out $@,$(MAKECMDGOALS))

# Uso: make scraping <platform> <country>
scraping: $(MAIN)
	python main.py --o scraping $(word 1,$(ARGS)) --c $(word 2,$(ARGS))

# Uso: make testing <platform> <country>
testing: $(MAIN)
	python main.py --o testing $(word 1,$(ARGS)) --c $(word 2,$(ARGS))

# Uso: make return <platform> <country>
return: $(MAIN)
	python main.py --o return $(word 1,$(ARGS)) --c $(word 2,$(ARGS))

# Uso: make justwatch <provider> <country>
justwatch: $(MAIN)
	python main.py JustWatch --c $(word 2,$(ARGS)) --provider $(word 1,$(ARGS)) --o jwscraping

# Uso: make piracy <name>
piracy: $(MAIN)
	python main.py --o piracy $(word 1,$(ARGS))

# Uso: make comprobe xx.xxxx 9999-99-99
# Ej: make comprobe us.netflix 2020-03-25
comprobe:
	python updates/upload.py --platformcode $(word 1,$(ARGS)) --createdat $(word 2,$(ARGS))

# Subirá por defecto a Misato
# Ej: make upload_misato us.netflix 2020-03-25
upload_misato:
	python updates/upload.py --platformcode $(word 1,$(ARGS)) --createdat $(word 2,$(ARGS)) --upload

# Subirá al server de Kaji
# Ej: make upload_kaji us.netflix 2020-03-25
upload_kaji:
	python updates/upload.py --platformcode $(word 1,$(ARGS)) --createdat $(word 2,$(ARGS)) --upload --server 2

# Ej: make bypass us.netflix 2020-03-25
# Por defecto lo subirá a Misato!
bypass:
	python updates/upload.py --platformcode $(word 1,$(ARGS)) --createdat $(word 2,$(ARGS)) --upload --bypass

test:
	# Add commands
