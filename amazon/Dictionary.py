import json


class Dictionary:
    @staticmethod
    def get_dictionary(soup, key):
        scripts = soup.find_all('script', {'type': 'text/template'})
        for script in scripts:
            diccionario = script.text
            diccionario_script = json.loads(diccionario)
            if key in diccionario_script['props']:
                return diccionario_script['props']
    
    @staticmethod
    def get_dictionary_serie(soup, key):
        scripts = soup.find_all('script', {'type': 'text/template'})
        for script in scripts[::-1]:
            diccionario = script.text
            diccionario_script = json.loads(diccionario)
            if key in diccionario_script['props']:
                return diccionario_script['props']

    @staticmethod
    def get_info(soup, tipo_contenido=None):
        """
        :param soup: es el html del contenido que se usa para buscar el diccinario con los datos
        :param tipo_contenido: selecciona el tipo de contenido que se desea retornar
        :return: el diccionario con los capitulos o los datos de la serie/pelicula
        """
        scripts = soup.find_all('script', {'type': 'text/template'})
        if tipo_contenido == 'episode':
            diccionario = scripts[4].text
        else:
            diccionario = scripts[3].text
        diccionario_script = json.loads(diccionario)
        return diccionario_script
