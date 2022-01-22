import pycountry
from googletrans import Translator
import re
import json
import codecs
import logging
from imp import reload
import time
import pathlib

#from time import perf_counter
'''
    INSTALACION: 
        [ pip install pycountry ]
        [ pip install googletrans==3.1.0a0 ] (o ultima versión estable)
    FINALIDAD:
        get_codes() es un método para obtener el nombre del lenguaje en ingles según su Código ISO 639,
        o validar que el nombre completo sea correcto en el caso de que dispongamos de este.
        Si el lenguaje viene en otro idioma que no sea el inglés, el método va a tratar de traducirlo al inglés.
        En el caso que no encuentre un match o el largo del string pasado sea incorrecto se le asigna None.
    PARAMETROS:
        -lang_codes es obligatorio y puede ser un string suelto o una lista de strings.
        Este campo representa código/s o nombre/s de lenguaje completo.
        -className es obligatorio, es un string y representa el nombre de la clase principal de la plataforma.
        -isFullName no es obligatorio y por defecto está en False. Solo se le asigna True al llamar al método si
        en lang_codes pasamos nombre/s completo/s y no código/s.
    USO:
        Caso Ej.:
            from handle.iso_language import get_codes
            languages = item.get('languages')
            if languages:
            #Si es un lenguaje completo lo que viene:
                dubbed = get_codes(languages,"ParamountPlus",isFullName = True)
            #Sino
                dubbed = get_codes(languages,"ParamountPlus")
                payload['Dubbed'] = dubbed
            else:
                payload['Dubbed'] = None
        RETORNO:
            El método retornara una lista conteniendo nombres de lenguajes en inglés, o None, dependiendo si logra
            validar el lenguaje. Ej.:
                *MAL: dubbed -----> ["Spanish","German",None,"Hindi"]
                BIEN: dubbed -----> ["Spanish","German","Tamil","Hindi"]
        *MAL:
            Si el upload rebota por tener algún None dentro de los campos Dubbed o Subtitles, checkear el archivo de log bajo el nombre
            lang_{nombre de la clase del script}.log en la carpeta log / Ej.: lang_ParamountPlus.log / Aquí vamos a encontrar listados los lenguajes que no
            se pudieron matchear. Se debe validar que estos lenguajes existan, si es así agregarlos manualmente al archivo
            data/languages_validator.json: 
            -Si el lenguaje está en idioma inglés, se deberá agregar un bloque nuevo al final del JSON Ej.:
                    "lenguaje": {
                        "other_names": []
                    },
            -Si el lenguaje es una variante en otro idioma de un lenguaje ya existente en ingles en el JSON, se deberá agregar
            al campo other_names de su respectivo bloque Ej. "castellano":
                    "Spanish":{
                        "other_names":["castellano"]
                    }
'''


def get_codes(lang_codes, className, isFullName=False):
    #t1_start = perf_counter()
    logger = None
    names = []
    file_name = "data/languages_validator.json"
    checked_languages = get_json_obj(file_name)
    translator = Translator()

    if type(lang_codes) is not list:
        loose_language = lang_codes
        lang_codes = []
        lang_codes.append(loose_language)

    if check_invalid(lang_codes):
        print(f'Error - Invalid data type detected. Value set to None.')
        return None

    for lang_code in lang_codes:
        code = lang_code.lower().strip()
        len_code = len(code)
        if code == 'pun':
            code = 'pan'
        if isFullName:
            name = lookup(code, checked_languages)
            if not name:
                language_pack = newTranslator(code, translator)
                if language_pack:
                    add_lang(language_pack, file_name, checked_languages)
                    name = language_pack[0]
                else:
                    print(
                        f'Error - No translation found for {lang_code}. Value set to None.')
        else:
            code = code.replace(' ', '')
            if len_code < 2 or len_code > 3:
                print(
                    f'Error - Language code {lang_code} is either to short or to long, must be two(2) or three(3) letters long. Value set to None.')
                name = None
            else:
                if len_code == 2:
                    try:
                        lang = pycountry.languages.get(alpha_2=code)
                        name = lang.name
                    except Exception:
                        name = None
                else:
                    try:
                        lang = pycountry.languages.get(alpha_3=code)
                        name = lang.name
                    except Exception:
                        name = None
            if not name:
                print(
                    f'Error - No language name for code {lang_code}. Value set to None.')
        if name:
            name = clean_language_name(name)
        else:
            if not logger:
                logger = newLogger(className)
            with open('log/LANG_{}_{}.log'.format(time.strftime("%Y-%m-%d"), className)) as log:
                if lang_code not in log.read():
                    logger.error(lang_code)
        names.append(name)
    #t1_stop = perf_counter()
    #print(f'\n{t1_stop - t1_start} seconds\n')
    logging.shutdown()
    return list(set(names))


def newTranslator(lang_str, translator):
    language_pack = None
    try:
        detection = translator.detect(lang_str)
        confidence = detection.confidence
        if detection.lang == 'en' and confidence > 0.95:
            language_pack = [lang_str, "english"]
        else:
            trans_name_bis = None
            translation = translator.translate(lang_str)
            trans_name = translation.text
            confidence = translation.extra_data['confidence']
            try:
                trans_name_bis = translation.extra_data['definitions'][0][1][0][2]
            except Exception:
                pass
            if trans_name_bis:
                detection_bis = translator.detect(trans_name_bis)
                confidence_bis = detection_bis.confidence
                if detection_bis.lang == 'en' and confidence_bis > confidence:
                    language_pack = [trans_name_bis, lang_str]
            if not language_pack:
                if trans_name and confidence > 0.6:
                    origin = translation.origin
                    language_pack = [trans_name, origin]
    except Exception:
        pass
    return language_pack


def clean_language_name(name_str):
    if '(' in name_str:
        name_str = re.sub(r"\([^()]*\)", '', name_str)
        name_str = name_str.strip()
    return name_str.capitalize()


def lookup(name_str, dct):
    if name_str in dct.keys():
        return name_str
    else:
        for i, v in dct.items():
            if name_str in v['other_names']:
                return i
    return None


def add_lang(laguage_pack, file_name_str, dct):
    name_str = laguage_pack[0].lower()
    name_str = name_str.strip()
    origin = laguage_pack[1]
    origin = origin.strip()
    check = lookup(name_str, dct)

    if check:
        if origin not in dct[name_str]["other_names"]:
            dct[name_str]["other_names"].append(origin)
    else:
        dct[name_str] = {
            "other_names": []
        }

    with codecs.open(file_name_str, 'w', 'utf-8') as f:
        json.dump(dct, f, indent=4, ensure_ascii=False)
        f.close()

    print(
        f'\n->Modified languages_validator.json: {name_str} : other_names = {dct[name_str]["other_names"]}!!!\n')


def get_json_obj(file_name_str):
    with codecs.open(file_name_str, encoding='utf-8') as f:
        json_object = json.load(f)
        f.close()
    return json_object


def newLogger(className):
    date_time = time.strftime("%Y-%m-%d")
    reload(logging)
    log_dir = 'log'
    pathlib.Path(log_dir).mkdir(exist_ok=True)
    file_name = '{}/LANG_{}_{}.log'.format(log_dir, date_time, className)
    logging.basicConfig(filename=file_name,
                        format='%(message)s',
                        filemode='a')
    logger = logging.getLogger()
    logger.setLevel(logging.ERROR)
    return logger


def check_invalid(list_):
    special_chars = '`-=[];,./\~!@#$%^&*()_+}{|:"<>?'
    if any(type(sub) is not str for sub in list_):
        return True
    if any(x == "" for x in list_):
        return True
    try:
        check = [any(s in special_chars for s in sub) for sub in list_]
        if any(x for x in check):
            return True
    except Exception:
        pass
    try:
        check = [any(s.isdigit() for s in sub) for sub in list_]
        if any(x for x in check):
            return True
    except Exception:
        pass
    return False
