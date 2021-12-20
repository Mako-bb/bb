# -*- coding: utf-8 -*-
class CharactersDecode():
    #   como usar la clase: from handle.charactersdecode.py impor CharactersDecode
    #       para pasar todos los caracteres con codigo %algo a str: CharactersDecode.decode_utf(string)
    #       para pasar todos los caracteres con codigo &#algo a str: CharactersDecode.decode_hexa(string)
    #       para pasar algunos los caracteres de str a %algo: CharactersDecode.encode_some_utf(string, "caracter"...) 
    #           cada caracter a hacer encoding debe ser pasada como un argumento tipo string, ej: (stgin, " ","}","[")
    #       para pasar algunos los caracteres de str a %algo: CharactersDecode.encode_some_hexa(string, "caracter"...) 
    #           cada caracter a hacer encoding debe ser pasada como un argumento tipo string, ej: (stgin, " ","}","[")
    @staticmethod
    def encode(string,dictionary):
        for key in dictionary:
            if dictionary[key] in string:
                string= string.replace(dictionary[key], key)

    @classmethod
    def encode_utf(cls, string):
        dictionary= cls.get_dict_utf()
        return cls.encode(string, dictionary)
          
    @classmethod
    def encode_hexa(cls, string):
        dictionary= cls.get_dict_hexa()
        return cls.encode(string, dictionary)
    
    @staticmethod
    def decode(string, dictionary):
        for key in dictionary:
            if key in string:
                string = string.replace(key, dictionary[key])
        return string

    @classmethod
    def decode_utf(cls, string):
        dictionary= cls.get_dict_utf()
        return cls.decode(string, dictionary)
          
    @classmethod
    def decode_hexa(cls, string):
        dictionary= cls.get_dict_hexa()
        return cls.decode(string, dictionary)

    @classmethod
    def decode_some_hexa(cls,string, *argv):
        dictionary = cls.get_dict_hexa()
        keys =dictionary.keys()
        for arg in argv:
            if arg in keys:
                string= string.replace(arg,dictionary[arg])
    
        return string
    @classmethod
    def decode_some_utf(cls,string, *argv):
        dictionary = cls.get_dict_utf()
        keys =dictionary.keys()
        for arg in argv:
            if arg in keys:
                string= string.replace(arg,dictionary[arg])
        return string

    @classmethod
    def encode_some_hexa(cls,string, *argv):
        dictionary = cls.get_dict_hexa()
        for arg in argv:
            for key in dictionary:
                if arg == dictionary[key]:
                    string= string.replace(arg,key)
        return string
    @classmethod
    def encode_some_utf(cls,string, *argv):
        dictionary = cls.get_dict_utf()
        for arg in argv:
            for key in dictionary:
                if arg == dictionary[key]:
                    string= string.replace(arg,key)
        return string

    @staticmethod
    def get_dict_utf():    
        return {
            "%20"   : " ",
            "%21"   : "!",
            "%22"   : "\"",
            "%23"   : "#",
            "%24"   : "$",
            "%25"   : "%",
            "%26"   : "&",
            "%27"   : "'",
            "%28"   : "(",
            "%29"   : ")",
            "%2A"   : "*",
            "%2B"   : "+",
            "%2C"   : ",",
            "%2D"   : "-",
            "%2E"   : ".",
            "%2F"   : "/",
            "%30"   : "0",
            "%31"   : "1",
            "%32"   : "2",
            "%33"   : "3",
            "%34"   : "4",
            "%35"   : "5",
            "%36"   : "6",
            "%37"   : "7",
            "%38"   : "8",
            "%39"   : "9",
            "%3A"   : ":",
            "%3B"   : ";",
            "%3C"   : "<",
            "%3D"   : "=",
            "%3E"   : ">",
            "%3F"   : "?",
            "%40"   : "@",
            "%41"   : "A",
            "%42"   : "B",
            "%43"   : "C",
            "%44"   : "D",
            "%45"   : "E",
            "%46"   : "F",
            "%47"   : "G",
            "%48"   : "H",
            "%49"   : "I",
            "%4A"   : "J",
            "%4B"   : "K",
            "%4C"   : "L",
            "%4D"   : "M",
            "%4E"   : "N",
            "%4F"   : "O",
            "%50"   : "P",
            "%51"   : "Q",
            "%52"   : "R",
            "%53"   : "S",
            "%54"   : "T",
            "%55"   : "U",
            "%56"   : "V",
            "%57"   : "W",
            "%58"   : "X",
            "%59"   : "Y",
            "%5A"   : "Z",
            "%5B"   : "[",
            "%5C"   : "\\",
            "%5D"   : "]",
            "%5E"   : "^",
            "%5F"   : "_",
            "%60"   : "`",
            "%61"   : "a",
            "%62"   : "b",
            "%63"   : "c",
            "%64"   : "d",
            "%65"   : "e",
            "%66"   : "f",
            "%67"   : "g",
            "%68"   : "h",
            "%69"   : "i",
            "%6A"   : "j",
            "%6B"   : "k",
            "%6C"   : "l",
            "%6D"   : "m",
            "%6E"   : "n",
            "%6F"   : "o",
            "%70"   : "p",
            "%71"   : "q",
            "%72"   : "r",
            "%73"   : "s",
            "%74"   : "t",
            "%75"   : "u",
            "%76"   : "v",
            "%77"   : "w",
            "%78"   : "x",
            "%79"   : "y",
            "%7A"   : "z",
            "%7B"   : "{",
            "%7C"   : "|",
            "%7D"   : "}",
            "%7E"   : "~",
            "%7F"   : " ",
            "%E2%82%AC" : "`",
            "%81"   : "",
            "%E2%80%9A" : "‚",
            "%C6%92"    : "ƒ",
            "%E2%80%9E" : "„",
            "%E2%80%A6" : "…",
            "%E2%80%A0" : "†",
            "%E2%80%A1" : "‡",
            "%CB%86"    : "ˆ",
            "%E2%80%B0" : "‰",
            "%C5%A0"    : "Š",
            "%E2%80%B9" : "‹",
            "%C5%92"    : "Œ",
            "%C5%8D"    : "",
            "%C5%BD"    : "Ž",
            "%8F"   : "",
            "%C2%90"    : "",
            "%E2%80%98" : "‘",
            "%E2%80%99" : "’",
            "%E2%80%9C" : "“",
            "%E2%80%9D" : "”",
            "%E2%80%A2" : "•",
            "%E2%80%93" : "–",
            "%E2%80%94" : "—",
            "%CB%9C"    : "˜",
            "%E2%84"    : "™",
            "%C5%A1"    : "š",
            "%E2%80"    : "›",
            "%C5%93"    : "œ",
            "%9D"   : "",
            "%C5%BE"    : "ž",
            "%C5%B8"    : "Ÿ",
            "%C2%A0"    : " ",
            "%C2%A1"    : "¡",
            "%C2%A2"    : "¢",
            "%C2%A3"    : "£",
            "%C2%A4"    : "¤",
            "%C2%A5"    : "¥",
            "%C2%A6"    : "¦",
            "%C2%A7"    : "§",
            "%C2%A8"    : "¨",
            "%C2%A9"    : "©",
            "%C2%AA"    : "ª",
            "%C2%AB"    : "«",
            "%C2%AC"    : "¬",
            "%C2%AD"    : "­",
            "%C2%AE"    : "®",
            "%C2%AF"    : "¯",
            "%C2%B0"    : "°",
            "%C2%B1"    : "±",
            "%C2%B2"    : "²",
            "%C2%B3"    : "³",
            "%C2%B4"    : "´",
            "%C2%B5"    : "µ",
            "%C2%B6"    : "¶",
            "%C2%B7"    : "·",
            "%C2%B8"    : "¸",
            "%C2%B9"    : "¹",
            "%C2%BA"    : "º",
            "%C2%BB"    : "»",
            "%C2%BC"    : "¼",
            "%C2%BD"    : "½",
            "%C2%BE"    : "¾",
            "%C2%BF"    : "¿",
            "%C3%80"    : "À",
            "%C3%81"    : "Á",
            "%C3%82"    : "Â",
            "%C3%83"    : "Ã",
            "%C3%84"    : "Ä",
            "%C3%85"    : "Å",
            "%C3%86"    : "Æ",
            "%C3%87"    : "Ç",
            "%C3%88"    : "È",
            "%C3%89"    : "É",
            "%C3%8A"    : "Ê",
            "%C3%8B"    : "Ë",
            "%C3%8C"    : "Ì",
            "%C3%8D"    : "Í",
            "%C3%8E"    : "Î",
            "%C3%8F"    : "Ï",
            "%C3%90"    : "Ð",
            "%C3%91"    : "Ñ",
            "%C3%92"    : "Ò",
            "%C3%93"    : "Ó",
            "%C3%94"    : "Ô",
            "%C3%95"    : "Õ",
            "%C3%96"    : "Ö",
            "%C3%97"    : "×",
            "%C3%98"    : "Ø",
            "%C3%99"    : "Ù",
            "%C3%9A"    : "Ú",
            "%C3%9B"    : "Û",
            "%C3%9C"    : "Ü",
            "%C3%9D"    : "Ý",
            "%C3%9E"    : "Þ",
            "%C3%9F"    : "ß",
            "%C3%A0"    : "à",
            "%C3%A1"    : "á",
            "%C3%A2"    : "â",
            "%C3%A3"    : "ã",
            "%C3%A4"    : "ä",
            "%C3%A5"    : "å",
            "%C3%A6"    : "æ",
            "%C3%A7"    : "ç",
            "%C3%A8"    : "è",
            "%C3%A9"    : "é",
            "%C3%AA"    : "ê",
            "%C3%AB"    : "ë",
            "%C3%AC"    : "ì",
            "%C3%AD"    : "í",
            "%C3%AE"    : "î",
            "%C3%AF"    : "ï",
            "%C3%B0"    : "ð",
            "%C3%B1"    : "ñ",
            "%C3%B2"    : "ò",
            "%C3%B3"    : "ó",
            "%C3%B4"    : "ô",
            "%C3%B5"    : "õ",
            "%C3%B6"    : "ö",
            "%C3%B7"    : "÷",
            "%C3%B8"    : "ø",
            "%C3%B9"    : "ù",
            "%C3%BA"    : "ú",
            "%C3%BB"    : "û",
            "%C3%BC"    : "ü",
            "%C3%BD"    : "ý",
            "%C3%BE"    : "þ",
            "%C3%BF"    : "ÿ"
            }
    
    @staticmethod
    def get_dict_hexa():
        return {
            "&#32": " ",
            "&#33": "!",
            "&#34": "\"",
            "&#35": "#",
            "&#36": "$",
            "&#37": "%",
            "&#38": "&",
            "&#39": "'",
            "&#40": "(",
            "&#41": ")",
            "&#42": "*",
            "&#43": "+",
            "&#44": ",",
            "&#45": "-",
            "&#46": ".",
            "&#47": "/",
            "&#48": "0",
            "&#49": "1",
            "&#50": "2",
            "&#51": "3",
            "&#52": "4",
            "&#53": "5",
            "&#54": "6",
            "&#55": "7",
            "&#56": "8",
            "&#57": "9",
            "&#58": ":",
            "&#59": ";",
            "&#60": "<",
            "&#61": "=",
            "&#62": ">",
            "&#63": "?",
        }