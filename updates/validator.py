# -*- coding: utf-8 -*-
import re
from datetime import datetime
from collections import defaultdict
from handle import iso_language

def check_packages(validator, item, key, defs):
    """
    Permite una sola definición por Package Type.
    Para movie y episode es obligatorio tener BuyPrice, RentPrice o SeasonPrice.
    Verifica que precios no excedan valores predefinidos.
    """
    definitions = defaultdict(list)
    item_type = item.get('Type') or 'episode'

    for idx, pkg in enumerate(item['Packages']):
        pkg_def = pkg.get('Definition')
        if pkg_def in definitions[pkg['Type']]:
            validator.add_error('dup', item, (*key, idx, 'Definition'))

        definitions[pkg['Type']].append(pkg_def)

        if pkg['Type'] != 'transaction-vod':
            continue

        if item_type == 'serie':
            continue

        if not pkg.get('Currency'):
            validator.add_error('missing_currency', item, (*key, idx))

        prices = prices_dict.get(pkg.get('Currency'), [])
        for price in prices:
            value = pkg.get(price)
            if value:
                max_price = prices[price].get(item_type)
                if max_price:
                    if value > max_price:
                        validator.add_error('wrong_value', item, (*key, idx, price), actual=value)

        if pkg.get('BuyPrice') or pkg.get('RentPrice'):
            continue

        if item_type == 'episode':
            if pkg.get('SeasonPrice') or pkg.get('SeasonBuyPrice') or pkg.get('SeasonRentPrice'):
                continue

        validator.add_error('missing_price', item, (*key, idx))


def check_id(validator, item, key, defs):
    item_id = item['Id']

    if item_id in validator.ids[validator.collection]:
        validator.add_error('dup', item, key)

    validator.ids[validator.collection].add(item_id)

    if item.get('Type') == 'serie' and validator.collection == 'titanScraping':
        validator.parent_ids.add(item_id)


def check_parent_id(validator, item, key, defs):
    if item['ParentId'] not in validator.parent_ids:
        validator.add_error('parent_id', item, key, actual=item['ParentId'])
    else:
        validator.parent_ids_used.add(item['ParentId'])


class ValueChecker:
    def __init__(self, func=None, regex=None):
        if regex:
            self._contains = lambda x: re.compile(regex).match(x)
        else:
            self._contains = func

    def __contains__(self, key):
        return self._contains(key)


validator_errors = {
    'dup': {
        'text': '{path} - repetido -> Id: "{id}"',
        'critical': True
    },
    'parent_id': {
        'text': '{path} - ParentId {actual} no existe en titanScraping -> Id: "{id}"',
        'critical': True
    },
    'no_content': {
        'text': 'No trajo contenido',
        'critical': False
    },
    'missing_episodes': {
        'text': 'Serie sin episodios -> Id: {actual}',
        'critical': False
    },
    'not_exists': {
        'text': '{path} - no existe -> Id: "{id}"',
        'critical': True
    },
    'missing_price': {
        'text': '{path} - falta el precio -> Id: "{id}"',
        'critical': True
    },
    'missing_currency': {
        'text': '{path} - falta la moneda -> Id: "{id}"',
        'critical': True
    },
    'is_none': {
        'text': '{path} - no puede ser None -> Id: "{id}"',
        'critical': True
    },
    'wrong_type': {
        'text': '{path} - tipo debe ser <{expected}>, ahora es <{actual}> -> Id: "{id}"',
        'critical': True
    },
    'wrong_value': {
        'text': '{path} - valor incorrecto: {actual} -> Id: "{id}"',
        'critical': True
    },
}

prices_dict = {
    'USD': {
        'BuyPrice': {
            'episode': 27
        },
        'RentPrice': {
            'episode': 10
        },
    },
    'GBP': {
        'BuyPrice': {
            'episode': 20
        },
        'RentPrice': {
            'episode': 10
        },
    },
    'EUR': {
        'BuyPrice': {
            'episode': 25
        },
        'RentPrice': {
            'episode': 10
        },
    }
}

validator_dict = {
    'PlatformCode': {
        'type': str,
        'required': True
    },
    'Id': {
        'type': str,
        'required': True,
        'specific': check_id
    },
    'Title': {
        'type': str,
        'required': True
    },
    'OriginalTitle': {
        'type': str,
    },
    'CleanTitle': {
        'type': str,
        'required': True,
        'collection': 'titanScraping',
    },
    'ParentId': {
        'type': str,
        'required': True,
        'collection': 'titanScrapingEpisodes',
        'specific': check_parent_id
    },
    'ParentTitle': {
        'type': str,
        'collection': 'titanScrapingEpisodes',
    },
    'Season': {
        'type': int,
        'collection': 'titanScrapingEpisodes',
        'allow_zero': True,
        'allow_null': True,
        'required': True,
    },
    'Episode': {
        'type': int,
        'collection': 'titanScrapingEpisodes',
        'required': True,
        'allow_null': True,
    },
    'Type': {
        'type': str,
        'required': True,
        'collection': 'titanScraping',
        'values': ('movie', 'serie')
    },
    'Year': {
        'type': int,
        'values': range(1870, datetime.now().year+1)
    },
    'Duration': {
        'type': int,
    },
    'Deeplinks': {
        'type': dict,
        'required': True,
        'keys': {
            'Web': {
                'type': str,
                'required': True
            },
            'Android': {
                'type': str,
            },
            'iOS': {
                'type': str,
            },
        }
    },
    'Image': {
        'type': list,
        'elements': {
            'type': str,
        }
    },
    'Playback': {
        'type': str
    },
    'Synopsis': {
        'type': str
    },
    'Rating': {
        'type': str
    },
    'Provider': {
        'type': list,
        'elements': {
            'type': str,
        }
    },
    'Genres': {
        'type': list,
        'elements': {
            'type': str,
        }
    },
    'Cast': {
        'type': list,
        'elements': {
            'type': str,
        }
    },
    'Directors': {
        'type': list,
        'elements': {
            'type': str,
        }
    },
    'Availability': {
        'type': str
    },
    'Download': {
        'type': bool
    },
    'IsOriginal': {
        'type': bool
    },
    'IsAdult': {
        'type': bool
    },
    'ExternalIds': {
        'type': list,
        'elements': {
            'type': dict,
            'keys': {
                'Provider': {
                    'type': str,
                    'required': True
                },
                'Id': {
                    'type': str,
                    'required': True
                }
            }
        }
    },
    'Crew': {
        'type': list,
        'elements': {
            'type': dict,
            'keys': {
                'Role': {
                    'type': str,
                    'required': True
                },
                'Name': {
                    'type': str,
                    'required': True
                }
            }
        }
    },
    'Packages': {
        'type': list,
        'required': True,
        'specific': check_packages,
        'elements': {
            'type': dict,
            'keys': {
                'Type': {
                    'type': str,
                    'required': True,
                    'values': ('subscription-vod', 'transaction-vod', 'free-vod', 'tv-everywhere', 'validated-vod')
                },
                'Plan': {
                    'type': str,
                },
                'Definition': {
                    'type': str,
                    'values': ('HD', 'SD', '4K', '8K')
                },
                'RentPrice': {
                    'type': float,
                    'values': ValueChecker(lambda x: x > 0)
                },
                'BuyPrice': {
                    'type': float,
                    'values': ValueChecker(lambda x: x > 0)
                },
                'SeasonPrice': {  # Obsoleto
                    'type': float,
                    'collection': 'titanScrapingEpisodes',
                    'values': ValueChecker(lambda x: x > 0)
                },
                'SeasonBuyPrice': {
                    'type': float,
                    'collection': 'titanScrapingEpisodes',
                    'values': ValueChecker(lambda x: x > 0)
                },
                'SeasonRentPrice': {
                    'type': float,
                    'collection': 'titanScrapingEpisodes',
                    'values': ValueChecker(lambda x: x > 0)
                },
                'Currency': {
                    'type': str,
                    'values': ValueChecker(regex=r'^[A-Z]{3}$')
                }
            }
        }
    },
    'Country': {
        'type': list,
        'elements': {
            'type': str,
        }
    },
    'Timestamp': {
        'type': str,
        'required': True
    },
    'CreatedAt': {
        'type': str,
        'required': True
    },
    # 'Dubbed':{
    #     'type': list,
    #     'elements':{
    #         'type':str,
    #         'values': ValueChecker(lambda x: iso_language.get_codes(x, isFullName=True) != None)
    #     },
    # },
    # 'Subtitles':{
    #     'type': list,
    #     'elements':{
    #         'type':str,
    #         'values': ValueChecker(lambda x: iso_language.get_codes(x, isFullName=True) != None)
    #     }
    # }
}


class ValidatorError:
    def __init__(self, collection, item, error, path, actual=None, expected=None):
        self.collection = collection
        self.id = item.get('Id') if item else None
        self.error = error
        self.path = path or ()
        self.actual = self.format(actual)
        self.expected = self.format(expected)

    def path_to_str(self):
        path_str = 'doc'

        for pos in self.path:
            path_str = f'{path_str}[{pos}]'

        return path_str

    def format(self, obj):
        type_obj = type(obj)

        if type_obj is type:
            fmt_obj = obj.__name__
        elif type_obj is str:
            fmt_obj = f'"{obj}"'
        elif type_obj is tuple:
            fmt_obj = ', '.join(obj)
        elif type_obj is range:
            fmt_obj = f'{min(obj)}-{max(obj)}'
        else:
            fmt_obj = obj

        return fmt_obj

    @property
    def message(self):
        attrs = dict(id=self.id,
                     path=self.path_to_str(),
                     actual=self.actual,
                     expected=self.expected)

        non_nulls_attrs = {k: v for k, v in attrs.items() if v is not None}

        return validator_errors[self.error]['text'].format(**non_nulls_attrs)

    def __str__(self):
        return self.message

    def __repr__(self):
        return self.__str__()


class Validator:
    def __init__(self):
        self.errors = []
        self.count = defaultdict(int)
        self.parent_ids = set()
        self.parent_ids_used = set()
        self.ids = defaultdict(set)

    def run_checks(self, collection, items):
        self.collection = collection

        for item in items:
            self.count_item()

            for key, defs in validator_dict.items():
                self.dispatch_check(item, (key,), defs)

        
        self.check_no_content()
        if collection == 'titanScrapingEpisodes':
            self.check_parent_ids_used()
        return self.checks_result(collection)

    def dispatch_check(self, item, key, defs):
        if defs['type'] is list:
            return self.check_list(item, key, defs)
        elif defs['type'] is dict:
            return self.check_dict(item, key, defs)
        else:
            return self.check(item, key, defs)

    def check_list(self, item, steps, defs):
        a_list = self.check(item, steps, defs)

        if a_list:
            elem_defs = defs['elements']
            for idx, element in enumerate(a_list):
                self.dispatch_check(item, (*steps, idx), elem_defs)

    def check_dict(self, item, steps, defs):
        a_dict = self.check(item, steps, defs)

        if a_dict:
            for key, key_defs in defs['keys'].items():
                self.dispatch_check(item, (*steps, key), key_defs)

    def check(self, item, steps, defs):
        expected_type = defs['type']
        required = defs.get('required')
        valid_values = defs.get('values')
        key_collection = defs.get('collection')
        allow_null = defs.get('allow_null')
        allow_zero = defs.get('allow_zero')
        specific_check = defs.get('specific')

        if key_collection and key_collection != self.collection:
            return

        try:
            value = self.walk(item, steps)
        except KeyError:
            if required:
                self.add_error('not_exists', item, steps)
            return

        if value is None:
            if required and not allow_null:
                self.add_error('is_none', item, steps)
            return

        if not isinstance(value, expected_type):
            self.add_error('wrong_type', item, steps, actual=type(value), expected=expected_type)
            return

        if required and value in ('', [], {}):
            self.add_error('wrong_value', item, steps, actual=value)
            return

        if value == 0 and value is not False and not allow_zero:
            self.add_error('wrong_value', item, steps, actual=value)
            return

        if valid_values and value not in valid_values:
            self.add_error('wrong_value', item, steps, actual=value)
            return

        if specific_check:
            specific_check(self, item, steps, defs)

        return value

    def check_no_content(self):
        if self.total(self.collection) == 0:
            self.add_error('no_content')

    def check_parent_ids_used(self):
        unused = self.parent_ids - self.parent_ids_used
        for parent_id in unused:
            self.add_error('missing_episodes', actual=parent_id)

    def add_error(self, error, item=None, keys=None, actual=None, expected=None):
        val_error = ValidatorError(self.collection, item, error, keys, actual, expected)
        print(val_error)
        self.errors.append(val_error)

    def walk(self, item, steps):
        val = item[steps[0]]

        for k in steps[1:]:
            val = val[k]

        return val

    def count_item(self):
        self.count[self.collection] += 1

    def total(self, collection):
        return self.count[collection]

    def unique_errors(self, collection=None):
        filtered = {}
        for item in self.errors:
            error, path = item.error, item.path
            if error in filtered:
                if path in filtered[error]:
                    continue
                else:
                    filtered[error][path] = item
            else:
                filtered[error] = {path: item}

        if collection:
            return [item for error in filtered.values() for item in error.values() if item.collection == collection]
        else:
            return [item for error in filtered.values() for item in error.values()]

    def checks_result(self, collection):
        col_errors = self.unique_errors(collection=collection)
        has_content = all(e.error != 'no_content' for e in col_errors)
        has_errors = bool(col_errors)
        no_epis = all(e.error == 'missing_episodes' for e in col_errors)
        ok = has_content and not has_errors

        return {'total': self.total(collection),
                'has_content': has_content,
                'has_errors': has_errors,
                'no_epis': no_epis,
                'ok': ok}
