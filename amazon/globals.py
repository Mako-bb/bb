from common import config

get_country = ""
get_currency = ""
get_domain = ""


def set_globals(_country, pagina):
    global get_country, get_currency, get_domain
    get_country = _country.lower()
    get_currency = config()['currency'][_country]
    get_domain = config()['ott_sites'][pagina]['countries'][_country]['domain']


def country():
    global get_country
    return get_country


def currency():
    global get_currency
    return get_currency


def domain():
    global get_domain
    return get_domain
