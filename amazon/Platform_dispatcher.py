import re
from amazon.globals import currency, country


class PlatformDispatcher:

    @staticmethod
    def has_data(diccionario_script):
        if not diccionario_script:
            return False
        return True

    @classmethod
    def dispatch_movie(cls, diccionario_script):
        if not cls.has_data(diccionario_script):
            return None
        id_ = diccionario_script['state']['pageTitleId']
        try:
            diccionario_script = diccionario_script['state']['action']['atf'][id_]
        except KeyError:
            return None

        dic = cls.get_platforms(diccionario_script)
        return dic

    def dispatch_episodes(self, diccionario_script, diccionario_):
        if not self.has_data(diccionario_script):
            return []
        season_price = self.dispatch_movie(diccionario_)
        if not season_price:
            season_price = {}
        diccionario_script = diccionario_script['state']['action']['btf']
        episodes = self.get_episodes_platforms(diccionario_script)
        episodes = self.complete_episodes(episodes, season_price)
        return episodes

    @staticmethod
    def complete_episodes(episodes, season_price):
        prices = {
            'SD': {},
            'HD': {}
        }
        if country() + ".amazon" not in season_price.keys():
            return episodes
        for _dict in season_price[country() + ".amazon"]:
            if 'SeasonPrice' not in _dict:
                continue
            prices[_dict['Definition']] = {'SeasonPrice': _dict['SeasonPrice']}

        for episode in episodes:
            for key in episode.keys():
                if country() + ".Amazon" not in episode[key].keys():
                    continue
                for amazon in episode[key][country() + ".Amazon"]:
                    if 'Definition' not in amazon.keys():
                        continue
                    if amazon['Definition'] == 'SD':
                        amazon.update(prices['SD'])
                    else:
                        amazon.update(prices['HD'])
        return episodes

    @classmethod
    def get_platforms(cls, data):
        ways_to_adquire = data['acquisitionActions']
        platforms = cls.platforms_and_packages(ways_to_adquire)
        if data['playbackActions']:
            platforms.update(cls.get_free_platforms(data['playbackActions']))
        return platforms

    @classmethod
    def get_episodes_platforms(cls, data):
        episodes = []
        for key in data.keys():
            episodes.append({key: cls.get_platforms(data[key])})
        return episodes

    @classmethod
    def platforms_and_packages(cls, data):
        platforms = {}
        amazon = []
        if "moreWaysToWatch" in data.keys():
            platforms = cls.acquisitionActions(data['moreWaysToWatch']['children'])
        else:
            if "svodWinners" in data.keys():
                cls.get_platforms_svodWinners(data, platforms)
            if "tvodWinners" in data.keys():
                cls.get_platforms_tvodwinners(data, amazon)
        if amazon:
            platforms[country() + '.amazon'] = cls.get_packages_amazon(amazon)
        return platforms

    @classmethod
    def acquisitionActions(cls, data):
        platforms = {}
        amazon = []
        for elements in data:
            for element in elements['children']:
                if element['__type'] == "atv.wps#SubscribeAction":
                    platforms[cls.get_platform_subscription(element['signupLink'], element['sType'])] = [
                        {"Type": "subscription-vod"}]
                elif element['__type'] == "atv.wps#TvodAction":
                    amazon.append(cls.get_amazon_transaction(element['purchaseData']))
        if amazon:
            platforms[country() + '.amazon'] = cls.get_packages_amazon(amazon)
        return platforms

    @classmethod
    def get_platforms_svodWinners(cls, data, platforms):
        for element in data['svodWinners']['children']:
            platforms[cls.get_platform_subscription(element['signupLink'], element['sType'])] = [
                {"Type": "subscription-vod"}]

    @classmethod
    def get_platforms_tvodwinners(cls, data, amazon):
        for element in data['tvodWinners']['children']:
            amazon.append(cls.get_amazon_transaction(element['purchaseData']))

    @classmethod
    def get_packages_amazon(cls, data):
        packages = []
        definitions = {
            'SD': {'Type': 'transaction-vod', 'Definition': 'SD'},
            'HD': {'Type': 'transaction-vod', 'Definition': 'HD'},
            'UHD': {'Type': 'transaction-vod', 'Definition': '4K'}
        }
        for element in data:
            if "RentPrice" in element.keys():
                definitions[element['Definition']]['RentPrice'] = cls.get_price(element['RentPrice'])
            elif "BuyPrice" in element.keys():
                definitions[element['Definition']]['BuyPrice'] = cls.get_price(element['BuyPrice'])
            else:
                definitions[element['Definition']]['SeasonPrice'] = cls.get_price(element['SeasonPrice'])
        for definition in definitions.keys():
            if len(definitions[definition]) > 2:
                definitions[definition]['Currency'] = currency()
                packages.append(definitions[definition])
        return packages

    @staticmethod
    def get_price(number):
        if isinstance(number, str):
            precio = number.replace(',', '')
            return float(precio)
        return float(number)

    @classmethod
    def get_amazon_transaction(cls, data):
        transaction = {'Type': 'transaction-vod', 'Definition': data['videoQuality']}
        precio = re.search(r'(\d\,)?\d{1,3}(\.\d{0,2})?$', data['text']).group(0)
        if data["offerType"] == "TVOD_RENTAL":
            if data["family"] == "SEASON":
                transaction['SeasonPrice'] = cls.get_price(precio)
            else:
                transaction['RentPrice'] = cls.get_price(precio)
            transaction['Currency'] = currency()
        else:
            if data["family"] == "SEASON":
                transaction['SeasonPrice'] = cls.get_price(precio)
            else:
                transaction['BuyPrice'] = cls.get_price(precio)
            transaction['Currency'] = currency()
        return transaction

    @staticmethod
    def get_platform_subscription(data, s_type):
        try:
            url = re.search(r"benefitId=(.*)\&", data).group(1)
            return f'{country()}.amazon-{url}'
        except AttributeError:
            return f'{country()}.amazon-prime'

    """
    if s_type == "PRIME":
        return country + ".amazon-prime"
    
    para saber el canal lo que se hace es usar un regex que matchea el nombre del mismo y teniendo en cuenta la 
    llave en "{lineBreak}". solo se consideran las palabras en mayuscula ya que asi estan escritas en la key.
    Por ejemplo:
        -   "Ve con HBO{lineBreak}Empieza tus 7 días de período de prueba" - matchea HBO
        -   "Ve con Hi-YAH!{lineBreak}Empieza tus 7 días de período de prueba" - matchea Hi-YAH!
        -   "Ve con Lifetime Movie Club{lineBreak}Empieza tus 7 días de período de prueba" - matchea 
            Lifetime Movie Club
    Esto hace que no se tenga que depender del idioma a pesar de que los ejemplos estan en español. Je..
    
    provider = re.search(r"((([A-Z])\w+)(\s|^|\-)){0,2}([A-Z])\w+\+?\!?(?={)", data).group(0)
    """

    @staticmethod
    def get_free_platforms(data):
        platforms = {}
        for element in data['main']['children']:
            if "subscriptionName" in element.keys():
                if element["subscriptionName"] == 'Prime':
                    platforms[country() + ".amazon-" + element['subscriptionName'].replace(" ", "").lower()] = [
                        {'Type': 'subsciption-vod'}]
                else:
                    platforms[country() + ".amazon-" + element['subscriptionName'].replace(" ", "").lower()] = [
                        {'Type': 'free-vod'}]
            else:
                platforms[country() + '.amazon'] = [{'Type': 'free-vod'}]
        return platforms

    @classmethod
    def get_serie_platforms(cls, episodes):
        platforms = list(set([epi['PlatformCode'] for epi in episodes]))
        packages = {}
        for platform in platforms:
            packages[platform] = []
            for epi in episodes:
                if epi['PlatformCode'] != platform:
                    continue
                for package in epi['Packages']:
                    if package['Type'] in [p['Type'] for p in packages[platform]]:
                        continue
                    if package['Type'] == 'transaction-vod':
                        packages[platform].append({'Type': package['Type'], 'Currency': currency()})
                    else:
                        packages[platform].append({'Type': package['Type']})
        return platforms, packages

    @staticmethod
    def get_movie_platforms(plataformas):
        return list(set(plataformas.keys()))
