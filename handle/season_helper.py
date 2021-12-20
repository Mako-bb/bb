    #  Import y usos:
    #   from handle.season_helper.py impor SeasonHelper
    #   
    #   si la plataforma no muestra ni un solo dato de seasons: 
    #        llamar a SeasonHelper().get_seasons_complete(episodes) y pasarle la lista de episodios de la serie como parametro
    #   si se requieren completar algunos campos:
    #        llamar a SeasonHelper().complete_fields_seasons(episodes,seasons,*args) y pasar los episodios de la serie, su field seasons
    #        y los nombres de los campos a llenar, ejemplo: SeasonHelper().complete_fields_seasons(episodes,seasons, 'Id', Synopsis')
    #           --Es importante que las seasons tengan el SeasonNumber al menos---
    #  si se requiere recuperar la lista de seasons, pero con solo algunos campos completos
    #        llamar a SeasonHelper().get_seasons_incomplete(episodes, *args), recibe los episodios de la serie y los fields a completar
    #        ejemplo:  SeasonHelper().get_seasons_incomplete(episodes, 'Id','Number','Synopsis)

class SeasonHelper():

    @staticmethod
    def get_season_payload():
        return {
                "Id": None,           #Importante
                "Synopsis": None,     #Importante
                "Title": None,        #Importante, E.J. The Wallking Dead: Season 1
                "Deeplink":  None,    #Importante
                "Number": None,       #Importante
                "Year": None,         #Importante
                "Image": None, 
                "Directors": None,   #Importante
                "Cast": None,        #Importante
                "Episodes": None,      #Importante
                "IsOriginal": None   
            }

    def get_seasons_complete(self, episodes, forced=False):
        seasons = []
        seasons_numb = self.get_seasons_number(episodes)
        for season_n in seasons_numb:
            episodes_season= self.get_episodes_season(episodes,season_n)
            if not episodes_season:
                continue
            seasons.append({
                      "Id": self.get_id(episodes_season),           #Importante
                      "Synopsis": self.get_synopsis(episodes_season, forced=forced),     #Importante
                      "Title": self.get_title(episodes_season, season_n),        #Importante, E.J. The Wallking Dead: Season 1
                      "Deeplink":  self.get_deeplink(episodes_season),    #Importante
                      "Number": season_n,       #Importante
                      "Year": self.get_year(episodes_season,forced=forced),         #Importante
                      "Image": self.get_images(episodes_season), 
                      "Directors": self.get_directors(episodes_season),   #Importante
                      "Cast": self.get_cast(episodes_season),        #Importante
                      "Episodes": self.get_cant_ep(episodes_season),      #Importante
                      "IsOriginal": self.is_original(episodes_season)    
                      })
        return seasons if seasons else None

    def get_seasons_incomplete(self, episodes, *args, forced=False):
        seasons=[]
        functions = self.get_functions_fields()
        seasons_numb = self.get_seasons_number(episodes)
        for season_n in seasons_numb:
            episodes_season= self.get_episodes_season(episodes,season_n)
            if not episodes_season:
                continue
            season_actual= self.get_season_payload()
            for arg in args:
                if (forced) and (arg == 'Image' or arg == 'Synopsis'):
                    season_actual[arg] = functions[arg](episodes_season,forced=forced)
                else:
                    season_actual[arg] = functions[arg](episodes_season)
            seasons.append(season_actual)
        
        return seasons if seasons else None


    def get_functions_fields(self):
        return {
            "Id": self.get_id,           #Importante
            "Synopsis": self.get_synopsis,     #Importante
            "Title": self.get_title,        #Importante, E.J. The Wallking Dead: Season 1
            "Deeplink":  self.get_deeplink,    #Importante
            "Year": self.get_year,         #Importante
            "Image": self.get_images, 
            "Directors": self.get_directors,   #Importante
            "Cast": self.get_cast,        #Importante
            "Episodes": self.get_cant_ep,      #Importante
            "IsOriginal": self.is_original  
        }

    def complete_fields_seasons(self, episodes, seasons, *args, forced=False):
        functions = self.get_functions_fields()
        for season in seasons:
            episodes_season = self.get_episodes_season(episodes,season['Number'])
            if not episodes_season:
                continue
            for arg in args:
                if (forced) and ((arg == 'Image') or (arg == 'Synopsis')):
                    season[arg] = functions[arg](episodes_season, forced)
                else:
                    season[arg] = functions[arg](episodes_season)
        return seasons if seasons else None
    @staticmethod
    def get_cant_ep(episodes):
       return len(episodes) 
    @staticmethod
    def is_original(episodes):
        #Pedido de joel:
        return None
        # for episode in episodes:
        #     if ( 'IsOriginal' not in episode.keys() ) or (not episode['IsOriginal']):
        #         return None
        # return True
    
    @staticmethod
    def get_list(episodes, field):
        _list = list()
        for episode in episodes:
            if episode[field]:
                _list += episode[field]
        return list(set(_list)) if _list else None
    def get_cast(self, episodes):
        return self.get_list(episodes, 'Cast')
    def get_directors(self,episodes):
        return self.get_list(episodes, 'Directors')
    def get_images(self,episodes):
        return self.get_list(episodes, 'Image')
        
    @staticmethod
    def get_year(episodes,forced=False):
        if (forced):
            for episode in episodes:
                if  episode['Year']:
                    return episode['Year']

        return episodes[0]['Year']
    @staticmethod
    def get_deeplink(episodes):
        return episodes[0]['Deeplinks']['Web']
    @staticmethod
    def get_id(episodes):
        return episodes[0]['Id']
    @staticmethod
    def get_synopsis(episodes, forced=False):
        if  (forced):
            for episode in episodes: 
                if episode['Synopsis']:
                    return episode['Synopsis']
        return episodes[0]['Synopsis']
    @staticmethod
    def get_title(episodes, season_n):
        return "{}: Season {}".format(episodes[0]['ParentTitle'],season_n)
    
    @staticmethod
    def get_episodes_season(episodes,season_number):
        episodes_s = list(filter(lambda x: x['Season'] == season_number, episodes))
        episodes_s = list(filter(lambda x: type(x['Episode']) == type(1), episodes_s))
        try:
            episodes_season = sorted(episodes_s, key=lambda k: k['Episode'])
        except:
            episodes_season = episodes_s

        return episodes_season
        
    @staticmethod
    def get_seasons_number(episodes):

        seasons=  list(set([ ep['Season'] for ep in episodes]))
    
        return list(sorted(filter(lambda x: type(x) == type(1), seasons)))



