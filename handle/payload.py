from datetime               import datetime, date
from handle.replace         import _replace

class Payload:
    def __init__(self, platform_code=None,
                 id_=None,
                 title=None,
                 original_title=None,
                 clean_title=None,
                 year=None,
                 duration=None,
                 deeplink_web=None,
                 deeplink_android=None,
                 deeplink_ios=None,
                 playback=None,
                 synopsis=None,
                 image=None,
                 rating=None,
                 provider=None,
                 external_ids=None,
                 genres=None,
                 cast=None,
                 directors=None,
                 availability=None,
                 download=None,
                 is_original=None,
                 is_branded=None,
                 is_adult=None,
                 packages=None,
                 country=None,
                 episodes=None,
                 parent_id=None,
                 parent_title=None,
                 episode=None,
                 season=None,
                 seasons=None,
                 number=None,
                 createdAt=None,
                 timestamp=None):
                 
        self._platformCode = platform_code
        self._id = id_
        self._title = title
        self._original_title = original_title
        self._clean_title = clean_title
        self._year = year
        self._duration = duration
        self._deeplink_web = deeplink_web
        self._deeplink_android = deeplink_android
        self._deeplink_ios = deeplink_ios
        self._playback = playback
        self._synopsis = synopsis
        self._image = image
        self._rating = rating
        self._provider = provider
        self._external_ids = external_ids
        self._genres = genres
        self._cast = cast
        self._directors = directors
        self._availability = availability
        self._download = download
        self._is_original = is_original
        self._is_branded = is_branded
        self._is_adult = is_adult
        self._packages = packages
        self._country = country
        #self._timestamp = timestamp
        self._timestamp = datetime.now().isoformat()
        self._episodes = episodes
        self._created_at = createdAt
        self._parent_id = parent_id
        self._parent_title = parent_title
        self._episode = episode
        self._season = season
        self._seasons = seasons
        self._number = number

    @property
    def platform_code(self):
        return self._platformCode

    @platform_code.setter
    def platform_code(self, new_platform):
        self._platformCode = new_platform

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, new_id):
        self._id = new_id
    
    @property
    def clean_title(self):
        return self._clean_title

    @clean_title.setter
    def clean_title(self, new_title):
        self._clean_title = new_title

    @property
    def title(self):
        return self._title

    @title.setter
    def title(self, new_title):
        self.clean_title = _replace(new_title)
        self._title = new_title

    @property
    def original_title(self):
        return self._original_title

    @original_title.setter
    def original_title(self, new_title):
        self._original_title = new_title

    @property
    def year(self):
        return self._year

    @year.setter
    def year(self, new_year):
        self._year = new_year

    @property
    def duration(self):
        return self._duration

    @duration.setter
    def duration(self, new_duration):
        self._duration = new_duration

    @property
    def deeplink_web(self):
        return self._deeplink_web

    @deeplink_web.setter
    def deeplink_web(self, new_deeplink_web):
        self._deeplink_web = new_deeplink_web

    @property
    def deeplink_android(self):
        return self._deeplink_android

    @deeplink_android.setter
    def deeplink_android(self, new_deeplink_android):
        self._deeplink_android = new_deeplink_android

    @property
    def deeplink_ios(self):
        return self._deeplink_ios

    @deeplink_ios.setter
    def deeplink_ios(self, new_deeplink_ios):
        self._deeplink_ios = new_deeplink_ios

    @property
    def playback(self):
        return self._playback

    @playback.setter
    def playback(self, new_playback):
        self._playback = new_playback

    @property
    def synopsis(self):
        return self._synopsis

    @synopsis.setter
    def synopsis(self, new_synopsis):
        self._synopsis = new_synopsis

    @property
    def image(self):
        return self._image

    @image.setter
    def image(self, new_image):
        self._image = new_image

    @property
    def rating(self):
        return self._rating

    @rating.setter
    def rating(self, new_rating):
        self._rating = new_rating

    @property
    def provider(self):
        return self._provider

    @provider.setter
    def provider(self, new_provider):
        self._provider = new_provider

    @property
    def external_ids(self):
        return self._external_ids

    @external_ids.setter
    def external_ids(self, new_external_ids):
        self._external_ids = new_external_ids

    @property
    def genres(self):
        return self._genres

    @genres.setter
    def genres(self, new_genres):
        self._genres = new_genres

    @property
    def cast(self):
        return self._cast

    @cast.setter
    def cast(self, new_cast):
        self._cast = new_cast

    @property
    def directors(self):
        return self._directors

    @directors.setter
    def directors(self, new_directors):
        self._directors = new_directors

    @property
    def availability(self):
        return self._availability

    @availability.setter
    def availability(self, new_availability):
        self._availability = new_availability

    @property
    def download(self):
        return self._download

    @download.setter
    def download(self, new_download):
        self._download = new_download

    @property
    def is_original(self):
        return self._is_original

    @is_original.setter
    def is_original(self, new_is_original):
        self._is_original = new_is_original

    @property
    def is_branded(self):
        return self._is_branded

    @is_branded.setter
    def is_branded(self, new_is_branded):
        self._is_branded = new_is_branded

    @property
    def is_adult(self):
        return self._is_adult

    @is_adult.setter
    def is_adult(self, new_is_adult):
        self._is_adult = new_is_adult

    @property
    def packages(self):
        return self._packages

    @packages.setter
    def packages(self, new_packages):
        self._packages = new_packages

    @property
    def country(self):
        return self._country

    @country.setter
    def country(self, new_country):
        self._country = new_country

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def parent_id(self):
        return self._parent_id

    @parent_id.setter
    def parent_id(self, new_parent_id):
        self._parent_id = new_parent_id

    @property
    def parent_title(self):
        return self._parent_title

    @parent_title.setter
    def parent_title(self, new_parent_title):
        self._parent_title = new_parent_title

    @property
    def episode(self):
        return self._episode

    @episode.setter
    def episode(self, new_episode):
        self._episode = new_episode

    #Numero de temporada.
    #----------------------------------#
    @property
    def season(self):
        return self._season

    @season.setter
    def season(self, new_season):
        self._season = new_season
    #----------------------------------#

    @property
    def number(self):
        return self._number

    @number.setter
    def number(self, new_number):
        self._number = new_number

    #Cantidad de temporadas.
    #----------------------------------#
    @property
    def seasons(self):
        return self._seasons

    @seasons.setter
    def seasons(self, new_season):
        self._seasons = new_season
    #----------------------------------#

    @property
    def createdAt(self):
        return self._created_at

    @createdAt.setter
    def createdAt(self,new_created_at):
        self._created_at = new_created_at

    @property
    def episodes(self):
        return self._episodes

    @episodes.setter
    def episodes(self,new_episodes):
        self._episodes = new_episodes

    def payload_movie(self):
        return {
            'PlatformCode': self._platformCode,
            'Id': self._id,
            'Title': self._title,
            'OriginalTitle': self._original_title,
            'CleanTitle': self._clean_title,
            'Type': 'movie',
            'Year': self._year,
            'Duration': self._duration,
            'Deeplinks': {
                'Web': self._deeplink_web,
                'Android': self._deeplink_android,
                'iOS': self._deeplink_ios
            },
            'Playback': self._playback,
            'Synopsis': self._synopsis,
            'Image': self._image,
            'Rating': self._rating,
            'Provider': self._provider,
            'ExternalIds': self._external_ids,
            'Genres': self._genres,
            'Cast': self._cast,
            'Directors': self._directors,
            'Availability': self._availability,
            'Download': self._download,
            'IsOriginal': self._is_original,
            'IsAdult': self._is_adult,
            'IsBranded': self._is_branded,
            'Packages': self._packages,
            'Country': self._country,
            'Timestamp': self._timestamp,
            'CreatedAt': self._created_at
        }

    def payload_serie(self):
        return {
            'PlatformCode': self._platformCode,
            'Id': self._id,
            'Title': self._title,
            'OriginalTitle': self._original_title,
            'CleanTitle': self._clean_title,
            'Type': 'serie',
            'Year': self._year,
            'Duration': self._duration,
            'Deeplinks': {
                'Web': self._deeplink_web,
                'Android': self._deeplink_android,
                'iOS': self._deeplink_ios,
            },
            'Seasons': self._seasons,
            'Playback': self._playback,
            'Synopsis': self._synopsis,
            'Image': self._image,
            'Rating': self._rating,
            'Provider': self._provider,
            'ExternalIds': self._external_ids,
            'Genres': self._genres,
            'Cast': self._cast,
            'Directors': self._directors,
            'Availability': self._availability,
            'Download': self._download,
            'IsOriginal': self._is_original,
            'IsBranded': self._is_branded,
            'IsAdult': self._is_adult,
            'Packages': self._packages,
            'Country': self._country,
            'Timestamp': self._timestamp,
            'CreatedAt': self._created_at
        }

    def payload_season(self):
        return {
            'Id': self._id,
            'Synopsis': self._synopsis,
            'Title': self._title,
            'Deeplink': self._deeplink_web,
            'Number': self._number,
            'Image': self._image,
            'Directors': self._directors,
            'Cast': self._cast,
            'Episodes': self._episodes,
            'IsOriginal': self._is_original,
            'Year': self._year
        }

    def payload_episode(self):
        return {
            'PlatformCode': self.platform_code,
            'ParentId': self._parent_id,
            'ParentTitle': self._parent_title,
            'Id': self.id,
            'Title': self.title,
            'Episode': self.episode,
            'Season': self.season,
            'Year': self.year,
            'Image': self._image,
            'Duration': self.duration,
            'Deeplinks': {
                'Web': self.deeplink_web,
                'Android': self.deeplink_android,
                'iOS': self._deeplink_ios
            },
            'Synopsis': self.synopsis,
            'Rating': self.rating,
            'Provider': self.provider,
            'ExternalIds': self._external_ids,
            'Genres': self.genres,
            'Cast': self.cast,
            'Directors': self.directors,
            'Availability': self.availability,
            'Download': self.download,
            'IsOriginal': self._is_original,
            'IsAdult': self._is_adult,
            'Country': self.country,
            'Packages': self.packages,
            'Timestamp': self.timestamp,
            'CreatedAt': self._created_at
        }