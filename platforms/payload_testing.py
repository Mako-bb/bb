class Payload:
    def __init__(self,
        platformCode = None,
        id = None,
        title = None,
        originalTitle = None,
        cleanTitle = None,
        type = None,
        year = None,
        duration = None,
        deeplinksWeb = None,
        deeplinksAndroid = None,
        deeplinksIos = None,
        playback = None,
        synopsis = None,
        image = None,
        rating = None,
        provider = None,
        genres = None,
        cast = None,
        directors = None,
        availability = None,
        download = None,
        isOriginal = None,
        isAdult = None,
        packages = None,
        country = None,
        timestamp = None,
        createdAt = None,
        parentId = None,
        parentTitle = None,
        episode = None,
        season = None):

        self._platformCode = platformCode
        self._id = id
        self._title = title
        self._originalTitle = originalTitle
        self._cleanTitle = cleanTitle
        self._type = type
        self._year = year
        self._duration = duration
        self._deeplinksWeb = deeplinksWeb
        self._deeplinksAndroid = deeplinksAndroid
        self._deeplinksIos = deeplinksIos
        self._playback = playback
        self._synopsis = synopsis
        self._image = image
        self._rating = rating
        self._provider = provider
        self._genres = genres
        self._cast = cast
        self._directors = directors
        self._availability = availability
        self._download = download
        self._isOriginal = isOriginal
        self._isAdult = isAdult
        self._packages = packages
        self._country = country
        self._timestamp = timestamp
        self._createdAt = createdAt
        self._parentId = parentId
        self._parentTitle = parentTitle
        self._episode = episode
        self._season = season

    @property
    def platformCode(self):
        return self._platformCode

    @platformCode.setter    
    def platformCode(self,newPlatform):
        self._platformCode = newPlatform

    @property
    def id(self):
        return self._id

    @id.setter    
    def id(self,newId):
        self._id = newId

    @property
    def title(self):
        return self._title

    @title.setter    
    def title(self,newTitle):
        self._title = newTitle

    @property
    def type(self):
        return self._type

    @type.setter    
    def type(self,newType):
        self._type = newType
    
    @property
    def originalTitle(self):
        return self._originalTitle

    @originalTitle.setter    
    def originalTitle(self, newTitle):
        self._originalTitle = newTitle
    
    @property
    def cleanTitle(self):
        return self._cleanTitle

    @cleanTitle.setter    
    def cleanTitle(self, newTitle):
        self._cleanTitle = newTitle

    @property
    def year(self):
        return self._year

    @year.setter    
    def year(self, newYear):
        self._year = newYear
    
    @property
    def duration(self):
        return self._duration

    @duration.setter
    def duration(self,newDuration):
        self._duration = newDuration

    @property
    def deeplinksWeb(self):
        return self._deeplinksWeb

    @deeplinksWeb.setter
    def deeplinksWeb(self,newDeeplinksweb):
        self._deeplinksWeb = newDeeplinksweb
    
    @property
    def deeplinksAndroid(self):
        return self._deeplinksAndroid

    @deeplinksAndroid.setter
    def deeplinksAndroid(self,newDeeplinksandroid):
        self._deeplinksAndroid = newDeeplinksandroid

    @property
    def deeplinksIos(self):
        return self._deeplinksIos

    @deeplinksIos.setter
    def deeplinksIos(self,newDeeplinksios):
        self._deeplinksIos = newDeeplinksios
    
    @property
    def playback(self):
        return self._playback

    @playback.setter
    def playback(self,newPlayback):
        self._playback = newPlayback
     
    @property
    def synopsis(self):
        return self._synopsis

    @synopsis.setter
    def synopsis(self,newSynopsis):
        self._synopsis = newSynopsis

    @property
    def image(self):
        return self._image

    @image.setter
    def image(self, newImage):
        self._image = newImage
    
    @property
    def rating(self):
        return self._rating

    @rating.setter
    def rating(self, newRating):
        self._rating = newRating

    @property
    def provider(self):
        return self._provider

    @provider.setter
    def provider(self, newProvider):
        self._provider = newProvider

    @property
    def genres(self):
        return self._genres

    @genres.setter
    def genres(self, newGenres):
        self._genres = newGenres

    @property
    def cast(self):
        return self._cast

    @cast.setter
    def cast(self, newCast):
        self._cast = newCast

    @property
    def directors(self):
        return self._directors

    @directors.setter
    def directors(self, newDirectors):
        self._directors = newDirectors
    
    @property
    def availability(self):
        return self._availability

    @availability.setter
    def availability(self, newAvailability):
        self._availability = newAvailability

    @property
    def download(self):
        return self._download

    @download.setter
    def download(self, newDownload):
        self._download = newDownload

    @property
    def isOriginal(self):
        return self._isOriginal

    @isOriginal.setter
    def isOriginal(self, newIsOriginal):
        self._isOriginal = newIsOriginal

    @property
    def isAdult(self):
        return self._isAdult

    @isAdult.setter
    def isAdult(self, newIsAdult):
        self._isAdult = newIsAdult

    @property
    def packages(self):
        return self._packages

    @packages.setter
    def packages(self, newPackages):
        self._packages = newPackages

    @property
    def country(self):
        return self._country

    @country.setter
    def country(self, newCountry):
        self._country = newCountry

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, newTimestamp):
        self._timestamp = newTimestamp

    @property
    def createdAt(self):
        return self._createdAt

    @createdAt.setter
    def createdAt(self, newCreatedAt):
        self._createdAt = newCreatedAt

    @property
    def parentId(self):
        return self._parentId

    @parentId.setter
    def parentId(self, newParentId):
        self._parentId = newParentId
    
    @property
    def parentTitle(self):
        return self._parentTitle

    @parentTitle.setter
    def parentTitle(self, newParentTitle):
        self._parentTitle = newParentTitle

    @property
    def episode(self):
        return self._episode

    @episode.setter
    def episode(self, newEpisode):
        self._episode = newEpisode

    @property
    def season(self):
        return self._season

    @season.setter
    def season(self, newSeason):
        self._season = newSeason
    
    def payloadJson(self):
        return {
                    'PlatformCode':  self._platformCode,
                    'Id':            self._id,
                    'Title':         self._title,
                    'OriginalTitle': self._originalTitle,
                    'CleanTitle':    self._cleanTitle,
                    'Type':          self._type,
                    'Year':          self._year,
                    'Duration':      self._duration,
                    'Deeplinks': {
                        'Web':       self._deeplinksWeb,
                        'Android':   self._deeplinksAndroid,
                        'iOS':       self._deeplinksIos,
                    },
                    'Playback':      self._playback,
                    'Synopsis':      self._synopsis,
                    'Image':         self._image,
                    'Rating':        self._rating,
                    'Provider':      self._provider,
                    'Genres':        self._genres,
                    'Cast':          self._cast,
                    'Directors':     self._directors,
                    'Availability':  self._availability,
                    'Download':      self._download,
                    'IsOriginal':    self._isOriginal,
                    'IsAdult':       self._isAdult,
                    'Packages':      self._packages,
                    'Country':       self._country,
                    'Timestamp':     self._timestamp,
                    'CreatedAt':     self._createdAt
                }
    
    def payloadEpisodeJson(self):
        return {
                    'PlatformCode'   : self._platformCode,
                    'ParentId'       : self._parentId,
                    'ParentTitle'    : self._parentTitle,
                    'Id'             : self._id,
                    'Title'          : self._title,
                    'Episode'        : self._episode,
                    'Season'         : self._season,
                    'Year'           : self._year,
                    'Duration'       : self._duration,
                    'Deeplinks'      : {
                                        'Web'     : self._deeplinksWeb,
                                        'Android' : self._deeplinksAndroid,
                                        'iOS'     : self._deeplinksIos
                    },
                    'Synopsis'       : self._synopsis,
                    'Rating'         : self._rating,
                    'Provider'       : self._provider,
                    'Genres'         : self._genres,
                    'Cast'           : self._cast,
                    'Directors'      : self._directors,
                    'Availability'   : self._availability,
                    'Download'       : self._download,
                    'IsOriginal'     : self._isOriginal,
                    'IsAdult'        : self._isAdult,
                    'Country'        : self._country,
                    'Packages'       : self._packages,
                    'Timestamp'      : self._timestamp,
                    'CreatedAt'      : self._createdAt
                }

