# -*- coding: utf-8 -*-
import time
import requests
import hashlib   
import pymongo 
import re
import json
import platform
from handle.replace import _replace
from common import config
from datetime import datetime
from handle.mongo import mongo
from slugify import slugify
from handle.datamanager import Datamanager
from updates.upload import Upload

class RokuChannel():
    '''
        Scraping de la plataforma The Roku Channel, la misma está asociada a una 
        serie de reproductores de medios digitales manufacturados por la empresa 
        estadounidense Roku.Inc. Presenta algunos contenidos Free to Watch, mientras 
        que se precisa suscripción para acceder a otros titulos.

        Para obtener todos los titulos primero se analiza la pagina principal, esta 
        trae aproximadamente 1300 contenidos, de los cuales se obtienen los ids de 
        los géneros de cada uno (acumula un aproximado de 114 géneros sin repetidos). 
        Luego, con los ids de cada género, se accede a la API de cada uno para traer
        todos los titulos que matchean con ese género. Esto acumula un aproximado de 
        6600 títulos.

        DATOS IMPORTANTES: 
            - ¿Necesita VPN? -> SI.
            - ¿HTML, API, SELENIUM? -> API
            - Cantidad de contenidos (ultima revisión): TODO
            - Tiempo de ejecucion: TODO
    '''
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config = config()['ott_sites'][ott_site_uid]
        self._start_url = self._config['start_url']
        self._platform_code = self._config['countries'][ott_site_country]
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        
        self.skippedTitles = 0
        self.skippedEpis = 0
        
        self.sesion = requests.session()
        self.headers  = {"Accept":"application/json",
                         "Content-Type":"application/json; charset=utf-8"}

        self.content_api = self._config['content_api']
        self.content_link = self._config['content_link']
        self.genre_api = self._config['genre_api']
        
        if type == 'return':
            '''
            Retorna a la Ultima Fecha
            '''
            params = {"PlatformCode" : self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']
                    
            self._scraping()
        
        if type == 'scraping':
            self._scraping()

        if type == 'testing':
            self._scraping(testing=True)

    def __query_field(self, collection, field, extra_filter=None):
        if not extra_filter:
            extra_filter = {}

        find_filter = {
            'PlatformCode': self._platform_code,
            'CreatedAt': self._created_at,
        }

        find_filter.update(extra_filter)

        query = self.mongo.db[collection].find(
            filter=find_filter,
            projection={
                '_id': 0,
                field: 1,
            },
            no_cursor_timeout=False
        )

        query = {item[field] for item in query}

        return query
 
    def _scraping(self, testing=False):

        payloads = []
        payloads_episodes = []

        scraped = Datamanager._getListDB(self, self.titanScraping)
        scraped_episodes = Datamanager._getListDB(self, self.titanScrapingEpisodios)

        # BORRAR:
        start_time = time.time()
        # TODO: ESTE ES EL QUE VA: titles_ids = self.get_content_ids()
        titles_ids = self.get_content_ids()
        # titles_ids = self.ids_de_contenidos_temp()

        for content_id in titles_ids:
            
            content_data = Datamanager._getJSON(self, self.content_api.format(content_id))

            # Creo una lista con las 4 opciones válidas que puede tener un tipo de contenido para validarlo luego
            possible_types = ['series', 'shortformvideo', 'movie', 'tvspecial']

            if content_data['type'] not in possible_types:
                continue
            else:
                self.general_scraping(content_id, content_data, payloads, payloads_episodes, scraped, scraped_episodes)

        Datamanager._insertIntoDB(self, payloads, self.titanScraping)
        Datamanager._insertIntoDB(self, payloads_episodes, self.titanScrapingEpisodios)

        print("--- {} seconds ---".format(time.time() - start_time))

    def ids_de_contenidos_temp(self):
        ids = ['7a355e1c34585118b2eb4773f8b5f409',
         'f527c213ae24561fb384c4adbde8c820',
         'e0eb4a1031a25a7385412e69d8e14532',
         '1a0701643b59569ba810ac3d283e6eda',
         '533472a4a2485a6985e9a5d51fc7151c',
         '890cdaada81b5f8dafa6c3aebfbe4bb1',
         'dc6e92c6a463540e9a7bfed41b3afa9f',
         'b2b837d3b6545137a3fa7cd3c676a163',
         'd1e49ec323f45428aea1a67fbaf2a31d',
         'f4fb7b63603952bdaae66957cc041d43',
         '39d021f958925737a2f5b96b722ecdd9',
         '32c6d2dfa0ce5161b5e0b0ca74028f67',
         '06f0b5d7627f5e258bd6b2f068536fd0',
         '0092dbbf98db508194ae8fe2a0f1a3f7',
         'c5144fcab44f530fa5714f47f8ecbce6',
         '544282ba5f035165868c9ad883c0d041',
         '15d198e4312253d3a2131f0e1a9f5d03',
         'f7519f52bc5d525987cbc2d394d82995',
         '138067330ae95a14bd1be9e0ed617ec6',
         'a5af246f979857d98c65485163943116',
         'f2549100bccb50039c2bece5c5eab391',
         '928631e908aa5462ae3f4fa8bf41c121',
         '42e3d365745b5759a1b45718bf79e063',
         '0fff8c1750de5517a2e6e976c8a42fb8',
         'd7adf08bd1a4593499bcb8f77bace3ac',
         'a6114130155d514cb96f838d7bec1203',
         '1c95223d70ca56d6aa080fdaf27a51f2',
         'e9583e892c97577c884741a8899e05c8',
         '35cc69f6f0095c94884b2620dbc3b310',
         '24b0687564a458cfabedb5fd9d72b0ac',
         '848532924c6a58e1a7f1d1c87701513c',
         '4382bfb1691c581ca13586ff0f78c1c5',
         '54e9dbcc05b55a8f991c3106f6ba2a21',
         'ddddbfe64507505bb59a79a5ebf5aada',
         '31d9b02baad9515b865ad359978be159',
         '4680e8cd2eb95618b03b57db0e1fb712',
         '331d50f93f4453e9a043bb22b0690d25',
         'a5b744217449588684d2c2a435a6ee00',
         '46fdb83ec10b5201b5b27cf72a3d3332',
         '2a82c22fc4e5512c90cd5312b024295b',
         '989a4be460c15555a3741c938bc82d5b',
         '094637fb3f185505a7a627582d5a8609',
         '7e6e92569c4f51bbb7d50fec276f87a6',
         '73767ca612ae55aba1ed1208ffc5a719',
         '87f31c74731659b3a6aff6f2ef15bacb',
         '2f0d55c28ca7572dbc7f3fa42779035e',
         '821aa0bb55ae5051969e96380b8e0789',
         '9c9a42297d1e5f1ebc67ade090dee32a',
         '3aaabd4f70055fb3865035dc43e8153c',
         '21ee831722c45c7bb4492a3433867560',
         '00ac18ea2182553397272ac7ae2cb08a',
         'ebd890b0e0ce5eb49de0f4e6a97d6d06',
         'f1e39f7ed2365f5599726abb6365032f',
         '37830456e9e15fa3bff8bdb4adc8264d',
         'a973fa935ff35b4281e66e5c87f50a74',
         '3603f1adbc11569eb1c938b618e0988a',
         'b70441927b725444a6b0022903d42305',
         'abb622cdf5f152168d0dc4c8d30b4e47',
         '930e52954bc454269a64eb99bc00441c',
         '4702608279c45d7a983c9b7f0cd2be14',
         'a22ada3347fb5c13bf7ab1169fd20a41',
         'e6f556915eed5f218e122f99b98d8959',
         'eadcfc46f5b656a7b7b1c2f54ea0ed9b',
         'ac945921d1225ee385f0b93726f21365',
         '52d54a6cb64f54b7b7055d25dda326ed',
         '0d83b666c0b05db5aed50c820b90b1ba',
         '689cec42402a52d093eb898f93260396',
         '031749447ad158978e31a4d441f9992c',
         '0b03601c6acd5764a02933792030ce66',
         '2572a645e5865c4f890392fe3565ab2f',
         '488e863478c856ceb67718de341a73f7',
         '9cd0c77bdca05e5aa5ba91b7f1f84ae3',
         '2c32763b04f356039b3b0a36e76ac83d',
         '044210b4084d5a59b2c0983a7275d8c5',
         '918d3643aa815a5d839b1f72c8305a10',
         '7f11ad795faf5f5f967abd5065b31c97',
         '66600e7977415d9fa2ac99796f387411',
         'dc8c8829622a576b967ed70498b91f95',
         'e675ac4b37585a00883cf2a4b3235dfa',
         'a1d6542ca76d5490948a8194bb7f088e',
         '100c440eb7fc5a2fbf9505adec7eecf3',
         '786e49e2a54b5eb3b4fe403cef2da07a',
         'c3e2ad24c4d2524c83479cc71c5f87b6',
         '7468430b9b615993b00abddaa793ecc2',
         '0cd3a2fa9a56574b9758628be4ade31c',
         'db6a098c1a865b54a9582c947a15be97',
         '8088cb9473b65b949ced7d3a73c332ea',
         'c863dbab5bf9502b9b02245bab4569f4',
         'a899f8c578715e8fbe0739ca1865b59c',
         '7036393bde885ac9921f407ca7f575ea',
         '0ef29fcb35d65688ba44efe09584be3b',
         'd32cddb183635e639c76dc193e0470a6',
         'e0559b3b07d25678898784e9f17c8800',
         '4463ada099715c54b06d85610af6355f',
         '71d7dcf7dfea51598cb7780d9592e1aa',
         'a735e1db74545c77a7636e67cac3798c',
         'f9a4f522c7775a238c4853a9324956db',
         '215e002fe7a95302a0b065032dd9250a',
         '475cdef33f4b5a94ab2387f08e287084',
         'c44af8b40aee51e598ca4cbc0c6ead99',
         '763ac3be176e5834b3bf2adfd903da08',
         'b5709fc636e65ebcb3b38ba91b3de228',
         '742ab352f3fa55189b7e752774d15d94',
         '1c4ef1a9eeed57b99c403b6f60fb7226',
         '880953df788e55a9ae18681c68ec8c1f',
         '432024e17ed45a5393c059a09c516b4f',
         'a4d73aa3097f5a6dbd8c7a9d64acf1a8',
         '754331283d1c5cf6b438c5c849f56e6d',
         '0f3605dc7ba9590fa8b0ca9aa30ca975',
         '9131e9d28bb95f2cb0efdee9e857bd3d',
         '5e4df8c78134559f8ce562ec78693ec8',
         '64ffa1bda36150ff97302b33c559f40c',
         'c8f2be63e2bb5c4b8168682deddab7ae',
         '1ff3cf5beb425b0bbe1831ca2731e930',
         '5866bd40ebb35af387153b329e1cbbca',
         'df8bfc2435c25c33a5c5cadec90b322e',
         '51c7d796bf905a82bb60d56a6b2dd136',
         '19cb07d346ca57a08b41ce1dca9f62f0',
         'c2b9cc395d245aceace1e7b9817e4eb1',
         'd664d722bd465c4cb90dc07512c7f6e9',
         '52d20d951d125e0f8b631abce5a3275a',
         'a4b76244c73559c2bc76d208d06a3767',
         'f43c0c0d97c05e20a5ce73a70007e6dc',
         '1f3da1668c3255d4973a13076554438a',
         '30c805e563ba50cc88d873b705b95e63',
         '097084f1f35e53d497976d3d6e5fdc92',
         'f3eae371c37a581fbe49a6edb9063b00',
         '73c4429686c25ee59bfc007bd1830cd3',
         'f10ab5983a9e50559dcae7dd452d03f9',
         '458f3936499d5efe9014e0d01dfebc06',
         'b3868b046e935858af8b67dfdbca7683',
         'd93e64c7b0fc5b8f9a1310ec6d266f72',
         '32ccb31cf0fa57b98feccefbd8046eef',
         '02675cc7098b56fca4ead13903c7a154',
         'a508a03f49855bfc85f4bd42058bd322',
         'e7985500c3845f52986f6f6ee0dc2117',
         'e7396d3150145dd0890be01a7d8e263c',
         '21d9ed64a61f59e884cf014ce08f1091',
         '0faa3e94619957c48e669d96c96b2d74',
         '534026c6625f52969d21048647492e96',
         'c625f188dbe157c98b742e8e3a8a63ab',
         '881822d0fe9d5eec8a3fdb4914e516d5',
         'e57ccfe3c5f155e7ba56a2dfaa9b6ab2',
         '15323c6104625745af53850b4d9d5a04',
         '6c3758ea09195e0f990ce8937a2fc691',
         '2fa4982a3d4d51c5bebb9078958c8238',
         'cd003cee61c75ceba384edc583cd9c5c',
         '4b0a46775a295feabb590f40bc856ce2',
         '295e5fa97224552fa32e4de344acc077',
         '1a14f147bb5354cd8a496a21fea68452',
         '3f5ba786b66c55f992ada18da9e6cf8e',
         '4763792e37de5a4db8f9caafdce45b90',
         '7bda7b48163e520989e95be10171105c',
         'bb17755883035a35a8411c80bfa6f5dc',
         'cf6f206a1c585c898c500aed5c137e1d',
         'b7da1cbdc72952d6b67be8fb4ff4eb6b',
         'd49ab6ab0ddb5f2ca6274e7e856486ce',
         'bb726cc39517533e8ad56a42c9b837f6',
         '82d317e1322755e49ef91b8f674c3f94',
         'd0a8d3681f165ec297799ce474865919',
         '1f6dc71e7d2d5cefbd69cce943bfd819',
         '6006518f92ae540e96e6b31c13a83e5b',
         '48072ffffff857bd9feacfcc924f48c3',
         '593f1a8b6cb75679b34e726b182c7d00',
         'bf1b276413365c1981fe2689573611d5',
         'e442d5eb79e05d559a405cb562668dce',
         '9e98369b89fa5f00a5e8f663b28591f9',
         '6d847b47abe057c9a4794f21b3573418',
         '2cf91fe3bb6f58a38616c83771ca621a',
         'b04d100367ea538bac1b61ad84574052',
         'ea5b8240019a5c4c801e5d6408648317',
         'b472233bae3550ac9c3f74b1f086cf27',
         'df67f9b908f358c1bc1d52ecf08ecf09',
         'eaf77cecfa445080a206c3f4ac34eb89',
         'ae262ab3eb0b5e6bbdebf89e50e43d54',
         'fcb6d6a9aebb51b29053fd6e5a356c96',
         'e75add81b1f75b6897cf60d74eef1068',
         '5a631313dce85cb5a3fde5ee77a70c5f',
         '599fb399182e5cc4be5efec35e1646fd',
         '2cf10e6d189a524981f6d016bab3e27e',
         'ed2f2b187d9d5622b8f3a6a77d6138e8',
         '0bf1ac925c6d5169ab7c8f1ebee2eab0',
         '65687f9903fb555ab5cd2e9534940390',
         '9737f130e5e45fd4915602c293c8472c',
         '0c861dc330905093a863e46970fce13a',
         '31e60ca54b115d66858a46df87b3b658',
         '46b4325b4cd157a696b75582e980cc15',
         '3c444f5cfb0557399878964826a40f65',
         '3d15e4170ed05627a4b2a2e3c8ceb94d',
         'b536dba3a84d54768d0bf609fd2b0c2c',
         'bb229aee949350e2b5ed5ef1e06b55f9',
         'd67e6ef59e2c5b5e9254d7043efa397e',
         '0ad8717397775c23b4b63afa58f8a293',
         '4522f2614ecb5503b15a751dcae2c32a',
         '5a282495d5f0590d8adf73198a943a23',
         '110b79cce5ce524b81ff64d9c3695856',
         'dc66d61b84b355fdb53531f86262eafc',
         '99043712cf5c5c6dbfce5782b52c3972',
         '9d910c5c5d975546a4e683b6b9e587db',
         '3cfb8bd42fb256b588e2868e2edbacf2',
         '5870540f5ff3593aa5e9279d3a0ed8d0',
         '78a354bf9aca5127a20442e69b09dc76',
         '2738cb61c1835b2cbc3e12f8bf9481fb',
         '6aad24975bb85ff6968c67199f9b39e3',
         'b7aecaed070352f190c5fcf686c8d41a',
         '39cd7b5a4d3f5bccacc83f867ef0bc0c',
         '42ffaf7a23005266a89e5c7a22143bd9',
         'dc261f679d8859499d0f294cf39a9351',
         '16aad6b4081954f0900e0ea85b39baa2',
         '480592a1379c509b9b2c015da12011fd',
         '5b8fa1394e185ca0b4def2d71568ae95',
         '9c38dd21af0c5f02a5e2df2711d30a68',
         '65f72f2eedaf5077b553e2ba69196dad',
         '9d36c56794d450b5a91a8971df1454fa',
         'e606c2e702a35ad4a3a52b3817e89d61',
         '5a4d3a79d4715acd958030fe7cd077a9',
         'ea0220c52ee059719013cba442d31eab',
         '24c3b1f249fa5c21a0150562885ecd11',
         'd1bbcfb53ec459bdbf26ac082054d6bb',
         '66cc50e20b9850979362bca326ee4630',
         'a0e863ae4bb051fa9d10258e70f5195f',
         '7c2bfcaae9955b3b9578248ace5f52b4',
         '0559cd84e2665ab18868e646e21df3fa',
         '172ed53712105842bd390ffe4947073d',
         '678fda66dfeb566ea1d955305cb78682',
         '0a6fa8bdd284562283fbeb046a0d8309',
         'e052bf7a9fc35d3b8f977d51ee53ae35',
         '2e06cfa29f575e67bb37e05ae039a67f',
         'cae9a74cf1995375923ab351d15a939e',
         '1bc2aff28b6457a1a3f4d82c92593fb9',
         '506dfdbd199a5841a65784d914ff0a4d',
         'b2c750ea26f25c3ea43321558d46e7dd',
         '261df197be8251d98aa2d97151763996',
         'eea2d158dddf50d8817b6b3bc8d7bc87',
         'f9195421d09a53e18572a7ac974869da',
         '35834305af4759f99b51b892b1fe83f4',
         'c8387bd685a55b63ba00207fbf38ba24',
         'e81cbbd11d885646b7c469297056d9b7',
         '3501d39a2049505d9279be2bb9f8b4d7',
         '8f589c64cfd8508d86ba386018bd65bd',
         '3886599a78a6589d9cc8392492a5c8cd',
         '59f799935532591597345dca46fb3c29',
         'ba60f42a898053e3891d37ebbcc7c392',
         '9358f0940cd151cb83cb9f164c6f20d0',
         '9e8eb77f7c4c513ab8228a59bd5b31e1',
         'a7c24adf3d4653608a22ff73d5cda223',
         '7e1430e0701e5802bf2aec8f300ba337',
         '0c8ba7dda7d9593cb9c3f4ac443b3519',
         '110ac87ca3af50518917e27957127c5b',
         '1d9776e3ad00558c876f7cea0e33e3b3',
         'f5dc3dba63d857fd86363c3b6a700f8d',
         '54df7c51a7d4575b9a4c7c384a6b4429',
         '17a17c4f21aa5e60b1abc783951ce6f2',
         '9e162de1a5e2569e8a48cbf8cda22748',
         '28acf7f0788e50edbfdc6783922957ba',
         '28550fb8a4f7517fb16ea8b84cb4f664',
         '5ae6f3895f1d5ddf9853635b349e3591',
         '404e0efa9e0355f0b35ff37ea8a904b1',
         'e7f095e742f852bc93009d6c3329c126',
         '93fdbb7ab632599b8479dc8bc183c389',
         '4fe4999670405bb7be7b7f3ca1f139fb',
         'c5e7ee41efa35c27b93109c70e9e3ff6',
         '2979dc800a9c530c805eceb3ad9ee882',
         '73a783a14c4750fc8c66c1ac728b2931',
         'ea022c0524b4508a82add4d16636bdce',
         'c6c11c6d0c415741ba9129ef5f2921c6',
         '6f14b6da84d45c5bbcd4728952caa7e4',
         '4bfc7b9b431a591fa9c8666544da8405',
         'f98399e2bc8152899b64264f723c017c',
         '29a5fee1fa6d55c5ac58ed632d81625a',
         '9bb4b0ed31f15b09ba8f30fdba469e02',
         '9b9a1f58fec05de3a223c08921879c5f',
         'f8cbd263fe825b729a5d71b5e8e4814c',
         '9305e3bb3b2a5e07ba6bb7766378a454',
         'd9495e1604865aa392e1cd3335ce851f',
         '260c30ba15535cc7bb53102da019d4c4',
         '65cc07dd52bb56648387ed49f0246b9d',
         '6c202f4fb5395e448a2444ea8368499c',
         '3e016272a47e5f929e723e52ca937740',
         'ceef0b466a295d008366e57c1523e44d',
          '7abf5e84134e54a394b5b42544c08caa',
          'd38b81bc998b58fea79628d5da40bb0b',
          '954efb990c645c29ad81e6537112a1a2',
          'c10f644bd45658d0b2b6da3a6bcbdc30',
          '384502b6c3935d10a465f91109036035',
          'ae13ec1c35e25104b3655968568b8fd0',
          'd00ffd45b20e5652b8ced272c94a5b43',
          'aaf489f1b92854ffb45aa72b8835bba5',
          'bc5542a48c3b5705befc25e87b820b54',
          '517e2456db2d59c49b3c075e49efdb7d',
          '259d28fbca455ed1a9a295c1a13ac390',
          '69f98d6da17e5f2e9c84eaa98e87fa09',
          'bc62514a06d85196a52019bc2a2ca16e',
          '09f94bf876b65820b1b3fbba69912f45',
          'dc65fa5ccc0752ee8010099707e443ab',
          '1589185316295fb9b742795a110212b5',
          '1000653156e5566b8053d695302cda17',
          'd40d0100f0615b0190aec2bff73ccaff',
          '4874dcc154435870b0b6489702df0717',
          'edcec6f888745bdeb9c531402aaa1f61',
          'f00e51ee161c5dd3a26fa4dcca47ce7c',
          '37761f8da38b5f0a8ca2eb454a780dfa',
          '2e32fa79be9855708a29edefc136c406',
          '5efa7a2fbd4f5acbaa80293f15ad433c',
          '292b6b7a99945dc88a584c5a20157a5a',
          'e71ef31ff9db525abf79e259f436290b',
          'e65795dff00056ad8ea41bfbee660442',
          '0c6d9e7dc3085c01bda5372a067e554b',
          '9cdaf016efe45813ab6ea8175f86e5af',
          'e9c8d25510af57f6803acf07b9cf1c2a',
          'aef3d844d012566a9ba768caa0ff63ec',
          '7cffd6593dbf55b78077bb2c704f4d4f',
          'e683755e1a0d5b5e9ceba42cdd5d3972',
          '00c97dd7daae5f19884a0c30891ad013',
          'be488b18df67529b92b32e6237f36640',
          'c2fbac1461b45d6688e3613919cbdebe',
          '65711108a55f5f08867b6d8c6a51e140',
          'd308a3759dc15222bdabb03ba7eef92e',
          '064435cf04a35048824b840a7a7e52ec',
          '2eae3cf1142355b794084b5e0c6dc93c',
          '504faa4925c55df7a37e29dded36938c',
          'b9e1356d2250559294569498e26dab29',
          '65cd1b59c76955768acd005ea8628eb8',
          '77d49532069b5bd585ef21da5a9c0c2d',
          'ea0800bbec4e5d938ca72a1695e57f76',
          '516e564dfedf5d70aa87b446e2f72e92',
          '77a1114cb69b56e7a5f0943f3217f7e7',
          '5ad8e434cead5102bc39bfc9afbef853',
          'f367835ab9b95a699530b00d519fbf3a',
          '11c0b1d2d5ad58e787ff36d8167a2130',
          '4a517e77a6015a2fbed6b75dc45eb180',
          '5f3ac7bffb7557ac86df67ffe4781a6e',
          'ee8fa6a7e57d5a2bb8b0e86ab109ac64',
          'b7a884638df7569a82647be7ca773331',
          '3a7f48e634f052a88372c544ae0bda63',
          '724319568ad15ce0a8904c630720ed65',
          '8b39db62e07754998055d1dcc32a5b09',
          '8331131c936c5e0ea327abf5717b6fa4',
          '49413a0ae5d35bbbac1864d2bcc62e0e',
          'c48473963de15ef19feb72594df400a5',
          'b193e4feb73e54b7b3ea93994a66040e',
          'bdcf7a911aeb5d33afcb4f0b3a2375f8',
          '2fd3da423bdc5151b8301dc71d9b2c14',
          '8b151ab0dc105064b37a1714a81e468c',
          '90cd5c57dad0562786af5214faf4fd54',
          'b53e2f0fd27a5051a8d0df8ca825929e',
          '8c485a4582a95e539e33ab03d14da3e7',
          '68aee6dfaa915992a39705cb2889055d',
          'c36a7df5440a5ad19048f607b79323e6',
          '7d093871c9ac5706a7d22fcdb46c7a55',
          'b9dd832b8b68535da5fc7c442a8372da',
          'cd8be8b3e76759d391b229b74b14587c',
          'd2e211aae6cb5765955221f0d17165a2',
          '6ceb394d1ff4561a8c735fde3fd87fed',
          'a539d560350858caa5623c1410a08ee8',
          '451ab9e553d65b928a303788511cb5f8',
          '2dedf92f98d857f99fb3f082c7a6c82f',
          '75de3ad2531f5de2851d6f25a6ffc040',
          '337f588292275e66a624e9f2a67ce1bd',
          '51f3bc00daaa5ab2965029db021e8021',
          'c9c4be41078058a2922afdcfc5a33ce4',
          '362b62edda99585aba5719c3ecc76e99',
          'a7397a82127858f0b31372f54924cb1b',
          'c1f29184e1db539197245c64246a635b',
          '6fc46c85c6705c3a91afafac0fa8a029',
          '023e3161d5ec549f870f950d072270c2',
          '9b48a492fc055d05bc7d892be172c956',
          'abb29d9ce33850329fbcc3c02c8f5b27',
          'f34a370495bf5430a5aa7209fb1661fd',
          '7ca91328f20a50d3bf789d21b4944536',
          '45278efe120b5d5f9f8755dc816fc1e3',
          '8894a88a65185e4e8d35222fa05cf617',
          '629ac75a7b2a5708b738e522032954c7',
          '7c2f40ff38695c2f8fdd685f9e9c1b00',
          '1deb9412a1565e12a0132933ace24373',
          '27912f78cbe2501db437e77c7b5fe60b',
          '941dbb6fc4105dd8b2573374dbdfc9b4',
          'cc3481f061e259ef812a3bfdd651baa1',
          'd1e68ab55f505ca9b3a49015ca74f2ed',
          'b550788d5a0452148ce0dec3c36fefa5',
          '07feb5c82e375caa91f0712e197a77d8',
          'a588857e37a9500eb0e5a8a45b121e9f',
          '85b9d65ec61153e5b0391d9d23700dd6',
          '5f8dfa21fc61509280c7b9ce979bc07b',
          'da5d6ebbf623504fb70fceeb3a3e16d4',
          '840a8c9c63db5803bf54f6a903c79624',
          '516e03bed36b532c85a628b0c0d1a8e3',
          'b14ce00038b45960b435f887526043cb',
          '378d03afe54a5c2682165ee900856512',
          'bb0db0ad47605827a49957fdfecfae71',
          '6a7b2a4424855a97b4e879a740d0b5bc',
          'c7c1c941f3e8500d9da8f4ed8029b3ac',
          '89e9297ee3055a959943834103b189d1',
          '171f0aab4bab5270a8942689b6690552',
          'ca71a14c32ba5448810b0210aaa54f31',
          '6f2afaba9f275ed8b84638b75ef97dfe',
          '48b22a15b2f15e1f8d6fc839d844c885',
          'a9d751ced16155a582f3e316f8c7964c',
          '0d03e596be845a85b931b19f0ffe5dc6',
          '0cd0b78574845957b90e8e200e4784e7',
          '55d3551b5eab51c98779758d34a924ee',
          '7682018e733f57629e65ebb939d18754',
          '01a5462d8ce35082b9d5e78ba976c0f9',
          '06d82e0bfa67521092c3a32408a920f7',
          '4afc5aa345bd5d799f953130c38b8a33',
          '58ec3ac4e61a50768dd6eeb03c6070d2',
          '5296a3ee51a65f8d8ad2532e38e1cfa4',
          'cdc02f58a4dc58f8a75664aa011ea4df',
          'ce752826d2ad550f90f738be5ade6d89',
          'f2b96301985f529cb109dea367c59060',
          '6360fb5635e359859b650ace56fe6ef4',
          '610964a22dee5569a16b9f906f6bd2bc',
          'e3456a642d6652ccac6b1218e916a538',
          '721d7f9b68e1510f85fb9f2629a64c90',
          '9a9905d24e885180a6a3731b7d4a3f98',
          '1ba7c862c1bb54fb8e22bcd820a411ba',
          '594a57da6e7f5fbfa0a4967102fbd6d9',
          'd764751399005366824f59f2ff2ff329',
          'fb2b3328297250f295c804397982d52c',
          'bc28710ae9f25e61bbefe22908e140ab',
          '71a2b49c336653fda7f200758e3e9b19',
          'a45afdcf52475aed92f4492281c2617c',
          '8fdd2e9bdc7e58fe98227758a3cb54e2',
          'f453494ea1675e8a877f48f664e2ddd7']

        return set(ids)

    def get_content_ids(self):
        '''
            Este método se encarga de analizar la página principal, con el objetivo 
            de obtener primero los ids de todos los posibles géneros (va trayendo los 
            géneros de cada contenido analizable en la página, evitando los duplicados). 
            Al tener aprox 1000 contenidos en la página se estarían trayendo todos los
            géneros posibles.

            Una vez que obtengo los ids de los géneros, accedo con ellos a sus respectivas
            APIs que contienen los títulos asociados a dicho género. Luego hace lo mismo que 
            en el paso anterior, acumula los ids de los títulos en un Set para que no haya 
            duplicados, con esto se obtiene un total de 6600 títulos aproximadamente.

            RETURN: Set de ids de contenidos/títulos.
        '''
        main_page_data = Datamanager._getJSON(self, self._start_url)

        # Este set va a servir para ir acumulando los ids de los contenidos (sin duplicados)
        contents_id = {}
        contents_id = set()
        # Este set va a servir para ir acumulando los ids de los generos (sin duplicados)
        genres_id = {}
        genres_id = set()

        # Hago una iteración con todas las categorias de contenido que presenta la página principal
        for collection in main_page_data['collections']:
            
            category = collection['title']

            # Como las categorías "Characters",
            #  "Live TV" y "Browse Premium Subscriptions" no presentan contenidos scrapeables,
            #  las salteo
            if category == "Characters" or category == "Browse Premium Subscriptions" or category == "Live TV":
                continue

            for content in collection['view']:

                content_data = content['content']

                # Busco el tipo de contenido,
                #  si no tiene un atributo 'type' probablemente esté parado sobre alguna categoria (la página principal mezcla contenidos
                # y categorias en la misma fila de contenidos) por lo que salteo al próximo contenido que sí sea scrapeable
                if not content_data.get('type'):
                    continue

                content_id = content_data['meta']['id']
                category_objects = content_data['categoryObjects']

                for category in category_objects:
                    
                    category_id = category['meta']['id']
                    genres_id.add(category_id)

                contents_id.add(content_id)

        for genre in genres_id:

            genre_contents = Datamanager._getJSON(self,
             self.genre_api.format(genre))

            # Valido que el género tenga una colección con contenidos,
            #  de no tenerla se saltea
            if not genre_contents['collections']:
                continue
            
            content_collection = genre_contents['collections'][0]['view']

            # Para cada contenido que se corresponda con el género actual,
            #  obtengo su id para agregarlo al set de ids
            for genre_content in content_collection:

                content_data = genre_content['content']
                content_id = content_data['meta']['id']

                contents_id.add(content_id)
        return contents_id

    def general_scraping(self, content_id, content_data, payloads, payloads_episodes, scraped, scraped_episodes):
        '''
            Este método se encarga de scrapear el .json que se le pasa por parámetro, 
            si se trata de una serie además scrapea los episodios.
            Para todos los casos se utilizan las funciones checkDBAndAppend e insertIntoDB
            del DataManager (se consulta con la base de datos y se sube a la misma)

            - PARÁMETROS:
                - content_id: el ID del contenido
                - content_data: el.json del contenido
                - payloads: la lista de payloads en la que se van acumulando los contenidos
                - payloads_episodes: la lista de payloads en la que se van acumulando los episodios
                - scraped: la BD con los contenidos ya scrapeados
                - scraped_episodes: la BD con los episodios ya scrapeados
            
        '''
        # TITULO
        content_title = content_data['title']

        # LINK DEL CONTENIDO
        content_link = self.content_link.format(content_id)

        # TIPO DE CONTENIDO
        if content_data['type'] == 'series':
            content_type = 'serie'
        else:
            content_type = 'movie'

        # AÑO DE ESTRENO
        # Hago una validación para obtener el releaseYear:
        if content_data.get('releaseYear'):
            content_year = content_data['releaseYear']
        elif content_data.get('releaseDate'):
            content_year = int(content_data['releaseDate'].split("-")[0])
        else:
            content_year = None

        # DURACIÓN
        if content_data.get('runTimeSeconds'):
            content_duration = content_data['runTimeSeconds'] // 60 if content_data['runTimeSeconds'] > 0 else None
        else:
            content_duration = None
        
        # DESCRIPCION
        # Como los contenidos tienen varias descripciones (cortas y largas, en ese orden) traigo todas y luego
        # obtengo la mas larga (ubicada en el ultimo lugar de la lista)
        descriptions = content_data['descriptions']
        descriptions_text = []
        for key in descriptions:
            descriptions_text.append(descriptions[key]['text'])
        content_description = descriptions_text[-1] if descriptions_text else None
        
        # IMÁGENES
        content_images = []
        for image in content_data['images']:
            image_path = image['path']
            content_images.append(image_path)

        # RATING
        if content_data.get('parentalRatings'):
            content_rating = ""
            for rating in content_data['parentalRatings']:
                if rating['code'] != 'UNRATED':
                    rating_code = rating['code']
                    content_rating += rating_code + ", "
        else:
            content_rating = None
        
        # GÉNEROS
        content_genres = content_data['genres'] if content_data.get('genres') else None

        # CAST & DIRECTORS
        content_cast = []
        content_directors = []
        for person in content_data['credits']:
            if person['role'] == 'ACTOR':
                content_cast.append(person['name'])
            if person['role'] == 'DIRECTOR':
                content_directors.append(person['name'])

        # Para obtener datos como disponibilidad de contenido, package y provider accedo a los viewOptions del mismo:
        content_view_options = content_data['viewOptions'][0]

        # AVAILABILITY
        content_availability = content_view_options['validityEndTime']

        # PACKAGES
        if content_view_options['license'] == "Subscription":
            content_package = [{'Type': 'subscription-vod'}]
        elif content_view_options['license'] == "Free":
            content_package = [{'Type': 'free-vod'}]

        # PROVIDER
        # Aclaración: Si el contenido es gratis ("Free to watch") generalmente el provider es TheRokuChannel. Los que son
        # contenidos pagos bajo suscripción tienen otros providers.
        content_provider = content_view_options['providerDetails']['title']

        payload = {
                'PlatformCode': self._platform_code,
                'Id': content_id,
                'Title': content_title,
                'OriginalTitle': None,
                'CleanTitle': _replace(content_title),
                'Type': content_type,
                'Year': content_year,
                'Duration': content_duration,
                'ExternalIds': None,
                'Deeplinks': {
                    'Web': content_link,
                    'Android': None,
                    'iOS': None,
                },
                'Playback': None,
                'Synopsis': content_description,
                'Image': content_images if content_images else None,
                'Rating': content_rating[:-2] if content_rating else None, # elimina la ultima coma del String
                'Provider': content_provider,
                'Genres': content_genres,
                'Cast': content_cast if content_cast else None, 
                'Directors': content_directors if content_directors else None,
                'Availability': content_availability,
                'Download': None,
                'IsOriginal': None,
                'IsAdult': None,
                'IsBranded': None,
                'Packages': content_package,
                'Country': None,
                'Timestamp': datetime.now().isoformat(),
                'CreatedAt': self._created_at
        }

        # Si el contenido es de tipo serie, debo agregarle el campo "Seasons" al payload general
        # También hay que llamar a la función que scrapea los episodios
        if content_type == 'serie':
            # Esta lista va a ir acumulando los dict con los datos de cada temporada, para luego agregarla
            # al payload de la serie
            seasons_payload = []

            seasons_data = content_data['seasons']

            for season in seasons_data:

                    # Implemento un try/except porque no todas las temporadas de una serie tienen
                    # información pertinente. Si algún dato no se puede traer se saltea la temporada
                    # y pasa a la siguiente (no queda registrado en el campo Seasons del payload de 
                    # la serie). Pero toda la seasons_data se analiza aparte para el payload de los 
                    # episodios.
                    try:
                        # SEASON ID, TITULO, LINK, NÚMERO Y AÑO DE ESTRENO
                        season_id = season['meta']['id']
                        season_title = season['title'] if season.get('title') else None
                        season_link = self.content_link.format(season_id) 
                        season_number = int(season['seasonNumber'])
                        season_release_year = season['releaseNumber'] if season.get('releaseNumber') else None

                        # IMÁGENES (SEASONS)
                        season_images = []
                        for image in season['images']:
                            image_path = image['path']
                            season_images.append(image_path)

                        # CAST & DIRECTORS (SEASONS)
                        season_cast = []
                        season_directors = []
                        for person in season['credits']:
                            if person['role'] == 'ACTOR':
                                season_cast.append(person['name'])
                            if person['role'] == 'DIRECTOR':
                                season_directors.append(person['name'])

                        season_payload = {
                                'Id': season_id,
                                'Synopsis': None,
                                'Title': season_title,
                                'Deeplink': season_link,
                                'Number': season_number,
                                'Year': season_release_year,
                                'Image': season_images if season_images else None,
                                'Directors': season_directors if season_directors else None,
                                'Cast': season_cast if season_cast else None
                                }

                        seasons_payload.append(season_payload)
                    except:
                        continue
            
            # Agrego el campo "Seasons" al payload con toda la información recopilada de las temporadas
            payload['Seasons'] = seasons_payload if seasons_payload else None

            self.episodes_scraping(content_id, content_title, seasons_data, payloads_episodes, scraped_episodes)

        Datamanager._checkDBandAppend(self, payload, scraped, payloads)

    def episodes_scraping(self, content_id, content_title, seasons_data, payloads_episodes, scraped_episodes):
        '''
            Este método se encarga de analizar una fracción del .json de las series 
            (el apartado de las temporadas). Scrapea los datos de los episodios
            y luego los carga en la BD mediante las funciones del DataManager.

            - PARÁMETROS:
                - content_id: el ID de la serie padre
                - content_title: el titulo de la serie padre
                - seasons_data: fragmento del .json de la serie padre
                - payloads_episodes: la lista de payloads en la que se van acumulando los episodios
                - scraped_episodes: la BD con los episodios ya scrapeados
        '''
        # Loop doble para iterar episodio por episodio en la lista de episodios de cada temporada
        for season in seasons_data:
            # Valido que la temporada cuente con episodios
            if season.get('episodes'):
                for episode in season['episodes']:

                    # EPISODE ID, TITULO, NUMERO, NUMERO SEASON, LINK
                    episode_id = episode['meta']['id']
                    episode_title = episode['title']
                    episode_number = episode['episodeNumber']
                    season_number = episode['seasonNumber']
                    episode_link = self.content_link.format(episode_id)

                    # AÑO DE ESTRENO (EPISODE)
                    episode_year = int(episode['releaseDate'].split("-")[0])

                    # DESCRIPCION (EPISODE)
                    # Como los contenidos tienen varias descripciones (cortas y largas, en ese orden) traigo todas y 
                    # luego obtengo la mas larga (ubicada en el ultimo lugar de la lista)
                    # La otra opcion es traer la descripcion que tienen por defecto (la mas corta)
                    if episode.get('descriptions'):
                        descriptions = episode['descriptions']
                        descriptions_text = []
                        for key in descriptions:
                            descriptions_text.append(descriptions[key]['text'])
                        episode_description = descriptions_text[-1]
                    elif episode.get('description'):
                        episode_description = episode['description']
                    else:
                        episode_description = None
                    
                    # IMÁGENES (EPISODE)
                    episode_images = []
                    for image in episode['images']:
                        image_path = image['path']
                        episode_images.append(image_path)

                    # Estas variables se declaran nulas antes de validar que se puedan obtener 
                    episode_view_options = None
                    episode_availability = None
                    episode_package = None
                    
                    if episode.get('viewOptions'):
                        # Para obtener datos como disponibilidad del episodio y package accedo a los viewOptions del mismo:
                        episode_view_options = episode['viewOptions'][0]

                        # AVAILABILITY (EPISODE)
                        episode_availability = episode_view_options['validityEndTime']

                        # PACKAGES (EPISODE)
                        if episode_view_options['license'] == "Subscription":
                            episode_package = [{'Type': 'subscription-vod'}]
                        elif episode_view_options['license'] == "Free":
                            episode_package = [{'Type': 'free-vod'}]

                    payload_episode = {
                                'PlatformCode': self._platform_code,
                                'Id': episode_id, 
                                'ParentId': content_id,
                                'ParentTitle': content_title,
                                'Episode': episode_number, 
                                'Season': season_number, 
                                'Title': episode_title,
                                'OriginalTitle': None, 
                                'Year': episode_year, 
                                'Duration': None,
                                'ExternalIds': None,
                                'Deeplinks': {
                                    'Web': episode_link,
                                    'Android': None,
                                    'iOS': None,
                                    },
                                'Synopsis': episode_description,
                                'Image': episode_images if episode_images else None,
                                'Rating': None,
                                'Provider': None,
                                'Genres':None,
                                'Cast': None,
                                'Directors': None,
                                'Availability': episode_availability,
                                'Download': None,
                                'IsOriginal': None,
                                'IsAdult': None,
                                'Packages': episode_package,
                                'Country': None,
                                'Timestamp': datetime.now().isoformat(),
                                'CreatedAt': self._created_at
                                }
                Datamanager._checkDBandAppend(self, payload_episode, scraped_episodes, payloads_episodes, isEpi=True)