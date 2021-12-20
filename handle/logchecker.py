import os
import sys
import json
from common import config
try:
    from root import servers
except ModuleNotFoundError:
    path = os.path.abspath('.')
    sys.path.insert(1, path)
    from root import servers
try:
    import settings
except ModuleNotFoundError:
    path = os.path.abspath('.')
    sys.path.insert(1, path)
from settings import get_logger
from settings import settings

# ###Notas:
# - Este script sirve para sacar los logs de los servers. Cuando se ejecuta busca la plataforma en Misato/titanStats y extrae cual fue el ultimo server donde se ejecuto.
# Luego, se conecta al servidor y saca todos los logs de la plataforma para ver los errores que hubo, etc.
# ###Funcionamiento:
# - Para mostrar las ultimas 100 lineas del ultimo log se usa:
#     Platform --c <country> --o log
# - Para mostrar las ultimas 100 lineas de una fecha determinada:
#     Platform --c <country> --o log --date <dia en formato YYYYMMDD o YYYY-MM-DD>
# - Para descargar el ultimo log:
#     Platform --c <country> --o logd
# - Para descargar el log de una fecha determinada:
#     Platform --c <country> --o logd --date <dia en formato YYYYMMDD o YYYY-MM-DD>

logger = get_logger(__name__)


class LogChecker():
    JSON_RESOURCES = servers.DICT_RESOURCES_JSON

    def __init__(self, ott_platforms, ott_site_country, operation, provider, logat=None):
        if logat:
            if "-" not in logat:
                logat = "{}-{}-{}".format(logat[0:4],logat[4:6],logat[6:8])

        for sv_location in self.JSON_RESOURCES:
            with open(self.JSON_RESOURCES[sv_location], 'r') as file:
                data = json.load(file)

            for item in data:
                platforms = item['platforms']
                for platform in platforms:
                    if ott_platforms == 'JustWatch':
                        if platform.get('Provider') == provider[0] and platform['Country'] == ott_site_country:
                            self.sendCommand(sv_location, ott_platforms, ott_site_country, operation, provider=provider[0], logat=logat)
                    else:
                        if platform['PlatformName'] == ott_platforms and platform['Country'] == ott_site_country:
                            self.sendCommand(sv_location, ott_platforms, ott_site_country, operation, provider=None, logat=logat)

    def sendCommand(self, sv_location, ott_platform, ott_country, operation, provider=None, logat=None):
        no_log = False
        try:
            username = settings.DEFAULT_USER_LOG
            port = settings.DEFAULT_PORT
            _connection = servers.Connection(server_name=sv_location)
            client = _connection.connect(username=username, port=port, use_paramiko=True)
            if ott_platform == 'JustWatch':
                ssh_stdin, ssh_stdout, ssh_stderr = client.exec_command('find log/{}_{}-{}*'.format(ott_country, ott_platform, provider))
            else:
                ssh_stdin, ssh_stdout, ssh_stderr = client.exec_command('find log/{}_{}*'.format(ott_country, ott_platform))

            for line in iter(ssh_stderr.readline, ""):
                if 'No such file or directory' in line:
                    no_log = True
            
            if not no_log:
                logs = []
                for line in iter(ssh_stdout.readline, ""):
                    if ott_platform == 'JustWatch':
                        logs.append(line.replace('log/{}_{}-{}_'.format(ott_country, ott_platform, provider),'').replace('.log','').strip())
                    else:
                        logs.append(line.replace('log/{}_{}_'.format(ott_country, ott_platform),'').replace('.log','').strip())

                if not logat:
                    created_at = logs[-1]
                else:
                    created_at = logat.replace("-","")

                if operation == 'logd':
                    try:
                        import os
                        sftp = client.open_sftp()
                        if not os.path.exists("log"):
                            os.mkdir("log")
                        elif os.path.isfile("log"):
                            os.remove("log")
                            os.mkdir("log")
                        del os
                        if ott_platform == 'JustWatch':
                            localpath = 'log/{}_{}-{}_{}.log'.format(ott_country, ott_platform, provider, created_at)
                            remotepath = 'log/{}_{}-{}_{}.log'.format(ott_country, ott_platform, provider, created_at)
                        else:
                            localpath = 'log/{}_{}_{}.log'.format(ott_country, ott_platform, created_at)
                            remotepath = 'log/{}_{}_{}.log'.format(ott_country, ott_platform, created_at)
                        
                        print("\x1b[1;32;40m *****COMIENZO DE DESCARGA***** \x1b[0m")
                        sftp.get(remotepath, localpath)
                        print("\x1b[1;32;40m *****FINAL DE DESCARGA***** \x1b[0m")
                    except FileNotFoundError:
                        no_log = True
                    finally:
                        sftp.close()
                else:
                    if ott_platform == 'JustWatch':
                        ssh_stdin, ssh_stdout, ssh_stderr = client.exec_command('tail -n 100 log/{}_{}-{}_{}.log'.format(ott_country, ott_platform, provider, created_at))
                    else:
                        ssh_stdin, ssh_stdout, ssh_stderr = client.exec_command('tail -n 100 log/{}_{}_{}.log'.format(ott_country, ott_platform, created_at))

                    print("\x1b[1;32;40m *****COMIENZO DE LOG***** \x1b[0m")

                    for line in iter(ssh_stdout.readline, ""):
                        print(line, end="")

                    if ott_platform == 'JustWatch':
                        print("\x1b[1;32;40m *****FIN DE LOG***** ({}_{}-{}_{} en {}) \x1b[0m".format(ott_country, ott_platform, provider, created_at, sv_location))
                    else:
                        print("\x1b[1;32;40m *****FIN DE LOG***** ({}_{}_{} en {}) \x1b[0m".format(ott_country, ott_platform, created_at, sv_location))

                    print("\x1b[1;32;40m *****OTROS LOGS***** \x1b[0m")

                    for log in logs:
                        if ott_platform == 'JustWatch':
                            print('{}_{}-{}_{}.log'.format(ott_country, ott_platform, provider, log))
                        else:    
                            print('{}_{}_{}.log'.format(ott_country, ott_platform, log))
        finally:
            client.close()

        return no_log
