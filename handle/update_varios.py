import os
import argparse
import sys



if __name__ == '__main__':

    parser =  argparse.ArgumentParser()
    parser.add_argument('dbs', help = 'Nombre del archivo con la lista de PlatformCode a ingresar',type=str)
    parser.add_argument('--at',help ='Nombre del archivo con la lista de CreatedAt a ingresar', type=str,default='last')
    parser.add_argument('-s', '--server', type=int, required=False, default=1)
    parser.add_argument('-u', '--upload', action='store_const', const=True, default=False)
    parser.add_argument('--bypass', action='store_const', const=True, default=False)
    parser.add_argument('--noepisodes', action='store_const', const=True, default=False)
    args = parser.parse_args()
    
    # comando = " Upload({platform_code},{created_at},testing={upload},bypass={bypass},has_episodes={has_episodes},server={server})".format(upload=not args.upload,bypass=args.bypass,has_episodes=not args.noepisodes,server=args.server)
    comando = "python updates/upload.py --upload --platformcode {platform_code} --createdat {created_at} --bypass"
    with open(args.countries,'r') as file:
        news_platform_code = file.readlines()
    with open(args.at,'r') as file:
        created_ats  = file.readlines()
    
    if len(news_platform_code) != len(created_ats):
        print("Tengo que tener la misma cantidad de platformcode que createdAt")
        sys.exit(1)
    
    for platform_code,created_at in zip(news_platform_code,created_ats):
        os.system(comando.format(platform_code=platform_code,created_at=created_at))
