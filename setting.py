import argparse
import configparser

def config_generator(args):
    config = configparser.ConfigParser()

    config['owl'] = {}
    config['owl']['path'] = args.owl_path

    config['elasticsearch'] = {}
    config['elasticsearch']['ip'] = args.server_ip
    config['elasticsearch']['name'] = args.index_name

    with open('config.ini', 'w', encoding='utf-8') as configfile:
        config.write(configfile)


def config_read(file_path = ''):
    config = configparser.ConfigParser()    
    config.read(file_path+'config.ini', encoding='utf-8') 
    return config

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--owl_path', type=str, default='', help='ontology data path (default: "")')
    parser.add_argument('--server_ip', type=str, default='', help='elasticsearch server ip (default: "")')
    parser.add_argument('--index_name', type=str, default='', help='elasticsearcg index name (default: "")')

    args = parser.parse_args()

    config_generator(args)