from configparser import ConfigParser

file = 'configuration.ini'
config = ConfigParser()
config.read(file)

print(config.sections())
print(list(config['es']))
