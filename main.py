from datetime import date, datetime

from dateutil.relativedelta import relativedelta
from elasticsearch import Elasticsearch
from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch import RequestsHttpConnection

from elasticsearch.connection import create_ssl_context

from configparser import ConfigParser
from configparser import ConfigParser
import ssl ,socket
import urllib3
import certifi


def del_index(months):
    past_date = date.today() - relativedelta(months=months)
    print('Months back date: %s wrt user inputs of %s months' % (past_date, months))
    past_year, last_week, dayofweek = past_date.isocalendar()
    print("Week # " + str(last_week) + " of year " + str(past_year))

    print("Testing Statistics indices")
    delete_week_index(last_week, "wazuh-statistics-*", past_year)

    print("Testing Monitoring  indices")
    delete_week_index(last_week, "wazuh-monitoring-*", past_year)

    print("Testing Alerts indices")
    delete_day_index(past_date, "wazuh-alerts-*")

    print("Testing Audit log indices")
    delete_day_index(past_date, "security-auditlog-*")


def delete_week_index(week_before_to_delete, index_name, year_before_to_delete):
    response_indexes_names = es.indices.get_alias(index_name).keys()
    for response_index in response_indexes_names:
        index_week = int(str(response_index).split(".")[1].split("w")[0])
        index_year = int(str(response_index).split(".")[0].split("-")[2])
        if (index_week < week_before_to_delete or index_year < year_before_to_delete) or ():
            print('Deleting Index %s' % response_index)
            response_indexes_names = es.indices.delete(index=response_index, ignore=[400, 404])
            print(response_indexes_names)


def delete_day_index(date_before_to_delete, index_name):
    response_indexes_names = es.indices.get_alias(index_name).keys()
    for response_index in response_indexes_names:
        if 'security-auditlog' in response_index:
            index_date_str = str(response_index).split("-")[2]
        else:
            index_date_str = str(response_index).split("-")[3]
        if 'sample' not in index_date_str:
            index_date = datetime.strptime(index_date_str, "%Y.%m.%d")
            if (date_before_to_delete - index_date.date()).days > 0:
                print('Deleting Index %s' % response_index)
                response_indexes_names = es.indices.delete(index=response_index, ignore=[400, 404])
                print(response_indexes_names)


if __name__ == '__main__':
    file = 'configuration.ini'
    config = ConfigParser()
    config.read(file)

    es = Elasticsearch(hosts=config['es']['hosts'], verify_certs=False,
                       timeout=int(config['es']['timeout']),
                       max_retries=int(config['es']['max_retries']),
                       retry_on_timeout=bool(config['es']['retry_on_timeout']), connection_class=RequestsHttpConnection,
                       use_ssl=True)
    del_index(int(config['es']['month']))
