from elasticsearch_dsl import Document, Text, InnerDoc, DateRange, Nested, RangeField, Double, Range, Keyword
from elasticsearch_dsl.connections import connections
from datetime import datetime

# Define a default Elasticsearch client
connections.create_connection(hosts=['localhost'])


class DoubleRange(RangeField):
    """
    Custom defined as the one in the python package is bugged
    """
    name = 'double_range'
    _core_field = Double()


class Block(InnerDoc):
    file_name = Keyword()
    locations = Keyword(multi=True)


class PartSpec(InnerDoc):
    key = Keyword()
    # Formats: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html
    date_range = DateRange(format="yyyy-MM-dd HH:mm:ss.SSS||yyy-MM-dd'T'HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss"
                                  "||yyy-MM-dd'T'HH:mm:ss||epoch_millis||epoch_second")
    geo_range = DoubleRange()


class Partition(Document):
    specs = Nested(PartSpec, multi=True)
    blocks = Nested(Block, multi=True)

    class Index:
        name = 'partition'


Partition.init()

# tested_partition = Partition()
# tested_partition.file_name = "myhdf5.hdf5"
# long_spec = PartSpec()
# long_spec.key = "Longitude"
# long_spec.date_range = Range(
#     gte=datetime(2018, 11, 17, 9, 0, 0),
#     lt=datetime(2018, 11, 17, 10, 0, 0)
# )
# tested_partition.specs.append(long_spec)
# tested_partition.save()

# Mapping
# {
#     "partition" : {
#         "mappings" : {
#             "properties" : {
#                 "blocks" : {
#                     "type" : "nested",
#                     "properties" : {
#                         "file_name" : {
#                             "type" : "keyword"
#                         },
#                         "locations" : {
#                             "type" : "keyword"
#                         }
#                     }
#                 },
#                 "specs" : {
#                     "type" : "nested",
#                     "properties" : {
#                         "date_range" : {
#                             "type" : "date_range",
#                             "format" : "yyyy-MM-dd HH:mm:ss.SSS||yyy-MM-dd'T'HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss||yyy-MM-dd'T'HH:mm:ss||epoch_millis||epoch_second"
#                         },
#                         "geo_range" : {
#                             "type" : "double_range"
#                         },
#                         "key" : {
#                             "type" : "keyword"
#                         }
#                     }
#                 }
#             }
#         }
#     }
# }

# TWO SEARCHES
# GET partition/_search
# {
#     "query":{
#         "bool":{
#             "must":[
#                 {
#                     "nested":{
#                         "path":"specs",
#                         "query":{
#                             "bool":{
#                                 "must":[
#                                     {
#                                         "term":{
#                                             "specs.key":"lat"
#                                         }
#                                     },
#                                     {
#                                         "range":{
#                                             "specs.geo_range":{
#                                                 "gte":10
#                                             }
#                                         }
#                                     }
#                                 ]
#                             }
#                         }
#                     }
#                 },
#                 {
#                     "nested":{
#                         "path":"specs",
#                         "query":{
#                             "bool":{
#                                 "must":[
#                                     {
#                                         "term":{
#                                             "specs.key":"timestamp"
#                                         }
#                                     },
#                                     {
#                                         "range":{
#                                             "specs.date_range":{
#                                                 "gte":0
#                                             }
#                                         }
#                                     }
#                                 ]
#                             }
#                         }
#                     }
#                 }
#             ]
#         }
#     }
# }
