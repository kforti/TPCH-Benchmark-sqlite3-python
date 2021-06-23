from dataclasses import dataclass

import sqlparse
from sqlparse.tokens import Token

from queries import *


Entities = ['CUSTOMER', 'ORDERS', 'REGION',
            'LINEITEM', 'PART', 'SUPPLIER',
            'NATION', 'PARTSUPP',
            ]

query_features = {}

SLOW_QUERIES = ['query_17', 'query_20', 'query_22']

@dataclass
class QueryFeatures:
    entity_counts: dict
    keyword_counts: dict
    bin: str

    def update_keyword_counts(self, keyword_counts):
        for k in keyword_counts:
            if k in self.keyword_counts:
                self.keyword_counts[k] += 1
            elif k not in self.keyword_counts:
                self.keyword_counts[k] = 1

    def update_entity_counts(self, entity_counts):
        for k in entity_counts:
            if k in self.entity_counts:
                self.entity_counts[k] += 1
            elif k not in self.entity_counts:
                self.entity_counts[k] = 1


for name, query in queries.items():
    if name in SLOW_QUERIES:
        bin = 'slow'
    else:
        bin = 'fast'
    features = QueryFeatures({}, {}, bin)

    query_string = query.template

    parsed = sqlparse.parse(query_string)[0]

    for i in parsed.tokens:
        entities = [s.value for s in i.flatten() if s.value in Entities]
        features.update_entity_counts(entities)
        keywords = [s.value for s in i.flatten() if s.ttype == Token.Keyword]
        features.update_keyword_counts(keywords)

    query_features[name] = features


for k, v in query_features.items():
    print(k, v. bin, v.entity_counts, v.keyword_counts)
