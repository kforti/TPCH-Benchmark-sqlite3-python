import sqlite3

import sqlparse
from sqlparse.tokens import Token

from queries import *


con = sqlite3.connect('data/tpch.db')
cur = con.cursor()


class DatabaseMetaData:
    def __init__(self):
        self.entity_attribute_map = {}
        self.attribute_entity_map = {}
        self.attribute_map = {}

    @classmethod
    def from_cursor(cls, cur):
        db_meta_data = DatabaseMetaData()
        db_meta_data.get_meta_data(cur)
        return db_meta_data

    def get_meta_data(self, cur):
        meta = self.describe_database(cur)
        for entity, attributes in meta.items():
            self.entity_attribute_map[entity] = attributes
            for attribute in attributes:
                self.attribute_entity_map[attribute[1]] = entity
                self.attribute_map[attribute[1]] = attribute
        return None

    def describe_database(self, cur):
        database_meta = {}
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        entities = [i[0] for i in cur.fetchall()]
        for entity in entities:
            cur.execute(f"PRAGMA table_info('{entity}')")
            attributes = [i for i in cur.fetchall()]
            database_meta[entity] = attributes
        return database_meta

    def get_entity_attributes(self, entity):
        if entity not in self.entity_attribute_map:
            return None
        return self.entity_attribute_map[entity]

    def get_attribute_entity(self, attribute):
        if attribute not in self.attribute_entity_map:
            return None
        return self.attribute_entity_map[attribute]

    def get_full_attribute(self, attribute):
        if attribute not in self.attribute_map:
            return None
        return self.attribute_map[attribute]

    @property
    def entities(self):
        return [i for i in self.entity_attribute_map]

    @property
    def attributes(self):
        return [v for v in self.entity_attribute_map.values()]

    @property
    def attribute_names(self):
        return [i for i in self.attribute_map]



def count_sub_queries(query):
    parsed = sqlparse.parse(query.render())[0]
    keywords = []
    for s in parsed.tokens:
        if s.ttype == Token.Keyword or s.ttype == Token.Keyword.DML:
            # print(s.ttype, s.value)
            keywords.append(s.value)
        elif s.ttype is None:
            keywords.extend([i.value for i in s.flatten() if i.ttype == Token.Keyword or Token.Keyword.DML])

    count = keywords.count('SELECT')
    return count

db_meta = DatabaseMetaData.from_cursor(cur)

for name, query in queries.items():
    print(name)
    sq_count = count_sub_queries(query)
    print(sq_count)

