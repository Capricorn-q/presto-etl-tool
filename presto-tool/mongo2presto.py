from pymongo import MongoClient
import prestodb
import argparse

"""
    mongo 单表数据到 presto(hive) 临时解决方案
    情况特殊：由于mongo版本低于2.6，数据量少，文档结构单一
    稳妥起见请使用开源etl工具
    python3 mongo2presto.py -m 127.0.0.1:27017/test/user -p 127.0.0.1:10300/test/test_hive/test/user
"""


def init_args():
    parser = argparse.ArgumentParser(prog='mongo2presto.py', description=' This is a simple mongo 2 presto etl script ')
    parser.add_argument('-m', '--mongo', type=str, action='store', dest='mongo', required=True,
                        help=''' ip:port/db/collection(mongodb-config) ''')
    parser.add_argument('-p', '--presto', type=str, action='store', dest='presto', required=True,
                        help=''' ip:port/user/catalog/schema/table(presto-config)''')
    args = parser.parse_args()
    return args


def exec(args):
    presto_args = args.presto
    host = presto_args.split(":")[0]
    info = presto_args.split(":")[1]
    port = info.split("/")[0]
    user = info.split("/")[1]
    catalog = info.split("/")[2]
    schema = info.split("/")[3]
    table = info.split("/")[4]
    col_names = []
    col_types = []
    with prestodb.dbapi.connect(
            host=host, port=port, user=user, catalog=catalog, schema=schema
    ) as con:
        cur = con.cursor()
        cur.execute('desc ' + table)
        r = cur.fetchall()
        for arr in r:
            col_names.append(arr[0])
            col_types.append(arr[1])
        print('presto col names : ', col_names)
        print('presto col types : ', col_types)

    mongo_args = args.mongo
    mongo_url = mongo_args.split("/")[0]
    client = MongoClient("mongodb://" + mongo_url + "/")
    db_name = mongo_args.split("/")[1]
    collection_name = mongo_args.split("/")[2]
    db = client[db_name]
    collection = db[collection_name]
    cursor = collection.find()
    # count = cursor.count()
    sql = ''
    if col_names:
        sql = 'insert into ' + table + ' ('
        for col in range(col_names.__len__() - 1):
            sql += col_names[col] + ','
        sql += col_names[col_names.__len__() - 1] + ' ) values '

    is_first = 0
    insert_sql = ''
    for doc in cursor:
        if is_first == 0:
            insert_sql += value(col_names, col_types, doc, True)
        else:
            insert_sql += value(col_names, col_types, doc, False)
        is_first += 1

    if insert_sql:
        print(sql + insert_sql)
        with prestodb.dbapi.connect(
                host=host, port=port, user=user, catalog=catalog, schema=schema
        ) as con:
            cur = con.cursor()
            cur.execute(sql + insert_sql)
            r = cur.fetchone()
            print(r)
    else:
        print('do nothing ... ')


def value(names, types, doc, is_first):
    sql = ''
    if is_first:
        sql += "("
    else:
        sql += ",("

    # 检查是否无匹配
    flag = True
    for name in names:
        if name in doc.keys():
            l = names.__len__() - 1
            for i in range(l):
                sql += get(doc, names[i], types[i]) + ','
            sql += get(doc, names[l], types[l]) + ')'
            flag = False
            break

    if flag:
        return ''
    else:
        return sql


# TODO deal with more type，处理更多数据类型
def get(doc, key, type):
    if key in doc.keys():
        if type != 'varchar':
            return doc[key]
        else:
            return "'" + doc[key] + "'"
    else:
        if type != 'varchar':
            return '0'
        else:
            return 'NULL'


if __name__ == '__main__':
    args = init_args()
    exec(args)
