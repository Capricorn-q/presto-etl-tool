import os.path
import pymysql
import prestodb

__presto_config_file = os.path.join(os.path.dirname(__file__), 'etl-presto.config')
__mysql_config_file = os.path.join(os.path.dirname(__file__), 'etl-mysql.config')


class Properties(object):
    def __init__(self, file_name):
        self.fileName = file_name
        self.properties = {}
        with open(self.fileName, 'r') as pro_file:
            for line in pro_file.readlines():
                line = line.strip().replace('\n', '')
                if line.find("#") != -1:
                    line = line[0:line.find('#')]
                if line.find('=') > 0:
                    strs = line.split('=')
                    strs[1] = line[len(strs[0]) + 1:]
                    self.properties[strs[0].strip()] = strs[1].strip()

    def get_properties(self):
        return self.properties

    def to_string(self):
        print(self.properties)


def get_presto_con():
    config = Properties(__presto_config_file).get_properties()
    return prestodb.dbapi.connect(
        host=config['presto.host'],
        port=config['presto.port'],
        user=config['presto.user'],
        catalog=config['presto.catalog']
    )


def get_mysql_con():
    config = Properties(__mysql_config_file).get_properties()
    return pymysql.connect(
        host=config['mysql.host'],
        port=int(config['mysql.port']),
        user=config['mysql.user'],
        password=config['mysql.pass'],
        db='etl',
        charset='utf8',
        cursorclass=pymysql.cursors.DictCursor
    )


# 批量一次执行presto-sql
def exec_presto_sqls(sqls):
    with get_presto_con() as con:
        cur = con.cursor()
        if sqls.__len__() >= 1:
            rs = []
            for sql in sqls:
                if sql and sql.strip() != '':
                    print("执行SQL：", sql)
                    cur.execute(sql.strip())
                    r = cur.fetchone()
                    print(r)
                    # rs = rs + cur.fetchall()  # 返回sql文件中所有sql
                    # print(rs)
            return rs
        else:
            return None


def exec_presto_sql(sql):
    with get_presto_con() as con:
        cur = con.cursor()
        if sql and sql.strip() != '':
            print("执行sql：", sql)
            cur.execute(sql.strip())
            r = cur.fetchone()
            print("结果：", r)
        return r


def exec_mysql_sql(sql):
    if sql:
        con = get_mysql_con()
        try:
            with con.cursor() as cur:
                print("执行sql：", sql)
                cur.execute(sql.strip())
                con.commit()
                r = cur.fetchone()
                print("结果：", r)
                return r
        except Exception as e:
            con.rollback()
        finally:
            con.close()
