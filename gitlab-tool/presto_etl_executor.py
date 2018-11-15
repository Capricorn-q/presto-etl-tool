import requests
import argparse
import datetime
from .db_util import *

SQL_FILE_SUFFIX = '.sql'


# 返回gitlab-raw目录下指定文件名的sql语句
def get_sqls_from_gitlab_rawdir_and_filenames(url_dir, file_names):
    if url_dir and file_names:
        sqls = []
        for file in file_names:
            if SQL_FILE_SUFFIX in file:
                url = url_dir + '/' + file
                sqls = sqls + get_sqls_from_url(url)
        return sqls


# 根据url获取sql语句
def get_sqls_from_url(url):
    if url is None:
        return []
    # print('request the url : ' + url)
    resp = requests.get(url)
    if resp.status_code == 200:
        text = resp.text
        return text.strip().split(';')
    else:
        raise Exception(str(resp.status_code) + ',' + resp.reason + ': ' + url)
        return []


# 返回gitlab-tree目录下所有sql文件名称，暂无使用
def get_file_names_from_gitlab_treedir(url_dir):
    # print('request the dir : ', url_dir)
    resp = requests.get(url_dir)
    if resp.status_code == 200:
        file_names = ''
        for line in resp.text.split('<a title="'):
            if '.sql" href="' in line:
                file_names = file_names + line.split('" href="')[0] + ','
        file_names = file_names[:-1]
        return file_names
    else:
        raise Exception(str(resp.status_code) + ',' + resp.reason + ': ' + url_dir)


def get_properties_kv(result_list):
    ph = {}
    for rs in result_list:
        ph[rs.split('=')[0]] = rs.split('=')[1]
    return ph


def replace_properties(args, sqls):
    if args.prepare_properties_url:
        resp = requests.get(args.prepare_properties_url)
        if resp.status_code == 200:
            # print(resp)
            rsqls = []
            text = resp.text
            ps = text.split('\n')
            kvs = get_properties_kv(ps)
            for sql in sqls:
                if sql:
                    for key in kvs.keys():
                        sql = sql.replace('${' + key + '}', str(kvs[key]))
                    rsqls.append(sql)
            return rsqls
    else:
        return sqls


def get_execute_psqls(args):
    sqls = []
    if args.psql_urls:
        for url in args.psql_urls:
            sqls = sqls + get_sqls_from_url(url)
    if args.sql_dir:
        if args.psql_file_names:
            sqls = sqls + get_sqls_from_gitlab_rawdir_and_filenames(args.sql_dir.replace('tree', 'raw'),
                                                                    args.psql_file_names)
    return sqls


def get_execute_sqls(args):
    sqls = []
    if args.sql_urls:
        for url in args.sql_urls:
            sqls = sqls + get_sqls_from_url(url)
    if args.sql_dir:
        if args.sql_file_names:
            sqls = sqls + get_sqls_from_gitlab_rawdir_and_filenames(args.sql_dir.replace('tree', 'raw'),
                                                                    args.sql_file_names)
        else:
            sqls = sqls + get_sqls_from_gitlab_rawdir_and_filenames(args.sql_dir.replace('tree', 'raw'),
                                                                    get_file_names_from_gitlab_treedir(
                                                                        args.sql_dir).split(','))
    return sqls


def init_args():
    parser = argparse.ArgumentParser(prog='presto_etl_executor.py', description=
    """
            python3 presto_etl_executor.py 
                    -p http://gitlab.xx/big-data/xx/raw/master/presto-exe/test/db.properties 
                    -d http://gitlab.xx/big-data/xx/raw/master/presto-exe/test 
                    -f test.sql test-pro.sql test-pl.sql
                    -pf pl.sql 
                    -k schema-table

            ~~ have fun ~~
            """
                                     )

    # properties config
    parser.add_argument('-p', '--prepare_properties_url', type=str, action='store', dest='prepare_properties_url',
                        help=''' 根据远程properties文件加载配置，进行sql的填充 
                            【e.g  原sql文件 : SELECT ${dev}; | 
                                  propreties文件 : dev=abcd | 
                                  填充后sql文件 : SELECT abcd; 】''')

    # gitlab params
    parser.add_argument('-d', '--sql_dir', type=str, action='store', dest='sql_dir',
                        help=' 远程gitlab对应sql目录的url，【注意：raw或tree地址】 ')
    parser.add_argument('-f', '--sql_file_names', type=str, action='store', nargs='*', dest='sql_file_names',
                        help=' 远程gitlab对应sql目录下需要执行的sql文件 ')
    parser.add_argument('-pf', '--psql_file_names', type=str, action='store', nargs='*', dest='psql_file_names',
                        help=' 远程gitlab对应占位符sql目录下需要执行的sql文件 ')

    # not gitlab params
    parser.add_argument('-urls', '--sql_urls', type=str, action='store', nargs='*', dest='sql_urls',
                        help=' 远程sql对应的url，可以是非gitlab的url ')
    parser.add_argument('-purls', '--psql_urls', type=str, action='store', nargs='*', dest='psql_urls',
                        help=' 远程占位符sql对应的url，可以是非gitlab的url ')

    # lock
    parser.add_argument('-k', '--lock_key', type=str, action='store', dest='lock_key',
                        help='任务锁，格式：schema.table')

    args = parser.parse_args()
    return args


def store_placeholder(psqls):
    pl = {}
    for psql in psqls:
        r = exec_presto_sql(psql)
        if r.__len__() > 0 and r.__len__() % 2 == 0:
            i = 0
            while i < r.__len__():
                pl[r[i]] = r[i + 1]
                i += 2
    return pl


def replace_placeholder(pl, sqls):
    rsqls = []
    for sql in sqls:
        if sql:
            for key in pl.keys():
                sql = sql.replace('${' + key + '}', str(pl[key]))
            rsqls.append(sql)
    return rsqls


def unlock(args, exflag):
    if args.lock_key:
        if len(args.lock_key.split('-')) != 2:
            raise Exception('lockKey-format : schema-table, please check')
        schema, table = args.lock_key.split('-')
        date = datetime.datetime.now()
        detester = date.strftime('%Y%m%d')
        update_sql = ''
        if exflag:
            update_sql = """UPDATE etl_lock SET is_lock={} WHERE schema_name='{}' AND table_name='{}'""". \
                format(0, schema, table)
        else:
            update_sql = """UPDATE etl_lock SET date_str='{}', is_lock={} WHERE schema_name='{}' AND table_name='{}'""". \
                format(detester, 0, schema, table)
        exec_mysql_sql(update_sql)


def lock(args):
    if args.lock_key:
        if len(args.lock_key.split('-')) != 2:
            raise Exception('lockKey-format : schema-table, please check')
        schema, table = args.lock_key.split('-')
        date = datetime.datetime.now()
        detester = date.strftime('%Y%m%d')
        select_sql = """SELECT * FROM etl_lock WHERE schema_name='{}' AND table_name='{}'""".format(schema, table)
        insert_sql = """INSERT INTO etl_lock (schema_name,table_name,date_str,is_lock) VALUES ('{}','{}','{}','{}')""".format(
            schema, table, 'new', 1)
        lock_sql = """UPDATE etl_lock SET is_lock={} WHERE schema_name='{}' AND table_name='{}'""". \
            format(1, schema, table)

        result = exec_mysql_sql(select_sql)
        if not result:
            exec_mysql_sql(insert_sql)
            return True
        else:
            if result['is_lock'] == 1:
                raise Exception('schema ' + schema + ', table ' + table + ' is lock')
            elif result['date_str'] == detester:
                return False
            else:
                exec_mysql_sql(lock_sql)
                return True
    return True


def exec():
    args = init_args()

    # 在azkaban等调度工具中设置重试间隔与重试次数
    flag = False
    if lock(args):
        try:
            exec_presto_sqls(
                replace_placeholder(
                    store_placeholder(
                        replace_properties(args, get_execute_psqls(args))),
                    replace_properties(args, get_execute_sqls(args))))
        except Exception as e:
            flag = True
            raise e
        finally:
            unlock(args, flag)
    else:
        print('============this job is done before============')


if __name__ == '__main__':
    exec()
