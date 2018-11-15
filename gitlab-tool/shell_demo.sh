## 操作示例
# gitlab-raw-url
python3 presto_execute.py \
        prepare.properties.url=http://gitlab.xx/raw/db.properties \
        sql.url.dir=http://gitlab.xx/raw/file \
        sql.file.names=test1.sql,test2.sql

# gitlab-tree-url(should be a dir)
python3 presto_execute.py \
        sql.url.dir=http://gitlab.xx/tree/file

# http-url/response should be a sql
python3 presto_execute.py \
        sql.urls=http://gitlab.xx/file/test1.sql,http://gitlab.xx/file/test2.sql
