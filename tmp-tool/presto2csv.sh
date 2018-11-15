#!/bin/bash
# 用于临时导出文件，superSet可为替代方案，但superSet有大小限制
./presto-cli --server localhost:10300 --user test --catalog jmx -f test.sql --output-format CSV >> demo.csv
./presto-cli --server localhost:10300 --user test --catalog jmx --execute 'your sql' --output-format CSV >> demo.csv
# iconv -f UTF8 -t GB18030 demo.csv data.csv ## 解决中文问题