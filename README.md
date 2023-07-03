#  contributors  
 
# master
 - name : liutao 
 - email: liutaobigdata@163.com 
# developer

 -   name : fanjiahui 
 -   email:fanjiahui2017@163.com
 -  name : wangyatao
 -  email: 15210648300@163.com

# flink sql ddl 

```
CREATE TEMPORARY TABLE rabbitmq_source (
  colunn1 STRING,
  colunn2 STRING,
  colunn3 STRING
) WITH (
  'connector' = 'rabbitmq',
  'queue' = '',
  'hosts' = '',
  'port' = '',
  'virtual-host' = '',
  'username' = '',
  'password' = '',
  'exchange-name'='',
  'format'='json'
);
```
