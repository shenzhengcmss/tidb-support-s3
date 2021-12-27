# TiDB 冷热数据分层存储

## he3团队

队长：薛港 

队员：时丕显、沈政

## 项目介绍

TiDB冷热数据分层存储主要考虑的是热数据存放在TiKV上以及很少几率查询分析的冷数据（如银行、电商几年前或者十几年前的数据）存放到便宜通用的云存储S3上，同时使S3存储引擎支持TiDB相关计算下推（如谓词下推、聚合下推等），实现TiDB基于S3冷数据分析查询。

主要功能：

- 实现TiDB基于S3表支持所有的算子。
- 实现S3存储引擎支持算子下推。
- 实现TPCH所有SQL查询基于S3查询都能正确返回，同时性能下降可接受。
- 实现S3上的冷表与TiKV的热表支持任意方式关联查询。

## 背景&动机

TiDB是一款HTAP数据库，在云上销售产品最重要的因素之一就是成本，TiDB对于存量冷数据做AP查询主要使用Tiflash组件，Tiflash的数据又是从TiKV实时同步，对于PB级的事务要求不高存量冷数据，使用Tiflash与TiKV保存冷数据将消耗大量的计算与存储资源，不利于TiDB在云上销售，如果能使用一种更便宜更通用的存储介质实现冷数据查询，将大大的降低TiDB在云上的成本。
为了解决TiDB在云上冷数据的查询成本问题，我们提出了TiDB冷热数据分层存储解决方案，让TiKV，Tiflash解决热数据交易型与分析型查询，让通用便宜的云存储S3解决TiDB冷数据查询。

## 项目设计

###系统整体架构如下：

![1640594715100](C:\Users\SHENZH~1\AppData\Local\Temp\1640594715100.png)



## 详细设计

### 本提案的实现分为三层：

- TiDB外部表部分查询算子下推：

  实现TiDB冷数据S3外部表存储，同时修改TiDB外部表元数据使查询支持谓词下推（逻辑运算、比较运算、数值运算） 、 支持聚合函数下推、支持Limit算子下推等操作到虚拟层Virtual。

- TiDB下推算子转化s3相关查询：

  虚拟层Virtual对于TiDB下推的算子进行并发查询与汇总。

- S3计算引擎执行查询：

  利用S3支持sql查询能力，返回相关sql查询结果。

### 设计元数据表存储S3相关信息：

设计S3相关信息serverhost，serverobject存储在mysql database下。

insert into mysql.serverobject values("tests3","eos-wuxi-1.cmecloud.cn","accesskey
","secretkey","bucketname");

insert into mysql.serverhost values("eos-wuxi-1.cmecloud.cn","10.10.10.1:80");

### 实现冷数据DML & DDL相关操作:

支持TiDB支持建外部表存储在S3上，支持对s3表执行TRUNCATE TABLE、DROP TABLE、CREATE TABLE，同时支持数据库导入与导出：

//tidb支持创建s3外部表语法

create table tests3(id int, name varchar(20)) s3options tests3;
其中，s3options后面跟的tests3就是第一步添加到系统表mysql.serverobject中的name值

//tidb普通表

create table test(id int, name varchar(20));

//导入数据到S3用户表上：
insert into tests3 select * from test;

//S3数据导入普通表

insert into test select * from tests3;

支持tpch所有查询

