
## Chapter 4. 스파크 SQL과 데이터 프레임: 내장 데이터 소스 소개

### 작성자: 이현준(6기 러너)

### 배울 내용 요약 

1. SparkSQL 특징
2. SQL 테이블과 뷰
3. 여러 파일 형식을 데이터 프레임으로 읽고 쓰기

<br>

### 1. SparkSQL 특징과 예제

- SparkSQL 특징
  - 다양한 구조화된 형식(예: JSON, Hive 테이블, Parquet, Avro, ORC, CSV)으로 데이터를 읽고 쓸 수 있다.
  - Tableau, Power BI, Talend와 같은 외부 비즈니스 인텔리전스(BI)부터 MySQL 및 PostgreSQL과 같은 RDBMS등 에서 JDBC/ODBC 커넥터를 사용하여 데이터를 쿼리할 수 있다.
  - Spark 애플리케이션의 데이터베이스에 테이블 또는 view로 저장된 정형 데이터와 상호 작용할 수 있는 프로그래밍 방식 인터페이스를 제공한다.

<br>

### 2. SQL 테이블과 뷰

- 스파크는 각 테이블과 해당 데이터에 관련된 정보인 스키마, 설명, 테이블명, 데이터베이스명, 칼럼명, 파티션, 실제 데이터의 물리적 위치 등의 메타데이터를 가지고 있다. 이 모든 정보는 중앙 메타스토어에 저장된다.
- 기본적으로는 /user/hive/warehouse에 모든 메타 데이터를 유지한다.

- 관리형 테이블과 비관리형 테이블의 특징
||관리형 테이블|비관리형 테이블|
|---|---|---|---|
|관리범위|메타데이터와 파일 저장소의 데이터 모두 관리|오직 메타데이터만 관리|
|저장소|로컬 파일 시스템, HDFS, Amazon S3, Azure Blob 같은 객체 저장소|카산드라와 같은 외부 데이터 소스에서 데이터를 직접 관리|
|사용|DROP TABLE <테이블명>과 같은 SQL 명령은 메타데이터와 실제 데이터를 모두 삭제|동일 명령이 실제 데이터는 그대로 두고 메타데이터만 삭제|

- 뷰

뷰는 전역(해당 클러스터의 모든 SparkSession)또는 세션 범위(단일 SparkSession)일 수 있으며, 테이블과 달리 데이터를 소유하지 않기 때문에 일시적으로 스파크애플리케이션이 종료되면 사라짐.

<br>

### 3. 여러 파일 형식을 데이터 프레임으로 읽고 쓰기

- DataFrameReader
    데이터 소스에서 데이터 프레임으로 데이터를 읽음
    ```shell
    # 정의된 형식 및 권장되는 사용 패턴
    DataFrameReader.format(args).option(“key”,”value”).schema(args).load()
    ```
    `SparkSession.read` 는 정적 데이터 소스에서 사용
    `SparkSession.readStream` 는 스트리밍 소스에서 사용

- DataFrameWriter
    지정된 내장 데이터 소스에 데이터를 저장하거나 쓰는 작업을 수행
    ```shell
    # 정의된 형식 및 권장되는 사용 패턴
    DataFrameWriter.format(args)
	.option(args)
	.bucketBy(args)
	.partitionBy(args)
	.save(path)

    DataFrameWriter.format(args).option(args).bucketBy(args).partitionBy(args).save(path)
    ```
    'DataFrame.write'
    'DataFrame.writeStream' 
    으로 Reader와 같이 사용한다.

- 위와 비슷한 방법으로 테이블, 파케이, JDBC, CSV, AVRO, ORC 등 파일을 읽고 쓸 수 있다.

<br>

##### * 코드 주의 사항
##### SQL에서 작업해야함에도 불구하고 spark.sql("SQL Query")를 사용해서 진행함

<br>