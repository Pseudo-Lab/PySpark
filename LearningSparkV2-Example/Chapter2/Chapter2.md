## Chapter 2. 스파크 개요 및 다운로드 하는 방법
- 작성자: 이성규(qkyu20@gmail.com)
<br/>

### 2단계: 스칼라 혹은 파이스파크 셸 사용

- 대화형 셸로 동작하여 네 가지 인터프리터들(pyspark, spark-shell, spark-sql, sparkR)로 일회성 데이터 분석이 가능
- 이 셸들은 클러스터에 연결하고 분산 데이터를 스파크 워커 노드의 메모리에 로드할 수 있도록 확장되어옴

- 파이스파크를 실행하려면 cd로 bin 디렉터리로 가서 pyspark를 입력해 셸을 띄움
- PyPI로 실행했다면 아무데서나 pyspark를 입력

![image](https://github.com/Pseudo-Lab/PySpark/assets/91249960/cddc2245-c0de-4267-8e6d-2cc3b784f4ab)
<br/>

### 로컬 머신 사용하기

- 스파크를 로컬 모드로 사용해서 예제들을 풀어갈 예정
- 스파크 연산들은 작업으로 표현, 작업들은 태스크라고 불리는 저수준 RDD 바이트 코드로 변환되며 실행을 위해 스파크의 이그제큐터들에 분산됨
- 데이터 프레임을 써서 텍스트 파일을 읽어서 보여주고 파일의 줄 수를 세는 예제는 다음과 같이 할 수 있음
- API 사용의 예이며 show(10, false) 연산을 통해 문자열을 자르지 않고 첫 번째 열 줄만 보여주게 됨
<img width="720" alt="Chapter_2" src="https://github.com/Pseudo-Lab/PySpark/assets/91249960/27abc3e6-1376-40bf-9253-7c5229eda8ca">

- 셸은 Ctrl + D를 통해 빠져나갈 수 있음
- 이 예제에서는 RDD를 쓰지 않고 상위 수준 정형화 API를 사용했다는 점에 주목하고 책에서는 이런것에 집중 할 것
<br/>

### 3단계: 스파크 애플리케이션 개념의 이해

- 실행해본 예제 코드 하부에서 일어나는 일에 대해 이해하기 위해서는 스파크 애플리케이션의 핵심 개념들과 코드가 어떻게 스파크 실행기에서 태스크로 변환되고 실행되는지 익숙해져야 하며 그 시작은 용어 정리

- 애플리케이션
    - API를 써써 스파크 위에서 돌아가는 사용자 프로그램, 드라이버 프로그램과 클러스터의 실행기로 구성
- SparkSession
    - 스파크 코어 기능들과 상호 작용 제공, 그 API로 프로그래밍 할 수 있게 해주는 객체
- 잡(job)
    - 스파크 액션(save(), collect())에 대한 응답으로 생성되는 여러 태스크로 이루어진 병렬 연산
- 스테이지(stage)
    - 각 잡은 스테이지라고 불리는 서로 의존성을 가지는 다수의 태스크 모음으로 나뉨
- 태스크(task)
    - 스파크 이그제큐터로 보내지는 작업 실행의 가장 기본적인 단위
- 트랜스포메이션
    - 이미 불변성의 특징을 가진 원본 데이터를 수정하지 않고 하나 의 스파크 데이터 프레임을 새로운 데이터 프레임으로 변형
    - ex) select나 filter같은 연산은 원본 데이터프레임을 수정하지 않으며 대신 새로운 데이터 프레임으로 연산 결과를 만들어 되돌려줌
- 지연평가
    - 액션이 실행되는 시점이나 데이터에 실제 접근하는 시점까지 실제 행위를 미루는 스파크의 전략
    - 하나의 액션은 모든 기록된 트랜스포메이션의 지연 연산을 발동시킴.
    - 쿼리 최적화를 가능하게 하는 반면, 리니지와 데이터 불변성은 장애에 대한 데이터 내구성을 제공함
- 액션
    - 쿼리 실행 계획의 일부로서 기록된 모든 트랜스포메이션들의 실행을 시작하게 함
<br/>

### **좁은/넓은 트랜스포메이션**

- **트랜스포메이션**은 지연 연산 종류이기 때문에 연산 쿼리를 분석하고 최적화하는 점에서 장점이 있음
    - 여기서 말하는 **최적화**는 연산들의 조인이나 파이프라이닝이 될 수 있고, 셔플이나 클러스터 데이터 교환이 필요한지 파악해 가장 간결한 실행 계획으로 데이터 연산 흐름을 만드는 것
- 트랜스포메이션을 아래 두가지 유형으로 분류할 수 있음
    - **좁은 의존성**: 하나의 출력 파티션에만 영향을 미치는 연산으로 파이션 간의 데이터 교환은 없으며, 스파크는 파이프라이닝을 자동으로 수행 ex. where(), filter(), contains()
    - **넓은 의존성**: 하나의 입력 파티션이 여러 출력 파티션에 영향을 미치는 연산으로, 스파크는 클러스터에서 파티션을 교환하는 셔플을 수행하고 결과는 디스크에 저장 ex. groupBy(), orderBy()
<br/>

### **스파크 UI**

- 스파크는 스파크 애플리케이션을 살펴볼 수 있는 그래픽 유저 인터페이스(GUI)를 제공
- 드라이버 노드의 4040 포트를 사용
- **스파크 UI 내용**
    - 스파크 잡의 상태(스케줄러의 스테이지와 태스크 목록)
    - 환경 설정 및 클러스터 상태 등의 정보
    - RDD 크기와 메모리 사용 정보
    - 실행 중인 이그제큐터 정보
    - 모든 스파크 SQL 쿼리

- 로컬 모드에서 웹 브라우저를 통해 http://localhost:4040 으로 접속하면, 아래와 같은 화면에서 잡, 스테이지, 태스크들이 어떻게 바뀌어 진행되는지, 연산 내에서 여러 개의 태스크가 병렬 실행될 때 스테이지의 자세한 상황 등등 확인할 수 있음
- 스파크 UI가 스파크 내부 작업에 대해 디버깅 및 검사 도구의 역할을 한다는 것을 이해하고  더 자세한 내용은 7장에서 다룸
![Chapter_3](https://github.com/Pseudo-Lab/PySpark/assets/91249960/00e8bb42-8144-4358-89f9-9d9c42947f4e)
<br/>

### 첫 번째 단독 어플리케이션

- 스파크 배포판에는 예제 애플리케이션이 존재한다.
    - 설치 디렉터리의 `examples` 디렉터리 참고
- 예제 실행하는 법 : `bin/run-example <클래스> [인자]`
    
    ```bash
    ./bin/run-example JavaWordCount README.md
    ```
    
    <img width="793" alt="Chapter_4" src="https://github.com/Pseudo-Lab/PySpark/assets/91249960/f960bb24-a0d9-428a-b7c8-6ccc7c5110d5">
    
    - 위 예제는 파일의 단어를 세는 애플리케이션
    - 파일이 큰 경우 데이터를 작은 조각으로 나누어 클러스터에 분산 → 스파크 프로그램이 각 파티션의 단어를 세는 태스크를 분산 처리 → 단어 개수 결과 집계
<br/>

### 쿠키 몬스터를 위한 M&M 세기

> 예제 : M&M을 사용하여 쿠키를 굽는 것을 좋아하는 데이터 과학자가 있으며, 그녀는 미국의 여러 주(state) 출신 학생들에게 상으로 쿠키들을 자주 주곤 한다. 그녀는 서로 다른 주에 사는 학생들에게 적절한 비율로 M&M의 색깔이 주어지는지 확인해 보고 싶어한다.
> 
- 선호하는 편집기로 `mnmcount.py` 작성 → 깃허브 저장소로부터 `mnm_dataset.csv` 다운로드 → `bin` 디렉터리의 `submit-spark` 스크립트로 스파크 잡 제출
    - 데이터 위치 : https://github.com/databricks/LearningSparkV2/blob/master/databricks-datasets/learning-spark-v2/mnm_dataset.csv
    - `SPARK_HOME` 환경변수 : 로컬 머신의 스파크 설치 디렉터리의 루트 레벨 경로로 지정
        - 맥os 기준 : `vim ~/.zshrc` → `export SPARK_HOME=/Users/jangboyun/spark/spark-3.3.2-bin-hadoop3` 추가 후 저장 (경로는 본인의 스파크 설치 디렉터리 경로)
    
    ```bash
    $SPARK_HOME/bin/spark-submit mnmcount.py data/mnm_dataset.csv
    ```
    
    - M&M 개수 집계를 위한 파이썬 코드 및 실행화면
        - 위치 : https://github.com/databricks/LearningSparkV2/blob/master/chapter2/py/src/mnmcount.py
        
        ```python
        # 필요한 라이브러리들을 불러온다.
        # 파이썬을 쓰므로 SparkSession과 관련 함수들을 PySpark 모듈에서 불러온다.
        import sys
        
        from pyspark.sql import SparkSession
        
        if __name__ == "__main__":
            if len(sys.argv) != 2:
                print("Usage: mnmcount <file>", file=sys.stderr)
                sys.exit(-1)
        
        # SparkSession API를 써서 SparkSession 객체를 만든다.
        # 존재하지 않으면 객체를 생성한다.
        # JVM마다 SparkSession 객체는 하나만 존재할 수 있다.
            spark = (SparkSession
                .builder
                .appName("PythonMnMCount")
                .getOrCreate())
        # 명령형 인자에서 M&M 데이터가 들어 있는 파일 이름을 얻는다.
            mnm_file = sys.argv[1]
        # 스키마 추론과 쉼표로 구분된 칼럼 이름이 제공되는 헤더가 있음을
        # 지정해 주고 CSV 포맷으로 파일을 읽어 들여 데이터 프레임에 저장한다.
            mnm_df = (spark.read.format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(mnm_file))
            mnm_df.show(n=5, truncate=False)
        
        # 데이터 프레임 고수준 API를 사용하고 RDD는 전혀 쓰지 않는다는 점에 주목하자.
        # 일부 스파크 함수들은 동일한 객체를 되돌려 주므로 함수 호출을 체이닝할 수 있다.
        # 1. 데이터 프레임에서 "State", "Color", "Count" 필드를 읽는다.
        # 2. 각 주별로, 색깔별로 그룹화하기 원하므로 groupBy()를 사용한다.
        # 3. 그룹화된 주/색깔별로 모은 색깔별 집계를 한다.
        # 4. 역순으로 orderBy() 한다.
            count_mnm_df = (mnm_df.select("State", "Color", "Count")
                            .groupBy("State", "Color")
                            .sum("Count")
                            .orderBy("sum(Count)", ascending=False))
        
        # 모든 주와 색깔별로 결과를 보여준다.
        # show()는 액션이므로 위의 쿼리 내용들이 시작되게 된다는 점에 주목하자.
            count_mnm_df.show(n=60, truncate=False)
            print("Total Rows = %d" % (count_mnm_df.count()))
        
        # 위 코드는 모든 주에 대한 집계를 계산한 것이지만
        # 만약 특정 주에 대한 데이터, 예를 들면 캘리포니아(CA)에 대해
        # 보기를 원한다면?
        # 1. 데이터 프레임에서 모든 줄을 읽는다.
        # 2. CA주에 대한 것만 걸러낸다.
        # 3. 위에서 했던 것처럼 주와 색깔별로 groupBy()한다
        # 4. 각 색깔별로 카운트를 합친다.
        # 5. orderBy()로 역순 정렬
        # 필터링을 통해 캘리포니아의 결과만 찾아낸다.
            ca_count_mnm_df = (mnm_df.select("*")
                               .where(mnm_df.State == 'CA')
                               .groupBy("State", "Color")
                               .sum("Count")
                               .orderBy("sum(Count)", ascending=False))
        
        # 캘리포니아의 집계 결과를 보여준다.
        # 위와 마찬가지로 show()는 전체 연산 실행을 발동하는 액션이다.
            ca_count_mnm_df.show(n=10, truncate=False)
        
        # SparkSession을 멈춘다.
            spark.stop()
        ```
        
        <img width="206" alt="Chapter_5" src="https://github.com/Pseudo-Lab/PySpark/assets/91249960/49a92436-4108-4857-bc77-10f9d0713d2b">
        <img width="229" alt="Chapter_6" src="https://github.com/Pseudo-Lab/PySpark/assets/91249960/8e2821c4-d91a-4cba-8012-c210087857e1">
        <img width="215" alt="Chapter_7" src="https://github.com/Pseudo-Lab/PySpark/assets/91249960/86fa1bd5-009f-4e6b-84a1-068a6b90b599">
<br/>

### Building Standalone Applications in Scala

- Scala Build Tool (sbt)를 활용한 Scala Spark Program 구축 방법
    - 간결함을 위해 이 책에서는 주로 Python과 Scala의 예제를 다룸
    - Maven 을 사용하여 Java Spark 프로그램을 빌드하는 방법은 교재 40p.g "guide" 링크 참고
    - build.sbt는 makefile과 같이 Scala를 설명 및 지시하는 파일이고, M&M 코드를 위해 간단한 sbt 파일은 Example 2-3 이다
    - Example 2-3. sbt build file
      
      ![Chapter_8](https://github.com/Pseudo-Lab/PySpark/assets/91249960/90cac5db-1218-4b3b-81a8-61c172ccbbad)

- JDK와 sbt가 설치 + JAVA_HOME, SPARK_HOME이 셋팅 되어 있으면 ⇒ Spark application을 build할 수 있음

```python
$ sbt clean package (명령어로 build 후) 
```

```python
$SPARK_HOME/bin/spark-submit --class main.scala.chapter2.MnMcount \
jars/main-scala-chapter2_2.12-1.0.jar data/mnm_dataset.csv (M&M count 예제를 Scala version으로 run 할 수 있다)
```

![Chapter_9](https://github.com/Pseudo-Lab/PySpark/assets/91249960/5c65b591-c5e5-4acf-af11-c90cfa6ba88b)

<br/>

### Summary

- 이 장에서 Apache Spark 시작하기 위해 세 가지 간단한 단계를 다룸
    - Apache Spark 프레임워크 다운로드
    - Scala 또는 PySpark 대화형 shell에 익숙해지기
    - 높은 수준의 Spark 애플리케이션 개념과 용어 이해하기
- 다음장(Chapter3)에서는 Spark를 통해 상위 수준의 구조화된 API를 어떻게 사용하는 지 알아볼 것
