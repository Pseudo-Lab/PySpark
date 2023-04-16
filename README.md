# PySpark

Big Data Analysis using PySpark (6th Academic Study)

### 1.설치방법

- Python 또는 Anaconda 설치 : [https://www.anaconda.com](https://www.anaconda.com)
- java 설치(openJDK 기준. OS 모두 포함) : [https://github.com/ojdkbuild/ojdkbuild](https://github.com/ojdkbuild/ojdkbuild)
- Apache Hadoop 설치 : [https://hadoop.apache.org/releases.html](https://hadoop.apache.org/releases.html)

  - binary download 항목의 binary 설치
  - MacOS 경우 `brew install hadoop`로 설치 가능
  - 경로 설정 해야함
  - 하둡 관련 자세한 사항은 해당 링크 참조
    - [맥 m1 환경에서 하둡(Hadoop), 스파크(Spark) 설치 및 환경설정하기](https://ingu627.github.io/tips/hadoop/install_hadoop_mac/)
- Apache Spark 설치 : [https://spark.apache.org/downloads.html](https://spark.apache.org/downloads.html)

  - MacOS 경우 `brew install spark`로 설치 가능
  - 경로 설정 해야함

<br>

### 2. 경로 설정

- `open ~/.zshrc` 또는 `open ~/.bashrc`로 실행
- 아래는 MacOS 기준 경로 설정 예시

```apache

# JAVA 경로
export JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk-19.0.2.jdk/Contents/Home"
export PATH=${PATH}:$JAVA_HOME/bin

# HADOOP 경로
export HADOOP_HOME="/Users/hyunseokjung/hadoop-3.3.5"
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

# SPARK 경로 
export SPARK_HOME="/Users/hyunseokjung/spark-3.3.0"
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export SPARK_LOCAL_HOSTNAME=localhost
export PYTHONPATH="${SPARK_HOME}/python/:${SPARK_HOME}/python/lib/py4j-*-src.zip):${PYTHONPATH}"
export SPARK_DIST_CLASSPATH=$HADOOP_HOME/share/hadoop/tools/lib/*

# pyspark-python 경로
export PYSPARK_PYTHON="/Users/hyunseokjung/opt/anaconda3/envs/pyspark/bin/python"


```

<br>

### 3. 가상 환경 설치

- 가상 환경에 pyspark 설치. 단, 해당 버전과 맞춰야함을 주의. [^1]

```apache
conda create -n 가상환경이름 python=3.10

conda activate 가상환경 이름

pip install --upgrade pip --user

pip install pandas matplotlib seaborn scipy sklearn

pip install pyspark

pip install jupyter notebook
pip install ipykernel
python -m ipykernel install --user --name 가상환경이름 --display-name 표시할 가상환경 이름

```

### 4. 도커 사용하기

- 우분투(20.04) 환경에서 파이썬, 자바, 하둡, 스파크 등이 설치되어 있는 이미지를 의미한다.

  - `Windows` : `docker pull ingu627/hadoop:spark3.3.0`
- `MacOS` : `docker pull ingu627/hadoop:pyspark-mac`

<br>

<br>

<br>

## Reference

[1]: [[2022최신] 텐서플로우(tensorflow), 파이토치(pytorch) GPU 설치 방법](https://ingu627.github.io/tips/install_cuda2/#8-가상환경)
