### MySQL 도커 생성

1. `docker pull mysql`
2. `docker run --name mysql -e MYSQL_ROOT_PASSWORD="0000" -d -p 3306:3306 mysql`
3. `docker exec -it mysql-container bash`

- 도커 터미널 진입 후

4. `mysql -u root -p`
5. 비밀번호 입력


### 간단한 쿼리

```sql
DROP TABLE IF EXISTS Employee CASCADE;
DROP TABLE IF EXISTS Department;

Create table If Not Exists Employee (Id int, Name varchar(255), Salary int, DepartmentId int);
Create table If Not Exists Department (Id int, Name varchar(255));

insert into Employee (Id, Name, Salary, DepartmentId) values ('1', 'Joe', '70000', '1');
insert into Employee (Id, Name, Salary, DepartmentId) values ('2', 'Jim', '90000', '1');
insert into Employee (Id, Name, Salary, DepartmentId) values ('3', 'Henry', '80000', '2');
insert into Employee (Id, Name, Salary, DepartmentId) values ('4', 'Sam', '60000', '2');
insert into Employee (Id, Name, Salary, DepartmentId) values ('5', 'Max', '90000', '1');

insert into Department (Id, Name) values ('1', 'IT');
insert into Department (Id, Name) values ('2', 'Sales');
```


<br>
