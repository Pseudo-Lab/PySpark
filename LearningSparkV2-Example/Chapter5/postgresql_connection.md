### Postgresql 생성

- port : 5432
- 도커로 사용할 시

  1. `docker pull postgresql`
  2. `docker run -d -p 5432:5432 -e POSTGRES_PASSWORD="0000" --name postgresql postgres`
  3. `docker exec -it postgresql /bin/bash`
  4. `psql -U postgres`
  5. `\q` : 종료



sql 쿼리

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
