/*
select customer_id,
CASE 
    WHEN (customer_id <= 100) THEN 'Premium'
    WHEN (customer_id > 100 AND customer_id <= 200) THEN 'Plus'
    ELSE 'Normal'
END AS customer_class
from customer;

select customer_id,
CASE customer_id
    WHEN 1 THEN 'Winner'
    WHEN 2 THEN 'Runner-up'
    WHEN 3 THEN 'Third Place'
    ELSE 'Participant'
END AS competition_result
from customer;

--- Counting films by rental rate ---

SELECT
SUM(CASE rental_rate
    WHEN 0.99 THEN 1
    ELSE 0
END) AS bargains,
SUM(CASE rental_rate
    WHEN 2.99 THEN 1
    ELSE 0
END) AS regular,
SUM(CASE rental_rate
    WHEN 4.99 THEN 1
    ELSE 0
END) AS premium
from film;

--- Count films by rating ---

SELECT
SUM(CASE rating
    WHEN 'R' THEN 1
    ELSE 0
END) AS r,
SUM(CASE rating
    WHEN 'PG' THEN 1
    ELSE 0
END) AS pg,
SUM(CASE rating
    WHEN 'PG-13' THEN 1
    ELSE 0
END) AS pg13
from film;

--- Cast example ---

SELECT CAST('5' AS INTEGER) + 10 AS result;

--- Usecase for CAST on data type ---

select CHAR_LENGTH(CAST(inventory_id AS VARCHAR)) from rental;

*/

--- NULL IF Example ---

SELECT NULLIF(5, 5) AS result1,  -- returns NULL
       NULLIF(5, 10) AS result2; -- returns 5




/*

CREATE TABLE account(
user_id SERIAL PRIMARY KEY,
username VARCHAR(50) UNIQUE NOT NULL,
password VARCHAR(50) NOT NULL,
email VARCHAR(250) UNIQUE NOT NULL,
created_on TIMESTAMP NOT NULL,
last_login TIMESTAMP
)

CREATE TABLE job(
job_id SERIAL PRIMARY KEY,
job_name VARCHAR(200) UNIQUE NOT NULL
)

CREATE TABLE account_job(
user_id INTEGER REFERENCES account(user_id),
JOB_ID INTEGER REFERENCES job(job_id),
hire_date TIMESTAMP
)

INSERT INTO account_job (user_id, job_id, hire_date)
VALUES (10, 10, current_timestamp);

Update join value from anpther table

update account_job
set hire_date = account.created_on
from account
where account_job.user_id = account.user_id;

Update with returning

update account
set last_login = current_timestamp
returning  user_id, last_login;


select * from job;/*

CREATE TABLE account(
user_id SERIAL PRIMARY KEY,
username VARCHAR(50) UNIQUE NOT NULL,
password VARCHAR(50) NOT NULL,
email VARCHAR(250) UNIQUE NOT NULL,
created_on TIMESTAMP NOT NULL,
last_login TIMESTAMP
)

CREATE TABLE job(
job_id SERIAL PRIMARY KEY,
job_name VARCHAR(200) UNIQUE NOT NULL
)

CREATE TABLE account_job(
user_id INTEGER REFERENCES account(user_id),
JOB_ID INTEGER REFERENCES job(job_id),
hire_date TIMESTAMP
)

INSERT INTO account_job (user_id, job_id, hire_date)
VALUES (10, 10, current_timestamp);

Update join value from anpther table

update account_job
set hire_date = account.created_on
from account
where account_job.user_id = account.user_id;

Update with returning

update account
set last_login = current_timestamp
returning  user_id, last_login;


select * from job;/*

CREATE TABLE account(
user_id SERIAL PRIMARY KEY,
username VARCHAR(50) UNIQUE NOT NULL,
password VARCHAR(50) NOT NULL,
email VARCHAR(250) UNIQUE NOT NULL,
created_on TIMESTAMP NOT NULL,
last_login TIMESTAMP
)

CREATE TABLE job(
job_id SERIAL PRIMARY KEY,
job_name VARCHAR(200) UNIQUE NOT NULL
)

CREATE TABLE account_job(
user_id INTEGER REFERENCES account(user_id),
JOB_ID INTEGER REFERENCES job(job_id),
hire_date TIMESTAMP
)

INSERT INTO account_job (user_id, job_id, hire_date)
VALUES (10, 10, current_timestamp);

Update join value from anpther table

update account_job
set hire_date = account.created_on
from account
where account_job.user_id = account.user_id;

Update with returning

update account
set last_login = current_timestamp
returning  user_id, last_login;

delete from job where job_name = 'tester'
returning job_id, job_name;


CREATE TABLE account(
user_id SERIAL PRIMARY KEY,
username VARCHAR(50) UNIQUE NOT NULL,
password VARCHAR(50) NOT NULL,
email VARCHAR(250) UNIQUE NOT NULL,
created_on TIMESTAMP NOT NULL,
last_login TIMESTAMP
)

CREATE TABLE job(
job_id SERIAL PRIMARY KEY,
job_name VARCHAR(200) UNIQUE NOT NULL
)

CREATE TABLE account_job(
user_id INTEGER REFERENCES account(user_id),
JOB_ID INTEGER REFERENCES job(job_id),
hire_date TIMESTAMP
)

INSERT INTO account_job (user_id, job_id, hire_date)
VALUES (10, 10, current_timestamp);

Update join value from anpther table

update account_job
set hire_date = account.created_on
from account
where account_job.user_id = account.user_id;

Update with returning

update account
set last_login = current_timestamp
returning  user_id, last_login;

ALTER TABLE public.students
ALTER COLUMN email DROP NOT NULL;

*/
