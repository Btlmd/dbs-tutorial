CREATE DATABASE test_db0;
CREATE DATABASE test_db1;
SHOW DATABASES;
USE test_db0;

CREATE TABLE t0(
     int_f0 INT,
     int_f1 INT,
     vc_f VARCHAR(20) NOT NULL,
     float_f FLOAT,

     PRIMARY KEY pk_name (int_f0, int_f1)
);

INSERT INTO t0 VALUES (1, 2, 'TEST_CHARS', 2.71828);