CREATE DATABASE test_db0;
CREATE DATABASE test_db1;
SHOW DATABASES;

CREATE TABLE scholars(
     int_f0 INT,
     int_f1 INT,
     vc_f VARCHAR(20) NOT NULL,
     float_f FLOAT,

     PRIMARY KEY pk_name (int_f0, int_f1),
    FOREIGN KEY fk_name (int_f0, int_f1) REFERENCES tt(int_f2, int_f3)
);