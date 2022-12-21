CREATE DATABASE DB;
USE DB;
CREATE TABLE a_table_name
(
    a INT,
    b FLOAT,
    c VARCHAR(8),
    d INT,
    e INT,
    PRIMARY KEY pk_name (a, d, e)
);

CREATE TABLE T
(
    A     INT,
    B     VARCHAR(10),
    C     FLOAT,
    D     CHAR(15),
    E     INT,
    F     VARCHAR(10),
    G     FLOAT NOT NULL DEFAULT 2.718,
    H     CHAR(15) DEFAULT 'default for H',
    II    INT NOT NULL,
    JJJ   VARCHAR(10),
    KKKK  FLOAT NOT NULL,
    LLLLL CHAR(15),
    PRIMARY KEY PK_NAME (LLLLL, H, D, A),
    FOREIGN KEY fk_1 (A, E, II) REFERENCES a_table_name (a, d, e),
    FOREIGN KEY fk_2 (A, E, II) REFERENCES a_table_name (a, d, e),
    FOREIGN KEY fk_3 (A, E, II) REFERENCES a_table_name (a, d, e),
    FOREIGN KEY fk_4 (A, E) REFERENCES a_table_name (a, e),
    FOREIGN KEY fk_5 (E) REFERENCES a_table_name (e)
);

DESC a_table_name;
DESC T;

USE DB;

DESC a_table_name;
DESC T;

USE DB;

DESC a_table_name;
DESC T;

USE DB;

DESC a_table_name;
DESC T;



