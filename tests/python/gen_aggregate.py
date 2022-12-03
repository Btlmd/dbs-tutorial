from random_gen import *

if __name__ == "__main__":
    sql = """    
    CREATE DATABASE test_db;
    USE test_db;

CREATE TABLE INFO(C1 INT, C2 INT, C3 FLOAT, C4 VARCHAR(10));
INSERT INTO INFO VALUES (1, 8, -3.0, 'A');
INSERT INTO INFO VALUES (2, 7, 3.0, 'B');
INSERT INTO INFO VALUES (3, 6, 1.1, 'C');
INSERT INTO INFO VALUES (4, 5, 1.1, 'A');
INSERT INTO INFO VALUES (-1, 4, 3.3, 'B');
INSERT INTO INFO VALUES (2, 3, 4.4, 'C');
INSERT INTO INFO VALUES (3, 2, 5.5, 'A');
INSERT INTO INFO VALUES (99, 6, 6.6, 'B');
INSERT INTO INFO VALUES (8, 6, 8.8, 'C');
INSERT INTO INFO VALUES (4, 5, 8.8, 'A');
INSERT INTO INFO VALUES (-1, 4, 9.9, 'B');
INSERT INTO INFO VALUES (233, -9, 10.1, 'C');
INSERT INTO INFO VALUES (73, 4, 9.9, 'B');
INSERT INTO INFO VALUES (2, 3, 10.1, 'C');
INSERT INTO INFO VALUES (-99, 4, 3.8, 'B');
INSERT INTO INFO VALUES (-7, 3, 36.3, 'C');
INSERT INTO INFO VALUES (2, 5, 3.8, 'C');

CREATE TABLE INFO2(C1 INT, C2 VARCHAR(10));
INSERT INTO INFO2 VALUES (333, 'A');
INSERT INTO INFO2 VALUES (666, 'A');
INSERT INTO INFO2 VALUES (999, 'B');
INSERT INTO INFO2 VALUES (777, 'C');
INSERT INTO INFO2 VALUES (111, 'C');
INSERT INTO INFO2 VALUES (222, 'C');

SELECT INFO.C2, MIN(INFO.C1), MAX(INFO.C1), AVG(INFO.C1), SUM(INFO.C1), COUNT(INFO.C1), COUNT(*) FROM INFO GROUP BY INFO.C2; 
SELECT INFO.C3, MIN(INFO.C1), MAX(INFO.C1), AVG(INFO.C1), SUM(INFO.C1), COUNT(INFO.C1), COUNT(*) FROM INFO GROUP BY INFO.C3; 
SELECT INFO.C4, MIN(INFO.C1), MAX(INFO.C1), AVG(INFO.C1), SUM(INFO.C1), COUNT(INFO.C1), COUNT(*) FROM INFO GROUP BY INFO.C4; 
SELECT INFO.C4, MIN(INFO.C3), MAX(INFO.C3), AVG(INFO.C3), SUM(INFO.C3), COUNT(INFO.C3), COUNT(*) FROM INFO GROUP BY INFO.C4;
SELECT COUNT(*) FROM INFO GROUP BY INFO.C1;

SELECT MIN(INFO.C1), MAX(INFO.C1), AVG(INFO.C1), SUM(INFO.C1), COUNT(INFO.C1), COUNT(*) FROM INFO;

SELECT INFO.C4, MIN(INFO.C1), MAX(INFO.C1), AVG(INFO.C1), SUM(INFO.C1), SUM(INFO2.C1), COUNT(*) FROM INFO, INFO2 WHERE INFO.C4 = INFO2.C2 GROUP BY INFO.C4;
    
    CREATE TABLE info (
        C1 INT DEFAULT 10,
        C2 FLOAT DEFAULT 3.14,
        C3 VARCHAR(50) DEFAULT 'default for C3',
        C4 CHAR(50) DEFAULT 'default for C4',
        C5 INT NOT NULL 
    );
    
    CREATE TABLE info2 (
        D1 CHAR(40),
        D2 VARCHAR(50),
        D3 FLOAT,
        D4 INT
    );
    
    CREATE TABLE info3 (
        E1 CHAR(40),
        E2 VARCHAR(50),
        E3 FLOAT,
        E4 INT
    );
    
    CREATE TABLE info4 (
        F1 FLOAT,
        F2 VARCHAR(50),
        F3 CHAR(40),
        F4 INT
    );
    
    CREATE TABLE info5 (
        G1 FLOAT,
        G2 VARCHAR(50),
        G3 CHAR(40),
        G4 INT
    );
    
    CREATE TABLE crafted (
        A INT,
        B FLOAT,
        C CHAR(20),
        D VARCHAR(30)
    );
    
    DESC info;
    DESC info2;
    DESC info3;
    DESC info4;
    DESC info5;
    DESC crafted;
    
    INSERT INTO crafted VALUES (18, NULL, NULL, 'jfhgfa'), (8, NULL, 'rgqh', 'yqiihqq'), (3, NULL, 'ergr', NULL), (NULL, NULL, 'string', 'halo');
    
{}
    
    SELECT COUNT(*), COUNT(crafted.A), COUNT(crafted.B), COUNT(crafted.C), COUNT(crafted.D) FROM crafted; 
    SELECT MAX(crafted.A), MAX(crafted.B), MAX(crafted.C), MAX(crafted.D) FROM crafted; 
    SELECT AVG(crafted.A), AVG(crafted.B), AVG(crafted.C), AVG(crafted.D) FROM crafted; 
    
    SELECT SUM(info.C2), AVG(info.C2), MAX(info.C2), MIN(info.C2), COUNT(*), COUNT(info.C2), info.C1 FROM info WHERE info.C1 > 5 AND info.C5 < 35 GROUP BY info.C1;

    """

    insert_sql = ""

    template = [i ,f, s(50), s(50), i]
    insert_sql += gen_insertions(template, 'info', 200)

    template = [s(40), s(50), f, i]
    insert_sql += gen_insertions(template, 'info2', 100)

    template = [s(40), s(50), f, i]
    insert_sql += gen_insertions(template, 'info3', 60)

    template = [f, s(50), s(40), i]
    insert_sql += gen_insertions(template, 'info4', 40)

    template = [f, s(50), s(40), i]
    insert_sql += gen_insertions(template, 'info5', 40)

    print(sql.format(insert_sql))







