from random_gen import *

if __name__ == "__main__":
    sql = """
    CREATE DATABASE test_db;
    USE test_db;
    
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
    
    
    DESC info;
    DESC info2;
    
    {}
    
    SELECT * FROM info;
    SELECT * FROM info2;
    
    -- info1;
    SELECT * FROM info LIMIT 1000;
    SELECT * FROM info LIMIT 100;
    SELECT * FROM info LIMIT 1;
    
    SELECT * FROM info LIMIT 100 OFFSET 0;
    SELECT * FROM info LIMIT 100 OFFSET 100;
    SELECT * FROM info LIMIT 100 OFFSET 1000;
    SELECT * FROM info LIMIT 100 OFFSET 10000000;
    
    SELECT * FROM info LIMIT 0 OFFSET 0;
    
    -- info2;
    SELECT * FROM info2 LIMIT 1000;
    SELECT * FROM info2 LIMIT 100;
    SELECT * FROM info2 LIMIT 1;
    
    SELECT * FROM info2 LIMIT 100 OFFSET 0;
    SELECT * FROM info2 LIMIT 100 OFFSET 100;
    SELECT * FROM info2 LIMIT 100 OFFSET 1000;
    SELECT * FROM info2 LIMIT 100 OFFSET 10000000;
    
    SELECT * FROM info2 LIMIT 0 OFFSET 0;
        
    -- with where cond;    
    SELECT * FROM info WHERE info.C1 < info.C5 AND info.C2 > 50 AND info.C5 > 20 LIMIT 1000;
    SELECT * FROM info WHERE info.C1 < info.C5 AND info.C2 > 50 AND info.C5 > 20 LIMIT 100;
    SELECT * FROM info WHERE info.C1 < info.C5 AND info.C2 > 50 AND info.C5 > 20 LIMIT 1;
    SELECT * FROM info WHERE info.C1 < info.C5 AND info.C2 > 50 AND info.C5 > 20 LIMIT 100 OFFSET 0;
    SELECT * FROM info WHERE info.C1 < info.C5 AND info.C2 > 50 AND info.C5 > 20 LIMIT 100 OFFSET 100;
    SELECT * FROM info WHERE info.C1 < info.C5 AND info.C2 > 50 AND info.C5 > 20 LIMIT 100 OFFSET 1000;
    SELECT * FROM info WHERE info.C1 < info.C5 AND info.C2 > 50 AND info.C5 > 20 LIMIT 100 OFFSET 10000000;
    SELECT * FROM info WHERE info.C1 < info.C5 AND info.C2 > 50 AND info.C5 > 20 LIMIT 0 OFFSET 0;
    SELECT * FROM info WHERE info.C1 < info.C5 AND info.C2 > 50 AND info.C5 > 20;
    """

    insert_sql = ""

    template = [i ,f, s(50), s(50), i]
    insert_sql += gen_insertions(template, 'info', 20000)

    template = [s(40), s(50), f, i]
    insert_sql += gen_insertions(template, 'info2', 10)

    print(sql.format(insert_sql))







