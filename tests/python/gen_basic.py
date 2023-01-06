from random_gen import *

if __name__ == "__main__":
    sql = """
    CREATE DATABASE test_db1;
    USE test_db1;
    
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
    INSERT INTO info VALUES (10, 1, 'hello', 'world', 20);

    SELECT * FROM info;
    SELECT * FROM info2;
    SELECT * FROM info3;
    SELECT * FROM info4;
    SELECT * FROM info5;
    

    SELECT * FROM info WHERE info.C5 IN (SELECT crafted.A FROM crafted);
    SELECT * FROM info WHERE info.C5 IN (SELECT crafted.A FROM crafted) AND info.C1 = (SELECT MAX(info2.D4) FROM info2);
    SELECT MAX(info.C1) FROM info WHERE info.C1 < (
        SELECT MAX(info.C1) FROM info WHERE info.C1 < (
            SELECT MAX(info.C1) FROM info WHERE info.C1 < (
                SELECT MAX(info.C1) FROM info WHERE info.C1 < (
                    SELECT MAX(info.C1) FROM info WHERE info.C1 < (
                        SELECT MAX(info.C1) FROM info WHERE info.C1 < (
                            SELECT MAX(info.C1) FROM info
                        )
                    )
                )
            )
        )
    );
    
    SELECT * FROM info WHERE info.C1 < info.C5 AND info.C2 > 50 AND info.C5 > 20;
    SELECT * FROM info WHERE info.C3 LIKE '%e_e%';
    SELECT * FROM info WHERE info.C4 LIKE '%e_e%';
    
    SELECT * FROM info WHERE info.C1 = 10;
    SELECT * FROM info WHERE info.C2 = 0;
    SELECT * FROM info WHERE info.C3 = 'hello';
    SELECT * FROM info WHERE info.C4 = 'world';
    
    UPDATE info SET C1 = 1000, C2 = 5, C3 = 'sel1', C4 = 'sel2', C5 = 255 WHERE info.C5 = 10;
    
    SELECT * FROM info;
    
    DELETE FROM info WHERE info.C5 = 10;
    
    SELECT * FROM info;
    
    DELETE FROM info WHERE info.C5 = 11;
    
    SELECT * FROM info;
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







