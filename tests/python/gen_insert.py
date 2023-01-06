from random_gen import *
from functools import partial

if __name__ == "__main__":
    sql = """
    CREATE DATABASE test_db2;
    USE test_db2;
    
    CREATE TABLE info (
        C1 INT DEFAULT 10,
        C2 FLOAT DEFAULT 3.14,
        C3 VARCHAR(50) DEFAULT 'default for C3',
        C4 CHAR(50) DEFAULT 'default for C4',
        C5 INT NOT NULL 
    );
    
    {insert1}
    
    SELECT * FROM info;
    
    UPDATE info SET C5 = 255, C3 = '{long_char}' WHERE info.C2 < 50;
    
    SELECT * FROM info;
    
    DELETE FROM info WHERE info.C5 < 5;
    
    SELECT * FROM info;
    
    {insert2}
    
    SELECT * FROM info;
    
    DELETE FROM info WHERE info.C5 > 35;
    
    SELECT * FROM info;

    UPDATE info SET C5 = 255, C3 = '{long_char}' WHERE info.C2 > 50;
    
    SELECT * FROM info;
        
    DELETE FROM info WHERE info.C5 < 15;
    
    SELECT * FROM info;
        
    """

    insert_sql = ""
    template = [partial(i, 0, 4000) ,f, s(20), s(50), i]
    insert_sql += gen_insertions(template, 'info', 100_000, show_tqdm=True)

    insert_sql2 = ""
    template = [partial(i, 0, 4000) ,f, s(5), s(20), i]  # into previous used slots
    insert_sql2 += gen_insertions(template, 'info', 100_000, show_tqdm=True)

    print(sql.format(insert1=insert_sql, insert2=insert_sql2, long_char='a'*50))







