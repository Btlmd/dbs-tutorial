from random_gen import *
from functools import partial

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
    
    {}
    
    SELECT * FROM info WHERE info.C1 > 1000 AND info.C1 < 1010;
    
    ALTER TABLE info ADD INDEX (C1);
    
    SELECT * FROM info WHERE info.C1 > 1000 AND info.C1 < 1010;
    
    """

    insert_sql = ""

    template = [partial(i, 0, 4000) ,f, s(50), s(50), i]
    insert_sql += gen_insertions(template, 'info', 1_000_000, show_tqdm=True)
    print(sql.format(insert_sql))







