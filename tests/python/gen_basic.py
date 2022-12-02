import random
from string import ascii_letters
from functools import partial

with open("../sql/wordlist.txt", "r") as f:
    word_list = f.read().split()

random.seed(4022)

def i():
    return random.randint(0, 40)

def f():
    return round(random.random() * 100, 3)

def s(max_len):
    def _s():
        ret = []
        r_len = random.random() * max_len
        while sum(map(len, ret)) + len(ret) - 1 < r_len:
            ret.append(random.choice(word_list))
        while sum(map(len, ret)) + len(ret) - 1 > max_len:
            ret.pop()
        return " ".join(ret)
    return _s

def to_record(fields):
    out = []
    for f in fields:
        if isinstance(f, str):
            out += ["'" + f + "'"]
        else:
            out += [str(f)]
    # print(out)
    return "(" + ", ".join(out) + ")"

insert = "INSERT INTO %s VALUES %s;\n"

def gen_insertions(template_func, name, count):
    _sql = []
    for j in range(count):
        _sql += [to_record([x() for x in template])]
    _sql = ", ".join(_sql)
    _sql = insert % (name, _sql);
    return _sql



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
    
    DESC info;
    DESC info2;
    DESC info3;
    DESC info4;
    DESC info5;
    
    %s
    
    SELECT * FROM info;
    SELECT * FROM info, info2, info3, info4, info5 WHERE info.C1 = info2.D4 AND info2.D4 = info3.E4 AND info3.E4 = info4.F4 AND info4.F4 = info5.G4;
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

    print(sql % insert_sql)







