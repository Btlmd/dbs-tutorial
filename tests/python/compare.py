import pymysql
from argparse import ArgumentParser
import connect_db
import decimal
import datetime
from pathlib import Path

class NullableTuple(tuple):
    def __lt__(self, other):
        for i, j in zip(self, other):
            if i is None and j is None:
                continue
            if i is None:
                return True
            if j is None:
                return False
            if i == j:
                continue
            return i < j
        return False


def round_resp(rows):
    rows = list(map(list, rows))
    for row in rows:
        for i in range(len(row)):
            if isinstance(row[i], decimal.Decimal):
                row[i] = float(row[i])
            if isinstance(row[i], float):
                row[i] = round(row[i], 3)
            if isinstance(row[i], datetime.date):
                row[i] = str(row[i])
    rows = map(NullableTuple, rows)
    rows = sorted(rows)
    return rows

def tidy_display(rows):
    print("LENS", len(rows))
    for row in rows:
        repr_ = []
        for f in row:
            if isinstance(f, str):
                repr_.append("'" + f + "'")
            else:
                repr_.append(str(f))
        print("(" + ", ".join(repr_) + ")")

def display_query(idx, q):
    print("[QUERY %d]" % idx, q)

def to_query_lines(_queries_raw):
    _queries = map(str.strip, _queries_raw.split(";"))
    _queries = filter(lambda x: x and not x.startswith("--"), _queries)
    _queries = map(lambda x: x + ";", _queries)
    _queries = list(_queries)
    return _queries

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('--host', type=str, default='127.0.0.1')
    parser.add_argument('--user', type=str, default='root')
    parser.add_argument('--password', type=str, default='root')
    parser.add_argument('--sql', type=str, required=True)
    args = parser.parse_args()

    connection = pymysql.connect(host=args.host,
                                 user=args.user,
                                 password=args.password,
                                 cursorclass=pymysql.cursors.Cursor)

    with open(args.sql, "r") as f:
        queries_raw = f.read()
        queries = to_query_lines(queries_raw)


    # get results from `dbs-tutorial`
    dbms_results = connect_db.query(queries_raw)


    # get results from MySQL
    mysql_results = []
    print("Executing MySQL queries ..")
    with connection:
        with open(Path(args.sql).parent / 'mysql_init.sql', 'r') as f:
            mysql_init_queries = to_query_lines(f.read())
        for q in mysql_init_queries:
            with connection.cursor() as cursor:
                cursor.execute(q)
                connection.commit()

        for idx, q in enumerate(queries):
            display_query(idx, q)
            with connection.cursor() as cursor:
                cursor.execute(q)
                connection.commit()
                mysql_results.append(cursor.fetchall())

    print(len(queries), len(mysql_results), len(dbms_results))

    fail = False

    failures = []

    for idx, (q, m, d) in enumerate(zip(queries, mysql_results, dbms_results)):
        display_query(idx, q)
        if d['type'] == 'record_table':
            m = round_resp(m)
            d = round_resp(d['resp'])
            if m != d:
                print("[MYSQL]")
                tidy_display(m)
                print("[dbs-tutorial]")
                tidy_display(d)
                print(len(m), len(d))
                fail = True
                print("[FAIL]")
                failures.append((idx, q))
            else:
                print(len(m), "row(s) in set")
                print("[PASS]")
        else:
            print("[dbs-tutorial]")
            if d['type'] == 'sys_table':
                tidy_display(d['resp'])
            else:
                print(d['resp'])
        print()

    print("failed queries")
    for idx, q in failures:
        print("%d: %s" % (idx, q))

    if fail:
        exit(1)
