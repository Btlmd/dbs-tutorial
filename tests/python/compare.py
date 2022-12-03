import pymysql
from argparse import ArgumentParser
import connect_db
import decimal

def round_resp(rows):
    rows = list(map(list, rows))
    for row in rows:
        for i in range(len(row)):
            if isinstance(row[i], decimal.Decimal):
                row[i] = float(row[i])
            if isinstance(row[i], float):
                row[i] = round(row[i], 3)
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
        queries = map(str.strip, queries_raw.split(";"))
        queries = filter(lambda x: x, queries)
        queries = map(lambda x: x + ";", queries)
        queries = list(queries)

    # get results from `dbs-tutorial`
    dbms_results = connect_db.query(queries_raw)


    # get results from MySQL
    mysql_results = []
    print("Executing MySQL queries ..")
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("DROP DATABASE IF EXISTS test_db;")
            connection.commit()

        for idx, q in enumerate(queries):
            display_query(idx, q)
            with connection.cursor() as cursor:
                cursor.execute(q)
                connection.commit()
                mysql_results.append(cursor.fetchall())

    print(len(queries), len(mysql_results), len(dbms_results))

    fail = False

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

    if fail:
        exit(1)
