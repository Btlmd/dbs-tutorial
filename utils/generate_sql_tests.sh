#!/usr/bin/env bash

PYGEN_DIR=$(dirname $0)/../tests/python
SQL_DIR=$(dirname $0)/../tests/sql

# generate SQL files
for PY in $(ls $PYGEN_DIR | grep gen_)
do
  echo " > Running"$PY
  python3 $PYGEN_DIR/$PY > $SQL_DIR/${PY}.sql
done

