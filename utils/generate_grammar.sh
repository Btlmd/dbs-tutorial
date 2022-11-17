#!/usr/bin/bash

set -e

java --version > /dev/null

CACHE_DIR=~/.cache/antlr
CACHE_FILE=$CACHE_DIR/antlr-4.9.3-complete.jar
GRAMMAR_DIR=$(realpath "$(dirname $0)/../src/grammar")

GLOBIGNORE="${GRAMMAR_DIR}/*.g4"
if rm ${GRAMMAR_DIR}/*; then
  echo " > Grammar CPP files removed"
else
  echo " > Grammar directory is clean"
fi
unset GLOBIGNORE

if [ ! -d $CACHE_DIR ]; then
  echo " > Create cache directory $CACHE_DIR"
  mkdir -p $CACHE_DIR
fi

if [ ! -f $CACHE_FILE ]; then
    echo " > Download ANTLR Java executable to $CACHE_FILE"
  wget -O $CACHE_FILE https://www.antlr.org/download/antlr-4.9.3-complete.jar
fi

CMD="java -jar $CACHE_FILE -Werror -Dlanguage=Cpp -no-listener -visitor -o $GRAMMAR_DIR $GRAMMAR_DIR/SQL.g4"
echo " > $CMD"; eval $CMD

echo " > Grammar CPP files generated successfully"
#ls $GRAMMAR_DIR