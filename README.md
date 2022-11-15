# dbs-turoial

Course project for *Introduction to Database Management System (2022 Fall)*.

## Environment

### Setup

1. Install Java
    ```bash
    sudo apt install default-jre
    ```
2. Generate Grammar CPP Files
    ```bash
    ./utils/generate_grammar.sh
    ```
   This will generate the CPP files from `./src/grammar/sql.g4` grammar file. 

### Dependencies

To make modifications easy, dependencies have been copied into `./exteral`.

- SQL Parser: [ANTLR 4.9.3](https://github.com/antlr/antlr4/tree/e4c1a74c66bd5290364ea2b36c97cd724b247357)

- CLI Interface: [cpp-terminal](https://github.com/jupyter-xeus/cpp-terminal)
