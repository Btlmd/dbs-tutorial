//
// Created by lambda on 22-11-26.
//

#ifndef DBS_TUTORIAL_QUERYSYSTEM_H
#define DBS_TUTORIAL_QUERYSYSTEM_H

#include <system/DBSystem.h>
#include <record/Record.h>
#include <display/Result.h>

class QuerySystem {
public:
    explicit QuerySystem(DBSystem &dbms);

private:
    DBSystem &dbms;
};


#endif //DBS_TUTORIAL_QUERYSYSTEM_H
