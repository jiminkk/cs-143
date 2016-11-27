/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
  RecordFile rf;            //record file containing the table
  RecordId   rid;           //record cursor for scanning table
  BTreeIndex treeindex;
  RC rc;

  // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
  fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
  return rc;
  }

  SelCond curr_cond;
  int curr_val;
  int min = -1;
  int max = -1;
  int ge = 0;
  int le = 0;

  int keyIsEQ = -1;             //key, if not -1
  string valIsEQ = "";          //value, if not empty

  bool isNE = false;            //if cond is not equals, no index tree
  bool hasValCond = false;      //if value condition exists, if not: skip disk reading
  bool indexUsed = false;       //check for closing index tree appropriately
  
  //variables for reading record file that contains the table
  int key;
  string value;
  int tupleCount = 0;
  int diff;

  //iterate through all of the conditions
  for (unsigned i = 0; i < cond.size(); i++) {
    curr_cond = cond[i];
    curr_val = atoi(cond[i].value);

    switch(curr_cond.attr) {
      case 1: //if attribute is a key (all operators can apply here)
        switch (curr_cond.comp) {
          case SelCond::EQ:
            keyIsEQ = curr_val;
            break;
          case SelCond::NE:
            isNE = true;
            break;
          case SelCond::LT:
            if (max == -1 || max >= curr_val)
              max = curr_val;
            break;
          case SelCond::GT:
            if (min == -1 || min <= curr_val)
              min = curr_val;
            break;
          case SelCond::LE:
            if (max == -1 || max > curr_val) {
              le = 1;
              max = curr_val;
            }
            break;
          case SelCond::GE:
            if (min == -1 || min < curr_val) {
              ge = 1;
              min = curr_val;
            }
            break;
        }
      case 2: //if attribute is a value (only eq operator applies here since we would be comparing value to value)
        if (curr_cond.comp == SelCond::EQ)
            valIsEQ = curr_val;
    }
  }

  //check if conditions can never be met
  if (max < min && min != -1 && max != -1)
    goto exit_select_early;

  if (max == min && !ge && !le && min != -1 && max != -1)
    goto exit_select_early;

  // DON'T use index trees if either:
  //  1. index tree does not exist
  //  2. isNE is true AND count(*) is not an attribute (4)
  if ((isNE && attr != 4) || treeindex.open(table + ".idx", 'r') != 0) {
    //do a full scan of table (not from index tree)
    rid.pid = 0;
    rid.sid = 0;
    tupleCount = 0;

    while (rid < rf.endRid()) {
      //read rf tuple
      if ((rc = rf.read(rid, key, value)) < 0) {
        fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
        goto exit_select;
      }

      //loop through all tuple conditions
      for (unsigned i = 0; i < cond.size(); i++) {
        //first, get diff of tuple value and condition value
        switch (cond[i].attr) {
          case 1: //key
            diff = key - atoi(cond[i].value);
            break;
          case 2: //value
            diff = strcmp(value.c_str(), cond[i].value);
            break;
        }

        //go to the next tuple if no condition is met
        switch (cond[i].comp) {
          case SelCond::EQ:
            if (diff != 0) goto next_tuple;
            break;
          case SelCond::NE:
            if (diff == 0) goto next_tuple;
            break;
          case SelCond::GT:
            if (diff <= 0) goto next_tuple;
            break;
          case SelCond::LT:
            if (diff >= 0) goto next_tuple;
            break;
          case SelCond::GE:
            if (diff < 0) goto next_tuple;
            break;
          case SelCond::LE:
            if (diff > 0) goto next_tuple;
            break;
        }
      }
      //if condition is met, increment tuple counter
      tupleCount++;

      //print tuple 
      switch (attr) {
        case 1:  //key
          cout << key << endl;
          break;
        case 2:  //value
          cout << value.c_str() << endl;
          break;
        case 3:  //*
          cout << key << ' ' << value.c_str() << endl;
          break;
      }

      //move to next tuple
      next_tuple:
        rid++;
    }
  } //END if-statement for using normal select
  else if (treeindex.open(table + "idx", 'r') == 0) { //check there is a B+ tree for this query
    indexUsed = true;
    tupleCount = 0;
    rid.pid = 0;
    rid.sid = 0;
    IndexCursor cursor;

    //set cursor position below
    if (keyIsEQ != -1)                  //if we are trying to find a particular key
      treeindex.locate(keyIsEQ, cursor);
    else if (min != -1 && ge == 1)      //key >= min
      treeindex.locate(min, cursor);
    else if (min != -1 && ge == 0)      //key > min
      treeindex.locate(min + 1, cursor);
    else                                //start from very first element
      treeindex.locate(0, cursor);

    //read through entire tree index
    while (treeindex.readForward(cursor, key, rid) == 0) {

      //if only key columns exist, avoid reading from disk
      if (!hasValCond && attr == 4) {
        //validate keys
        if (keyIsEQ != -1 && key != keyIsEQ)    //equality conflict
          goto exit_select_early;

        if (max != -1) {                        //LE conflict
          if ((le && max < key) || (!le && max <= key))
            goto exit_select_early;
        }

        if (min != -1) {                        //GE conflict
          if ((ge && min > key) || (!ge && min >= key))
            goto exit_select_early;
        }

        tupleCount++;
        continue;     //continue while loop, skip over reading from disk
      }

      //read from disk tuple
      if ((rc = rf.read(rid, key, value)) < 0) {
        fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
        goto exit_select;
      }

      //loop through all tuple conditions
      for (unsigned i = 0; i < cond.size(); i++) {
        //first, get diff of tuple value and condition value
        switch (cond[i].attr) {
          case 1: //key
            diff = key - atoi(cond[i].value);
            break;
          case 2: //value
            diff = strcmp(value.c_str(), cond[i].value);
            break;
        }

        //skip tuple if no condition is met, continue while loop
        switch (cond[i].comp) {
          case SelCond::EQ:
            if (diff != 0) {
              if (cond[i].attr == 1)      //if key, we're past the key that was once equal, so no more keys that will equal
                goto exit_select_early;   //since all keys are sorted
              goto continue_while_loop;
            }
            break;
          case SelCond::NE:
            if (diff == 0) goto continue_while_loop;
            break;
          case SelCond::GT:
            if (diff <= 0) goto next_tuple;
            break;
          case SelCond::LT:
            if (diff >= 0) {
              if (cond[i].attr == 1)      //if this fails, following keys will fail as well
                goto exit_select_early;
              goto next_tuple;
            }
            break;
          case SelCond::GE:
            if (diff < 0) goto next_tuple;
            break;
          case SelCond::LE:
            if (diff > 0) {
              if (cond[i].attr == 1)      //same for less than or equal to
                goto exit_select_early;
              goto next_tuple;
            }
            break;
        }
      }

      tupleCount++;     //when condition is met, increment count of tuples

      // print the tuple
      switch (attr) {
        case 1:  // SELECT key
          fprintf(stdout, "%d\n", key);
          break;
        case 2:  // SELECT value
          fprintf(stdout, "%s\n", value.c_str());
          break;
        case 3:  // SELECT 
          fprintf(stdout, "%d '%s'\n", key, value.c_str());
      }

      continue_while_loop:
        ;//do nothing
    }
  } //END else-statement for using tree index

  exit_select_early:    //go here if tuple fails a condition
    //print matching tuple count if "select count(*)"
    if (attr == 4)
      fprintf(stdout, "%d\n", tupleCount);
    rc = 0;

  // close the table file and return
  exit_select:
    if (indexUsed)  
      treeindex.close();
    rf.close();
    return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  RC rc = 0;  //any potential error
  // if recordfile called "(table).tbl" doesn't exist
    // create table file as a RecordFile instance
  struct stat buffer;
  string recordfile = table + ".tbl";
  fstream fs;
  fs.open(loadfile.c_str(), fstream::in); // open loadfile

  BTreeIndex treeindex;

  // no need to check whether recordfile exists (the open() function of RecordFile helps with this)
  RecordFile rf = RecordFile(recordfile, 'w');
  string line;
  int key;
  string value;
  RecordId rId;

  if (index) {
    string name = table + ".idx";
    treeindex.open(name, 'w'); //get rootpid and treeheight if it already exists

    while (getline(fs, line)) {
      parseLoadLine(line, key, value);
      if (rf.append(key, value, rId)) //gets location of pageid and slotid and store it into rId
        return RC_FILE_WRITE_FAILED;
      if (treeindex.insert(key, rId)) //insert key rId tuple into table
        return RC_FILE_WRITE_FAILED;
    }

    treeindex.close();

  } else {
    while (getline(fs, line)) {
      // cout << line << endl;
      parseLoadLine(line, key, value);
      rc = rf.append(key, value, rId);
    }
  }
  
  rf.close();
  fs.close();

  return rc;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
