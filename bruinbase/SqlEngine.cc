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
  RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning
  BTreeIndex treeindex;

  RC     rc;
  int    key;     
  string value;
  int    count;
  int    diff;

  // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }

  SelCond condition;
  int val;
  int min = -1;
  int max = -1;
  int ge = 0;
  int le = 0;
  int keyIsEQ = -1;
  string valIsEQ = "";
  bool isNE; //checks if condition is not equals, if so, we don't want to run it through the index (as mentioned in the spec)

  for (unsigned i = 0; i < cond.size(); i++) { //iterate through all of the conditions
    condition = cond[i];
    val = atoi(cond[i].value);
    switch(condition.attr) {
      case 1: //if attribute is a key (all operators can apply here)
        switch (condition.comp) {
          case SelCond::EQ:
            keyIsEQ = val;
            break;
          case SelCond::NE:
            isNE = true;
            break;
          case SelCond::LT:
            if (max == -1 || max >= val) {
              max = val;
            }
            break;
          case SelCond::GT:
            if (min == -1 || min <= val) {
              min = val;
            }
            break;
          case SelCond::LE:
            if (max == -1 || max > val) {
              max = val;
            }
            le = 1;
            break;
          case SelCond::GE:
            if (min == -1 || min < val) {
              min = val;
            }
            ge = 1;
            break;
        }
      case 2: //if attribute is a value (only eq operator applies here since we would be comparing value to value)
        if (condition.comp == SelCond::EQ) {
          valIsEQ = cond[i].value;
        }
    }
  }

  if (isNE) {
    goto not_index; //if we are dealing with a not equals, we want to do a full scan of table (and not from index tree)
  }

  if (max < min && max != -1) { //What happens here? This should be invalid since the query range for max should never be less than min. Do we go to notindex?
    goto exit_select;
  }


  if (treeindex.open(table + "idx", 'r') == 0) { //there is a B+ tree for this query
    IndexCursor cursor;

    if (keyIsEQ != -1) { //if we are trying to find a particular key
      indextree.locate(keyIsEQ, cursor);
    } else if (min != -1 && ge == 1 ) {
      indextree.locate(min, cursor); //>=
    } else if (min != -1 && ge == 0) {
      indextree.locate(min + 1, cursor); //>
    } else {
      indextree.locate(0, cursor); //start from very first element.
    }

  }

  not_index: //come here if we don't want to go through index tree
  // scan the table file from the beginning
  rid.pid = rid.sid = 0;
  count = 0;
  while (rid < rf.endRid()) {
    // read the tuple
    if ((rc = rf.read(rid, key, value)) < 0) {
      fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
      goto exit_select;
    }

    // check the conditions on the tuple
    for (unsigned i = 0; i < cond.size(); i++) {
      // compute the difference between the tuple value and the condition value
      switch (cond[i].attr) {
      case 1:
	diff = key - atoi(cond[i].value);
	break;
      case 2:
	diff = strcmp(value.c_str(), cond[i].value);
	break;
      }

      // skip the tuple if any condition is not met
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

    // the condition is met for the tuple. 
    // increase matching tuple counter
    count++;

    // print the tuple 
    switch (attr) {
    case 1:  // SELECT key
      fprintf(stdout, "%d\n", key);
      break;
    case 2:  // SELECT value
      fprintf(stdout, "%s\n", value.c_str());
      break;
    case 3:  // SELECT *
      fprintf(stdout, "%d '%s'\n", key, value.c_str());
      break;
    }

    // move to the next tuple
    next_tuple:
    ++rid;
  }

  // print matching tuple count if "select count(*)"
  if (attr == 4) {
    fprintf(stdout, "%d\n", count);
  }
  rc = 0;

  // close the table file and return
  exit_select:
  rf.close();
  return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
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
      rf.append(key, value, rId); //gets location of pageid and slotid and store it into rId
      treeindex.insert(key, rId); //insert key rId tuple into table
    }

    treeindex.close();

  } else {
    while (getline(fs, line)) {
      // cout << line << endl;
      parseLoadLine(line, key, value);
      rf.append(key, value, rId);
    }
  }
  

  fs.close();

  return 0;
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
