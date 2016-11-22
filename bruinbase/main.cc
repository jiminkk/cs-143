/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include "BTreeIndex.h"
#include "BTreeNode.h"
#include "Bruinbase.h"
#include "SqlEngine.h"

#include <cstdio>
#include <iostream>
#include <string.h>
#include <assert.h>

using namespace std;

int main() {
	// printf("Starting BTLeafNode tests....\n");
	// BTLeafNode node;
	// int key1 = 99;
	// RecordId rid1;
	// rid1.pid = 90;
	// rid1.sid = 90;
	// RC rc1 = node.insert(key1, rid1);
	// if (rc1 != 0) {
	// 	cout << "error inserting key into node" << endl;
	// 	return 0;
	// }
	// int eid;
	// rc1 = node.locate(key1, eid);
	// if (rc1 != 0) {
	// 	cout << "error locating key in node" << endl;
	// 	return 0;
	// }
	// cout << endl;


	printf("Starting BTreeIndex tests....\n");

	BTreeIndex b;

	b.open("test.idx", 'w');

	// Key to insert
	int key;
	// RID to insert
	RecordId rid;
	// RC returned
	RC rc;
	// Index cursor returned
	IndexCursor ic;
	// Key to return
	int retKey;
	// RID to return
	RecordId retRid;

	// key = 1;
	// rid.pid = 1;
	// rid.sid = 1;
	// cout << "inserting key: " << key << endl;
	// rc = b.insert(key, rid);
	// if (rc != 0) {
	// 	cout << "Error inserting key:" << key << ", pid:" << rid.pid << ", sid:" << rid.sid << ", rc:" << rc << endl;
	// 	return 0;
	// }
	// cout << endl;
	// cout << "locating key: " << key << endl;
	// rc = b.locate(1, ic);
	// if (rc != 0) {
	// 	cout << "Error locating 1 after 1: " << rc;
	// 	return 0;
	// }

	// cout << endl;
	// key = 2;
	// rid.pid = 2;
	// rid.sid = 2;
	// cout << "inserting key: " << key << endl;
	// rc = b.insert(key, rid);

	// cout << endl;
	// cout << "locating key: " << key << endl;
	// rc = b.locate(2, ic);
	// if (rc != 0) {
	// 	cout << "Error locating 1 after 1: " << rc;
	// 	return 0;
	// }
	// bug: locate can't locate the last key inserted into treeIndex


  for (int i = 1; i < 5000; i++)
  {
     key = i;
     rid.pid = i;
     rid.sid = i;
     rc = b.insert(key, rid);
     if (rc != 0)
     {
        fprintf(stdout, "Error inserting %i, (%i %i): %i\n", key, rid.pid, rid.sid, rc);
        return 0;
     }

     for (int j = 1; j <= i; j += 85)
     {
        rc = b.locate(j, ic);
        if (rc != 0)
        {
          fprintf(stdout, "Error locating %i after %i: %i\n", j, i, rc);
          return 0;
        }
        rc = b.readForward(ic, retKey, retRid);
        if (rc != 0)
        {
          fprintf(stdout, "Error reading %i after %i: %i\n", j, i, rc);
          return 0;
        }
        if (retKey != j || retRid.pid != j || retRid.sid != j)
        {
          fprintf(stdout, "Error: inserted %i (%i %i), located %i (%i %i)\n", key, rid.pid, rid.sid, retKey, retRid.pid, retRid.sid);
          return 0;
        }
     }
  }
 /* run the SQL engine taking user commands from standard input (console).*/
  SqlEngine::run(stdin);

  return 0;
}
