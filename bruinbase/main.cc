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

using namespace std;

int main()
{
  // run the SQL engine taking user commands from standard input (console).
  //SqlEngine::run(stdin);

	/*BTLeafNode leaf;
	cout << leaf.getKeyCount() << "\n";

	RecordId rid;
	rid.pid = 1;
	rid.sid = 5;

	leaf.print();

	int insertLocation = -1;
	leaf.locate(2, insertLocation);
	cout << "insertLocation is " << insertLocation << "\n"; //index of found element

	leaf.insert(2, rid);
	cout << leaf.getKeyCount() << "\n"; //key count increases by 1

	int eid = -1;

	leaf.locate(2, eid);
	leaf.print();
	cout << "eid is " << eid << "\n"; //index of found element

	int key = -1;
	rid.pid = -1;
	rid.sid = -1;

	leaf.readEntry(eid, key, rid); //print correct key and rid values

	cout << "key is " << key << "\n";
	cout << "rid.pid is " << rid.pid << "\n";
	cout << "rid.sid is " << rid.sid << "\n";

	int insertLocation2;
	leaf.locate(3, insertLocation2);
	cout << "insertLocation2 is " << insertLocation2 << "\n"; //index of found element

	leaf.insert(3, rid);
	leaf.insert(4, rid);
	leaf.insert(6, rid);
	leaf.insert(5, rid);
	leaf.insert(1, rid);

	cout << "key count is " << leaf.getKeyCount() << "\n";

	leaf.print();

	for (int i=6; i < 86; i++) {
		leaf.insert(7, rid);
	}
	cout << "key count is " << leaf.getKeyCount() << "\n";

	leaf.readEntry(84, key, rid); //print correct key and rid values

	//cout << "key is " << key << "\n";
	//cout << "rid.pid is " << rid.pid << "\n";
	//cout << "rid.sid is " << rid.sid << "\n";
	//cout << "key count is " << leaf.getKeyCount() << "\n";

	//leaf.insert(1, rid);
	leaf.print();

	//leaf.locate(4, eid);
	//cout << "eid is " << eid << "\n"; //index of found element

	BTLeafNode leaf2;
	cout << "key count (leaf2) is " << leaf2.getKeyCount() << "\n";
	RecordId rid2 = {20, 40};
	int sibkey = -1;
	leaf.insertAndSplit(50, rid2, leaf2, sibkey);

	cout << "first key in leaf2 is " << sibkey << "\n";

	leaf2.print();
	//leaf.print();*/


  return 0;
}
