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

// const int MAX_NONLEAFDATA = 127;

void checkStatus(string msg, RC status) {
  
  if(status != 0){
    fprintf(stderr, "%s  | status = %d\n", msg.c_str(), status);
    //  exit(-1);
  }

}


RC BTNonLeafNode_insert_size_check(BTNonLeafNode& nl) {
   // Add some stuff to the empty BTNonLeafNode
  nl.insert(25, 30);
  nl.insert(35, 40);
  
  if(nl.getKeyCount() != 3)
    return -1;
  else return 0;

}


// RC BTNonLeafNode_insert_max_check(BTNonLeafNode& nl) {
  
//   // DEBUG
//   printf("maxnonleafdata = %d\n", MAX_NONLEAFDATA);


//   // Keep adding stuff until we get to the max
//   for(int i = 0; i <  MAX_NONLEAFDATA * 2; i++){
    
//     printf("Inserting key = %d\n", i);
//     RC status = nl.insert(i, i);

    

    
//     if(status != 0) 
//       return status;
//   }

//   // fprintf(stdout, "Size of nl now is = %d", nl.getKeyCount());
//   return 0;
// }

// RC BTNonLeafNode_search_test(BTNonLeafNode& nl) {
//   // By now we should have the keys 10 [23] 20 [25] 30 [35] 40
  
//   PageId pid;
  
  
//   // Look for the key in between
//   RC search_status = nl.locateChildPtr(24, pid);
//   checkStatus("Error in searching for 24!", search_status);
//   assert(pid == 20);
  

//   // Look for the key on match
//   search_status = nl.locateChildPtr(23, pid);
//   checkStatus("Error in searching for 23!", search_status);
//   assert(pid == 20);
  
  
//   // Look for the key on the first
//   search_status = nl.locateChildPtr(1, pid);
//   checkStatus("Error in searching for 1!", search_status);
//   assert(pid == 10);
  

//   // Look for the key on the last
//   search_status = nl.locateChildPtr(38, pid);
//   checkStatus("Error in searching for 38!", search_status);
//   assert(pid == 40);
  
  
//   fprintf(stdout, "search for 38: pid = %d", pid);
//   //  fprintf(stdout, "BTNonLeafNode Search Test Passed!");
  

// }

// void BTNonLeafNode_test(){
//   // Open up a new pagefile
//   PageFile pf;
  
//   RC open_status = pf.open("testing.pf", 'w');
//   checkStatus("Error in opening testing.pf in write mode!", open_status);
  
//   // Create and init an empty BTNonLeafNode
//   BTNonLeafNode nl;
 
  
//   RC root_status = nl.initializeRoot(10, -1, -1);
//   checkStatus("Error in initializing root!", root_status);
  
//   //RC insert_status = BTNonLeafNode_insert_size_check(nl);
//   //checkStatus("Error in inserting to BTNonLeafNode!", insert_status);
  

//   // Use a new page file
//   PageFile pf2;
//   open_status = pf2.open("testing2.pf", 'w');
//   checkStatus("Error in opening testing.pf in write mode!", open_status);
  
//   // Read the contents of pf2
//   //nl.read(1, pf2);

//   RC insert_max_status = BTNonLeafNode_insert_max_check(nl);
//   checkStatus("Error in inserting max nodes to BTNonLeafNode!", insert_max_status);
  

//   // // Now write to the page file
//   // nl.write(1, pf2);

//   fprintf(stdout, "Size of BTNonLeafNode = %d\n", nl.getKeyCount());
  
  

//   // // Search test
//   // // BTNonLeafNode_search_test(nl);
  
  
//   // // Write the empty leaf node to the page file.
  
//   // nl.write(pf.endPid(),pf);


//   // fprintf(stdout, "Writing to pid = %d", pf.endPid());

//   // Open up the NonLeafNode from the page file
  
//   // Go through its contents.
  


// }



void BTreeIndex_test() {
  
  BTreeIndex index;
  
  index.open("tree.index", 'w');
  //index.readMetadata();
  

  int numRecords = 12;
  // Go from [10 - 50]
  for (int i = 1; i < numRecords; i++) 
    {
      RecordId insertMe;
      insertMe.pid = i;
      insertMe.sid = i;
      
      int key_to_insert = i;
      
      RC insert_status = index.insert(key_to_insert, insertMe);
      checkStatus("Error in insert of BTreeIndex!", insert_status);
     

      printf("Inserting %d with pid = %d, sid = %d\n", key_to_insert, insertMe.pid, insertMe.sid);
    }
  
  
  // Now try to find all the records
  for (int j = 1; j < numRecords; j++) 
    {
      IndexCursor cursor;
      cursor.pid = -1;
      cursor.eid = -1;
      int key_to_search = j;
      RC search_status = index.locate(key_to_search, cursor);

      fprintf(stderr, "Checking key = %d\n.", j);
      checkStatus("Error in locate of BTreeIndex!", search_status);
      
      int key;
      RecordId rid;
      RC read_status = index.readForward(cursor, key, rid);
      //      checkStatus("Error in readForward of BTreeIndex!", read_status);
      
      printf("Search for j = %d returned pid = %d, sid = %d.\n", key_to_search, rid.pid, rid.sid);
      
    }
  

}//   int numRecords = 50;






// RC BTNonLeafNode_insertAndSplit_check() {
  
//   BTNonLeafNode nl;
  
//   nl.initializeRoot(10,-1,-1);
//   // DEBUG
//   printf("maxnonleafdata = %d\n", MAX_NONLEAFDATA);

//   // Keep adding stuff until we get to the max
//   for(int i = 0; i <  MAX_NONLEAFDATA; i++){
    
//     printf("Inserting key = %d\n", i);
//     RC status = nl.insert(i, i);
    
//     if(status != 0) 
//       return status;
//   }

  
//   BTNonLeafNode sibling;
//   int midKey;

//   // Now split this node
//   nl.insertAndSplit(10, 128, sibling, midKey);
  
//   fprintf(stdout, "Size of nl now is = %d\n", nl.getKeyCount());
//   fprintf(stdout, "Size of sibling now is = %d\n", sibling.getKeyCount());
  
//   fprintf(stdout, "Midkey is = %d\n",midKey);
    
//   return 0;
// }



int main()
{
  
//   //BTNonLeafNode_test();
   //BTreeIndex_test();

	RecordId rid;
	rid.pid = 2;
	rid.sid = 5;
	BTLeafNode leaf;

	for (int i = 0; i <= 20; i++) {
		leaf.insert(i, rid);
	}


	leaf.print();

	int eid;
	RC error_code = leaf.locate(15, eid);

	cout << "eid is " << eid <<  "\n";
  

//   // BTNonLeafNode_insertAndSplit_check();
 
//   return 0;
// }

// int main()
// {
// 	// BTLeafNode leaf;
// 	// cout << leaf.getKeyCount() << "\n";

// 	// RecordId rid;
// 	// rid.pid = 1;
// 	// rid.sid = 5;

// 	// leaf.print();

// 	// // int insertLocation = -1;
// 	// // leaf.locate(2, insertLocation);
// 	// // cout << "insertLocation is " << insertLocation << "\n"; //index of found element

// 	// leaf.insert(2, rid);
// 	// // cout << leaf.getKeyCount() << "\n"; //key count increases by 1

// 	// // int eid = -1;

// 	// // leaf.locate(2, eid);
// 	// // leaf.print();
// 	// // cout << "eid is " << eid << "\n"; //index of found element

// 	// // int key = -1;
// 	// // rid.pid = -1;
// 	// // rid.sid = -1;

// 	// // leaf.readEntry(eid, key, rid); //print correct key and rid values

// 	// // cout << "key is " << key << "\n";
// 	// // cout << "rid.pid is " << rid.pid << "\n";
// 	// // cout << "rid.sid is " << rid.sid << "\n";

// 	// // int insertLocation2;
// 	// // leaf.locate(3, insertLocation2);
// 	// // cout << "insertLocation2 is " << insertLocation2 << "\n"; //index of found element

// 	// leaf.insert(3, rid);
// 	// leaf.insert(4, rid);
// 	// leaf.insert(6, rid);
// 	// leaf.insert(5, rid);
// 	// leaf.insert(1, rid);

// 	// // cout << "key count is " << leaf.getKeyCount() << "\n";

// 	// // leaf.print();

// 	// for (int i=6; i < 86; i++) {
// 	// 	leaf.insert(7, rid);
// 	// }
// 	// cout << "key count is " << leaf.getKeyCount() << "\n";

// 	// // leaf.readEntry(84, key, rid); //print correct key and rid values

// 	// // //cout << "key is " << key << "\n";
// 	// // //cout << "rid.pid is " << rid.pid << "\n";
// 	// // //cout << "rid.sid is " << rid.sid << "\n";
// 	// // //cout << "key count is " << leaf.getKeyCount() << "\n";

// 	// leaf.insert(1, rid);
// 	// // leaf.print();

// 	// // //leaf.locate(4, eid);
// 	// // //cout << "eid is " << eid << "\n"; //index of found element

// 	// BTLeafNode leaf2;
// 	// cout << "key count (leaf2) is " << leaf2.getKeyCount() << "\n";
// 	// RecordId rid2 = {20, 40};
// 	// int sibkey = -1;
// 	// leaf.insertAndSplit(50, rid2, leaf2, sibkey);

// 	// cout << "first key in leaf2 is " << sibkey << "\n";

// 	// leaf2.print();
// 	// leaf.print();



// 	/* nonleaf node testing	*/
// 	BTNonLeafNode root;

// 	//Initialize root and verify number of keys
// 	root.initializeRoot(0,99,1);
// 	cout << "root node has numKeys: " << root.getKeyCount() << endl;
// 	root.print();

// 	//Insert to root and verify number of keys
// 	root.insert(999, 2);
// 	cout << "After insert, root node has numKeys: " << root.getKeyCount() << endl;
// 	root.print();

// 	//Try to insertAndSplit (this should fail)
// 	BTNonLeafNode sibling;
// 	int median = -1;
// 	root.insertAndSplit(9999, 3, sibling, median);
// 	cout << "After insertAndSplit, root node has numKeys (should be same): " << root.getKeyCount() << endl;
// 	cout << "Median: " << median << endl;

// 	//Check child pointers
// 	PageId rootPid = -1;

// 	root.locateChildPtr(50, rootPid);
// 	cout << "50 has child pointer: " << rootPid << endl;

// 	root.locateChildPtr(500, rootPid);
// 	cout << "500 has child pointer: " << rootPid << endl;
// 	rootPid = -1;
// 	root.locateChildPtr(5000, rootPid);
// 	cout << "5000 has child pointer: " << rootPid << endl;

// 	root.print();
// 	for(int k=0; k<5; k++)
// 		root.insert(9,4);

// 	cout << "root node has numKeys: " << root.getKeyCount() << endl;

// 	for(int k=0; k<121; k++)
// 		root.insert(9999,5);

// 	root.print();
// 	cout << "root node has numKeys: " << root.getKeyCount() << endl;


// 	root.insertAndSplit(127, 6, sibling, median);
// 	cout << "After insertAndSplit, root node has numKeys: " << root.getKeyCount() << endl;
// 	cout << "After insertAndSplit, sibling node has numKeys: " << sibling.getKeyCount() << endl;
// 	cout << "Median: " << median << endl;

// 	//Let's test for median more accurately
// 	BTNonLeafNode root2;
// 	root2.initializeRoot(0,2,1);
// 	for(int k=0; k<127; k++)
// 		root2.insert(2*(k+2),2);
// 	// root2.print();
// 	cout << median << endl;
// 	// Currently, root2 looks something like  (2, 4, 6, 8, ..., 126, 128, 130, ..., 252, 254)
// 	// Median should be 128 if we insert 127 [reason: adding 127 bumps 128 up into the middle key]
// 	// Median should be 129 if we insert 129 [reason: 129 itself becomes the middle most key]
// 	// Median should be 130 if we insert 131 [reason: adding 131 bumps 130 down into the middle key]

// 	median = -1;
// 	BTNonLeafNode sibling2;
// 	root2.insertAndSplit(131, 6, sibling2, median); //Replace the key to be inserted with the numbers above to test
// 	cout << "After insertAndSplit, root2 node has numKeys: " << root2.getKeyCount() << endl;
// 	cout << "After insertAndSplit, sibling2 node has numKeys: " << sibling2.getKeyCount() << endl;
// 	cout << "Median: " << median << endl;
	

// 	/* TreeIndex Testing */
	int maxEid = (PageFile::PAGE_SIZE-sizeof(PageId))/(sizeof(RecordId)+sizeof(int)); //This produces 85

// 	PageFile pf;
// 	pf.open("test", 'w');
// 	cout << "pf.endPid() on initialization: " << pf.endPid() << endl;
// 	//check for endPid changes
// 	BTLeafNode thisLeaf;
// 	for(int i=0; i<85; i++)
// 		thisLeaf.insert(1, (RecordId) {1,1});
// 	cout << "thisLeaf has key count: " << thisLeaf.getKeyCount() << endl;
// 	// thisLeaf.print();
		
// 	//Try inserting leaf node
// 	//If succesful, write back into PageFile
// 	cout << "thisLeaf write: " << thisLeaf.write(1, pf) << endl;
// 	cout << "pf.endPid() after thisLeaf write: " << pf.endPid() << endl;
// 	// //Try inserting leaf node via splitting
// 	BTLeafNode anotherLeaf;
// 	int anotherKey;
// 	thisLeaf.insertAndSplit(2, (RecordId) {2,2}, anotherLeaf, anotherKey);
// 	cout << "thisLeaf has key count: " << thisLeaf.getKeyCount() << endl;
// 	cout << "anotherLeaf has key count: " << anotherLeaf.getKeyCount() << endl;
// 	cout << "pf.endPid() after insert and split: " << pf.endPid() << endl;
// 	//Write new contents into thisLeaf and anotherLeaf
// 	//Notice that anotherLeaf starts writing at the end of the last pid
// 	cout << "anotherLeaf write: " << anotherLeaf.write(pf.endPid(), pf) << endl;
// 	cout << "pf.endPid() after anotherLeaf write: " << pf.endPid() << endl;
// 	thisLeaf.setNextNodePtr(pf.endPid());
// 	cout << "pf.endPid() after setting thisLeaf's next node ptr: " << pf.endPid() << endl;
// 	pf.close();
// 	pf.open("test", 'r');
// 	BTLeafNode readTest;
// 	cout << "readTest: " << readTest.read(1, pf) << endl;
// 	BTLeafNode readTest2;
// 	cout << "readTest2: " << readTest2.read(2, pf) << endl;
// 	cout << "readTest has key count: " << readTest.getKeyCount() << endl;
// 	cout << "readTest2 has key count: " << readTest2.getKeyCount() << endl;



 /* run the SQL engine taking user commands from standard input (console).*/
  //SqlEngine::run(stdin);

  return 0;
}
