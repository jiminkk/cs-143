/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"
#include <string.h>

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    rootPid = -1;
    treeHeight = 0;
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
    RC error_code;

    error_code = pf.open(indexname, mode);

    if (error_code != 0) {
    	return error_code;
    }

    error_code = pf.read(0, buffer); //pageid = 0 is for our stuff that we want to store on disk like rootpid and height info

    if (error_code != 0){
    	return error_code;
    }

    PageId id;
    int height;

    memcpy(&id, buffer, sizeof(PageId)); //get id from disk if it exists
    memcpy(&height, buffer + sizeof(PageId), sizeof(int)); //get height from disk if it exists

    if (height > treeHeight) { //change treeHeight if necessary
    	treeHeight = height;
    }

    if (id > rootPid) { //update the rootPid if necessary
    	rootPid = id;
    }

    return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
    RC error_code;

    memcpy(buffer, &rootPid, sizeof(PageId)); //store rootpid into buffer
    memcpy(buffer + sizeof(PageId), &treeHeight, sizeof(int)); //store height into buffer

    error_code = pf.write(0, buffer); //write buffer to disk (saves rootPid and treeHeight values)

    if (error_code != 0) {
    	return error_code;
    }

    error_code = pf.close();
    return error_code;
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
    if (key < 0) {
        return RC_INVALID_ATTRIBUTE;
    }

    RC error_code;

    if (treeHeight == 0) { //no B+ tree has been implemented yet
        BTLeafNode leaf_node;
        leaf_node.insert(key, rid);
        treeHeight++;

        rootPid = 1; //I think this is right? We want to start pid from 1, leaving pid of 0 for addition helper stuff like getting values of rootpid and treeheight from disk.

        if (pf.endPid() != 0) { //Do I need this, if there are multiple pages?
            rootPid = pf.endPid();
        }

        return leaf_node.write(rootPid, pf); //write to disk
    }

    //we have to insert somewhere in the tree

    //call insertHelper(...)


    return 0;
}

RC BTreeIndex::insertHelper(int key, const RecordId& rid, PageId pid, int height) //i'm not sure if I'm missing any paramters
{
    return 0;
}

/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf node where searchKey may exist. If an index entry with
 * searchKey exists in the leaf node, set IndexCursor to its location
 * (i.e., IndexCursor.pid = PageId of the leaf node, and
 * IndexCursor.eid = the searchKey index entry number.) and return 0.
 * If not, set IndexCursor.pid = PageId of the leaf node and
 * IndexCursor.eid = the index entry immediately after the largest
 * index key that is smaller than searchKey, and return the error
 * code RC_NO_SUCH_RECORD.
 * Using the returned "IndexCursor", you will have to call readForward()
 * to retrieve the actual (key, rid) pair from the index.
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
    RC error_code;
    BTLeafNode leaf_node;
    BTNonLeafNode nonleaf_node;
    int height = 1; //start from root node

    int eid;
    PageId pid = rootPid;

    for (height; height <= treeHeight; height++) { //start from root make way down the tree to leafnode
        error_code = nonleaf_node.read(pid, pf); //read in the buffer from disk
        /*if (error_code != 0) {
            return error_code;
        }*/

        error_code = nonleaf_node.locateChildPtr(searchKey, pid); //locate the childptr that we should travel to next, next pid is stored in pid
        if (error_code != 0) {
            return error_code;
        }
    }

    //we have reached leafnode level
    error_code = leaf_node.read(pid, pf);
    /*if (error_code != 0) {
        return error_code;
    }*/

    error_code = leaf_node.locate(searchKey, eid); //locate the index entry from the current leaf

    cursor.pid = pid;
    cursor.eid = eid;


    if (error_code != 0) {
        return error_code; //returns RC_NO_SUCH_RECORD from the locate function of leafnode if the specific key cannot be found
    }

    return 0;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
    RC error_code;
    PageId pid = cursor.pid;
    int eid = cursor.eid;

    BTLeafNode leaf_node;
    error_code = leaf_node.read(pid, pf);

    error_code = leaf_node.readEntry(eid, key, rid); //use leafnode's readEntry function to read in the key and rid given the eid

    if (error_code != 0) {
        return error_code;
    }

    if (eid >= leaf_node.getKeyCount() - 1) { //if eid is greater than or equal to the max amount of keys, then we would consider this an overflow (-1 is for converting from 1 start to 0 start)
        cursor.eid = 0; //reset index entry to 0 for neighboring leafnode
        cursor.pid = leaf_node.getNextNodePtr(); //change to next neighboring node if we exceeded max on current node
        return 0;
    } 

    cursor.eid = cursor.eid + 1; //move forward the cursor to next entry
    cursor.pid = pid; //we would still be in the same page, so pageid is still the same

    return 0;
}
