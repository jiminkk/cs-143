#include "BTreeNode.h"
#include <iostream>
#include <string.h>

using namespace std;

BTLeafNode::BTLeafNode() {
	memset(buffer, 0, PageFile::PAGE_SIZE); //clear buffer, set everything to -1
	tupleSize = sizeof(RecordId) + sizeof(int); //set private variable of tuplesize to be the size of both the key (int) and the record (pageid, slotid)
	numTotalTuples = (PageFile::PAGE_SIZE - sizeof(PageId))/tupleSize; //getting the number of tuples limit of the page. It subtracts the PageID, since the PageID uses the last 4 bytes of the page
	typeCount = 3;
}
/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{ 
	return pf.read(pid, buffer); //use PageFile's read function to read the content from the page and store in buffer
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{ 
	return pf.write(pid, buffer); //write the buffer's contents into the Pagefile with its own write function
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{ 
	int keyCount = 0;
	int * tempbuffer = (int *) buffer; //cast char* to int*
	int i = 0;
	while (i < numTotalTuples) {
		int key = *tempbuffer;
		if (key != 0) { //check if key is empty or not
			keyCount++;
		} else {
			return keyCount;
		}
		tempbuffer += typeCount; //increment tempbuffer to point to next position of key
		i++;
	}
	return keyCount;
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{ 
	
	int keyCount = getKeyCount();
	if (keyCount >= numTotalTuples) { //if node is already full
		return RC_NODE_FULL;
	} 

	int * tempbuffer = (int *) buffer;
	int insertLocation = 0;
	locate(key, insertLocation);

	int i = insertLocation * typeCount; // location of where we want to insert
	int j = typeCount * keyCount - 1; 	// ?? instead of numTotalTuples, maybe should be typeCount? ?? ? ?? ? 
	while ( j >= i) {
		tempbuffer[j + 3] = tempbuffer[j]; //move the tuples to the next slot (adjust array so that we can insert our tuple in slot i)
		j--;								// shouldn't this move by 3 tuples? ???
	}

	tempbuffer[i] = key; // insert the key into the buffer
	tempbuffer[i+1] = rid.pid; // insert the pid into the buffer
	tempbuffer[i+2] = rid.sid; //insert the slotid into the buffer

	memcpy(buffer, tempbuffer, PageFile::PAGE_SIZE); // insert back into buffer

	return 0;

}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{ 
	int keyCount = getKeyCount();
	if (keyCount < numTotalTuples) { //if node is not full, then we don't want to split
		return RC_INVALID_ATTRIBUTE;
	} 


	if (sibling.getKeyCount() != 0) {
		return RC_INVALID_ATTRIBUTE;
	}

	int split = (getKeyCount() + 1)/2; //get half-number to use as split
	int indexsplit = split * tupleSize; //number of bytes that will roughly go into each buffer

	char *tempbuffer = buffer + indexsplit;

	memcpy(sibling.buffer, tempbuffer, sizeof(tempbuffer)); //copy second half of buffer to sibling.
	memset(tempbuffer, 0, PageFile::PAGE_SIZE - sizeof(PageId)); //set second half of current buffer to null
	memcpy(&siblingKey, sibling.buffer, sizeof(int)); //set the first key in the sibling node after the split

	if (key < siblingKey) { //insert in first half if key is less than start of second half
		insert(key, rid);
	} else {
		sibling.insert(key, rid);
	}

	sibling.setNextNodePtr(getNextNodePtr()); //set the pid of the next sibling's node to be the current pid's next

	return 0; 

}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{ 
	int numKeys = getKeyCount();

	int * tempbuffer = (int *) buffer;
	int i = 0;
	while (i < numKeys ) {
		int key = *tempbuffer; //set key to be key in buffer
		if (key == searchKey) {
			eid = i; //index entry number
			return 0;
		} else if (key > searchKey) {
			eid = i;
			return RC_NO_SUCH_RECORD; 
		}
		tempbuffer += typeCount; //change buffer to next key position
		i++;
	}

	return RC_NO_SUCH_RECORD; 

}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{ 
	if (eid < 0 || eid >= getKeyCount()) {
		return RC_NO_SUCH_RECORD;
	}
	int * tempbuffer = (int *) buffer;
	tempbuffer += (eid*typeCount); //move to correction position in array (from index)
	key = *tempbuffer; //get the key
	rid.pid = *(tempbuffer + 1); //get the pid for RecordId
	rid.sid = *(tempbuffer + 2); //get the slotid for RecordId
	return 0; 

}

/*
 * Return the pid of the next sibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{ 
	PageId pid = 0;
	int * tempbuffer = (int *) buffer;
	tempbuffer += (numTotalTuples * typeCount);
	pid = *tempbuffer;
	return pid;
}

/*
 * Set the pid of the next sibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{ 
	if (pid < 0) {
		return RC_INVALID_PID; 
	}
	int * tempbuffer = (int *) buffer;
	tempbuffer[numTotalTuples * typeCount] = pid;
	memcpy(buffer, tempbuffer, PageFile::PAGE_SIZE); 
	return 0; 
}

/*
	BTNonLeafNode Constructor
*/
BTNonLeafNode::BTNonLeafNode() {
	memset(buffer, 0, PageFile::PAGE_SIZE); //clear buffer, set everything to -1
	pairSize = sizeof(PageId) + sizeof(int);
	numTotalKeys = (PageFile::PAGE_SIZE - sizeof(PageId))/pairSize; //# of keys limit of each page, by subtracting the first pageId for simpler calculation
	typeCount = 2;
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{
	return pf.read(pid, buffer); //use PageFile's read function to read the content from the page and store in buffer
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{
	return pf.write(pid, buffer); //write the buffer's contents into the Pagefile with its own write function
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{
	int keyCount = 0;
	int * tempbuffer = (int *) buffer; //cast char* to int*
	int i = 0;
	while (i < numTotalKeys) {
		int key = *tempbuffer;
		if (key != 0) { //check if key is empty or not
			keyCount++;
		} else {
			return keyCount;
		}
		tempbuffer += typeCount; //increment tempbuffer to point to next position of key
		i++;
	}
	return keyCount;
}

/*
	one difference from BTLeafNode: key starts at index after PageId
	eid = __th key, starting from 0
*/
RC BTNonLeafNode::locate(int searchKey, int& eid)
{ 
	int numKeys = getKeyCount();

	int * tempbuffer = (int *) buffer;
	tempbuffer++;	// take account of the size of first PageId
	int i = 0;
	while (i < numKeys ) {
		int key = *tempbuffer; //set key to be key in buffer
		if (key == searchKey) {
			eid = i; //index entry number
			return 0;
		} else if (key > searchKey) {	//if searchKey is smaller than current key, return eid of current key
			eid = i;
			return RC_NO_SUCH_RECORD; 
		}
		tempbuffer += typeCount; //change buffer to next key position
		i++;
	}
	return RC_NO_SUCH_RECORD; 
}

/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{
	int keyCount = getKeyCount();
	if (keyCount >= numTotalKeys) { //check if node is already full
		return RC_NODE_FULL;
	}

	int * tempbuffer = (int *) buffer;
	int insertLocation = 0;
	locate(key, insertLocation);	//get correct insertLocation for key

	int i = insertLocation * typeCount+1;	//+1 to account for the 1st index which is a single pageId
	int j = typeCount * keyCount;	//the next index right after the last of existing keys, no need for -1 here (since 1st index is pid, not paired with any key)
	while (i <= j) {
		tempbuffer[j+2] = tempbuffer[j]; //move the tuples to the next slot (adjust array so that we can insert our tuple in slot i)
		j--;
 	}

 	tempbuffer[i] = key;
 	tempbuffer[i+1] = pid;
 	
 	memcpy(buffer, tempbuffer, PageFile::PAGE_SIZE); // insert back into buffer

	return 0;
}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{ return 0; }

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{ return 0; }

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{ return 0; }
