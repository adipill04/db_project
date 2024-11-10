// Names + IDs:
// Jeffrey Wang, jjwang8@wisc.edu, 9084378281
// Aarya Deshpande, avdeshpande4@wisc.edu, 9083817461
//
// Purpose:
// This file contains the main implementation of the Buf Manager class. It is a
// buffer manager that manages the buffer pool. It is responsible for
// reading and writing pages to and from the buffer pool and disk.
// 

#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    //memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) {
    bufTable[i] = BufDesc(); 
    }
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

/**
 * Finds an available frame in the buffer pool using clock algorithm and 
 * returns the frame number and the status.
 * 
 * Scans through the buffer pool twice searching for:
 * 1. Invalid frames (no page is loaded in the frame)
 * 2. Valid frames that are:
 *    - Not referenced (refbit = false)
 *    - Not pinned (pinCnt = 0)
 *    - Can be safely evicted (i.e., no Status error)
 * 
 * If a dirty frame (refbit = true, pinCnt = 0) is found, writes it back to 
 * disk before reuse (updating diskwrites statistics), removes old page 
 * mapping from hash table, clears the frame, and returns the frame number 
 * as the frame for replacement (selection) to the caller function.
 * 
 * @param frame - output parameter containing selected frame number for new page
 * @return Status codes:
 *         OK - if a frame is found
 *         UNIXERR - error writing dirty page to disk
 *         HASHTBLERROR - error remove old page from hash table
 *         BUFFEREXCEEDED - no frames available (all pinned) after two passes
 */
const Status BufMgr::allocBuf(int & frame) 
{
    for (int i = 0; i < numBufs*2;i++){ // two passes to find frame for new page
        advanceClock();
        if (bufTable[clockHand].valid == false){ // returns frame's number if invalid
            // bufTable[clockHand].Set();
            bufTable[clockHand].Clear();
            frame = clockHand;
            bufStats.accesses++;
            return OK;
        } else { // else, frame is valid and is checked if page can be evicted
            if (bufTable[clockHand].refbit == true){
                bufTable[clockHand].refbit = false;
                continue;
            }
            if (bufTable[clockHand].pinCnt > 0){
                continue;
            } // if not recently accessed (refbit = false) and no active users (pinCnt = 0),
              // frame is cleaned up and is selected as the candidate for new page
            if (bufTable[clockHand].dirty == true){ // cleaning: saves any changes to disk
                if (bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, bufPool+clockHand) != OK){
                    bufStats.diskwrites++;
                    return UNIXERR;
                }
            }
            // cleaning: removed from hash table (regardless of whether it was dirty or not)
            if (hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo) == HASHTBLERROR){
                return HASHTBLERROR;
            }
            // cleaning: clears frame, candidate now selected for new page
            bufTable[clockHand].Clear();
            frame = clockHand;
            bufStats.accesses++;
            return OK;
        }
    }
    return BUFFEREXCEEDED;
}

/**
 * Reads a page from disk into buffer pool.
 * 
 * First checks if page is already in buffer pool via hash table lookup:
 * - If found: increments pin count and sets reference bit (to indicate access + usage)
 * - If not found:
 *   1. Gets a frame using allocBuf
 *   2. Reads page from disk into frame
 *   3. Inserts page into hash table, updates buffer metadata, 
 *      and marks first ever access/usage with Set function
 * 
 * @param file - file containing the page to be read
 * @param PageNo - page number to read from file
 * @param page – output parameter that will contain pointer to page in buffer
 * @return Status codes:
 *         OK - successfully read page into buffer pool
 *         UNIXERR - Unix error saving old page to disk for replacement
 *         BUFFEREXCEEDED - no frames available (all pinned) after two passes
 *         HASHTBLERROR - error updating hash table with new page
 */
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    // checks if page is already in buffer pool via hash table lookup
    int frame_num = 0;
    Status stat = hashTable->lookup(file, PageNo, frame_num);

    if (stat == HASHNOTFOUND){ // page not found in buffer pool, attempts to 
        // find frame for new page (allocBuf), read page into it (readPage), 
        // update hash table (insert), and mark first ever access/usage (Set)
        Status alloc_message = allocBuf(frame_num);
        if (alloc_message == UNIXERR){ // couldn't save old page to disk for replacement
            return UNIXERR;
        } 
        if (alloc_message == BUFFEREXCEEDED){ // buffer full, all frames in use
            return BUFFEREXCEEDED;
        }
        if (file->readPage(PageNo, bufPool + frame_num) != OK){
            return UNIXERR;
        }
        bufStats.diskreads++;
        if (hashTable->insert(file, PageNo, frame_num) == HASHTBLERROR){
            return HASHTBLERROR;
        }
        bufTable[frame_num].Set(file, PageNo);
        page = bufPool + frame_num;
    } else { // page found in buffer pool, marks additional access/usage
        bufTable[frame_num].refbit = true;
        bufTable[frame_num].pinCnt++;
        page = bufPool + frame_num;
    }
    return OK; // page successfully read and accessed
}

/**
 * Decrements the pin count for a page in the buffer pool, such as when 
 * a user makes changes to a page in the buffer pool (or has not changed it) 
 * and is done using the page.
 * 
 * Looks up the frame containing the specified page and:
 * - Decrements frame's pin count if page is currently pinned
 * - Updates frame's dirty flag if user made changes to the page (per dirty argument)
 * 
 * @param file - file containing the page to be unpinned
 * @param PageNo - page number to unpin
 * @param dirty - indicates whether the page has been modified during use and ought
 * to be marked dirty in the buffer pool
 * @return Status codes:
 *          OK - successfully unpinned page
 *          HASHNOTFOUND - page not found in buffer pool
 *          PAGENOTPINNED - page was not pinned to begin with
 */
const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    //get frame using file, PageNo
    int frame;
    hashTable->lookup(file, PageNo, frame);

    //not found
    if (frame == HASHNOTFOUND){
        return HASHNOTFOUND;
    }
    
    //unpin if needed
    if (bufTable[frame].pinCnt > 0){
       bufTable[frame].pinCnt--;
       if (dirty) bufTable[frame].dirty=dirty;
    }
    else{
        return PAGENOTPINNED;
    }

    return OK;

}

/**
 * Creates a new, empty page in the specified file on disk and immediately 
 * reads it into the buffer pool ready for writing, with a pointer to the 
 * page in the buffer pool returned.
 * 
 * 1. Allocates new page on disk
 * 2. Allocates frame in buffer pool for new page
 * 
 * @param file - file to create page in
 * @param pageNo - page number to create
 * @param page - output parameter that will contain pointer to page in buffer
 * @return Status codes:
 *         UNIXERR - Unix error creating new page on disk
 *         HASHTBLERROR - error updating hash table with new page
 *         OK - successfully created page in disk and added to buffer pool
 */
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    // allocate new page on disk, updating disk read statistics
    if (file->allocatePage(pageNo) == UNIXERR){
        return UNIXERR;
    }
    bufStats.diskreads++;

    // allocate a frame for the page in the buffer pool, updating hash table
    int frame = 0;
    Status allocBuf_stat = allocBuf(frame);
    if (allocBuf_stat != OK){
        return allocBuf_stat;
    }
    if (hashTable->insert(file, pageNo, frame) == HASHTBLERROR){
        return HASHTBLERROR;
    }
    bufTable[frame].Set(file, pageNo); // updates buffer table with new, empty page
    page = bufPool + frame;
    return OK; // successfully created new page in disk, ready for writing in buffer pool

}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


