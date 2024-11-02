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


const Status BufMgr::allocBuf(int & frame) 
{
    for (int i = 0; i < numBufs*2;i++){
        advanceClock();
        if (bufTable[clockHand].valid == false){
            // bufTable[clockHand].Set();
            bufTable[clockHand].Clear();
            frame = clockHand;
            bufStats.accesses++;
            return OK;
        } else {
            if (bufTable[clockHand].refbit == true){
                bufTable[clockHand].refbit = false;
                continue;
            }
            if (bufTable[clockHand].pinCnt > 0){
                continue;
            }
            if (bufTable[clockHand].dirty == true){ 
                if (bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, bufPool+clockHand) != OK){
                    bufStats.diskwrites++;
                    return UNIXERR;
                }
            }
            if (hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo) == HASHTBLERROR){
                return HASHTBLERROR;
            }
            bufTable[clockHand].Clear();
            frame = clockHand;
            bufStats.accesses++;
            return OK;
        }
    }
    return BUFFEREXCEEDED;
}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    
    int frame_num = 0;
    Status stat = hashTable->lookup(file, PageNo, frame_num);

    if (stat == HASHNOTFOUND){
        Status alloc_message = allocBuf(frame_num);
        if (alloc_message == UNIXERR){
            return UNIXERR;
        } 
        if (alloc_message == BUFFEREXCEEDED){
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
    } else {
        bufTable[frame_num].refbit = true;
        bufTable[frame_num].pinCnt++;
        page = bufPool + frame_num;
    }
    return OK;
}


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

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    if (file->allocatePage(pageNo) == UNIXERR){
        return UNIXERR;
    }
    bufStats.diskreads++;
    int frame = 0;
    Status allocBuf_stat = allocBuf(frame);
    if (allocBuf_stat != OK){
        return allocBuf_stat;
    }
    if (hashTable->insert(file, pageNo, frame) == HASHTBLERROR){
        return HASHTBLERROR;
    }
    bufTable[frame].Set(file, pageNo);
    page = bufPool + frame;
    return OK;

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


