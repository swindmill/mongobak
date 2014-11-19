__author__ = 'Sterling Windmill'

import hashlib
import zlib
import time
from pymongo import Connection
from pymongo import (ASCENDING, DESCENDING)
from bson.binary import Binary
from bson.objectid import ObjectId
import sys
import base64
from Queue import Queue
from threading import Thread

#import cStringIO
import datetime
import platform
import os

from progressbar import ProgressBar, Percentage, Bar, ETA, FileTransferSpeed, RotatingMarker

mongoblocks = []

seenhashes = set()

blocksize = 1024 * 1024

#chunksize = blocksize * 1

mongohost = '10.8.0.1'

homedir = os.path.expanduser('~')

#seenhashes = set()

files_id = ObjectId()

mongoconn = Connection(host = mongohost, port = 27017)


def main(filein):
    
    def run():

        while True:
            
    
            inblock, blocknum = q.get()
        
            mongohashes = mongoconn.circuitbackup.hashes
        
            mongochunks = mongoconn.circuitbackup.chunks
        
            mongochunks.ensure_index([("files_id", ASCENDING), ("n", ASCENDING)], unique=True)
        
            #workblocks = cStringIO.StringIO(inblock)
        
            #blocknum = workunit[1]
        
            #blocks = workunit[2]
        
            #blockdict = {}
        
            #blocklist = []
        
        
            #for x in xrange(blocks):
        
            #workblock = workblocks.read(blocksize)
        
            blockhash = hashlib.sha256(inblock).digest()
        
            basehash = base64.b64encode(blockhash)
            
            mongochunks.insert({"files_id": files_id, "n": blocknum, "hash": Binary(blockhash, subtype=0)})
        
            
            if mongohashes.find({"_id": basehash}).count() == 0:
                
                outdict = {}
        
                outdict['_id'] = basehash
        
        
                compressedblock = zlib.compress(inblock)
        
                if len(compressedblock) < len(inblock):
                    outdict['c'] = "z"
                    outdict['d'] = Binary(compressedblock, subtype=0)
        
                else:
                    outdict['c'] = "n"
                    outdict['d'] = Binary(inblock, subtype=0)
                    
                                                
                mongohashes.insert(outdict)
                
                mongoblocks.append(1)
                
            q.task_done()



    def cleanquit():
        
        pbar.finish()

        print "waiting for queue"
        q.join()

        print str(block) + " blocks (" + str(block * blocksize) + " bytes) read in total"
        #print str(len(seenhashes)) + " unique blocks (" + str(len(seenhashes) * blocksize) + " bytes) seen"
        print str(sum(mongoblocks)) + " blocks written to database\n"
        sys.exit()
        
    def get_file_size(filename):
        "Get the file size by seeking at end"
        fd= os.open(filename, os.O_RDONLY)
        try:
            return os.lseek(fd, 0, os.SEEK_END)
        finally:
            os.close(fd)
            
    
    
    #if chunksize % blocksize:
    #    print "Error: chunksize must be a multiple of blocksize"
    #    sys.exit(1)
    
    filesize = get_file_size(filein)
    

    mongodb = mongoconn.circuitbackup
    
    mongofiles = mongodb.files

    mongofiles.ensure_index([("fileName", ASCENDING), ("uploadDate", DESCENDING)])
    
    mongofiles.insert({"_id": files_id, "chunkSize": blocksize, "length": filesize, "uploadDate": datetime.datetime.utcnow(), "complete": False, "fileName": filein, "hostName": platform.node()})


    q = Queue(16)
    
    for i in xrange(8):
        t = Thread(target=run)
        t.daemon = True
        t.start()
        
    start = time.time()
    
    

    
    widgets = ['Progress: ', Percentage(), ' ', Bar(marker=RotatingMarker()),
               ' ', ETA(), ' ', FileTransferSpeed()]
    
    pbar = ProgressBar(widgets=widgets, maxval=filesize).start()
        
    fin = open(filein, 'rb')
    
    block = 0
    
    while True:
        
        try:
            
            inblock = fin.read(blocksize)
            if not inblock:
                break
            else:
                
                #inblocksize = len(inblock)
                
                #if inblocksize == chunksize or inblocksize % blocksize == 0:
                #    blocks = inblocksize / blocksize
                #elif inblocksize > blocksize:
                #    blocks = inblocksize / blocksize + 1
                #else:
                #    blocks = 1
                    
                q.put((inblock, block))
                    
                #pool.apply_async(run, (queue,), callback=runcallback)


                #if block != 0 and block % (chunksize / blocksize * 10) == 0:
                    
                pbar.update(block * blocksize)
    
                block += 1
        
        except KeyboardInterrupt:
            print "KeyboardInterrupt"
            cleanquit()
            
    
    
    
    pbar.finish()
    print "waiting for queue"
    
    q.join()

    mongofiles.update({"_id": files_id}, { "$set" : { "complete" : True } })
    
    mongoconn.end_request()
    
    
    print str(block) + " blocks read from input"
    
    #print str(len(seenhashes)) + " unique blocks (" + str(len(seenhashes) * blocksize) + " bytes) seen"
        
    print str(sum(mongoblocks)) + " blocks written to database"
    
    #print "seen hashes set consumed " + str(sys.getsizeof(seenhashes)) + " bytes"
    
    end = time.time()
     
    elapsed = end - start
    
    print "elapsed time" + str(elapsed)


if __name__ == '__main__':
    
    if len(sys.argv) != 2:
        print "Need one argument (filename)"
        sys.exit(1)
    else:
        sys.exit(main(sys.argv[1]))
