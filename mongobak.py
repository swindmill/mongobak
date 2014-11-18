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
import Queue
import threading
import cStringIO
import datetime
import platform
import os

from progressbar import ProgressBar, Percentage, Bar, ETA, FileTransferSpeed, RotatingMarker

mongoblocks = []

blocksize = 256 * 1024

chunksize = blocksize * 32

mongohost = '10.8.0.1'

homedir = os.path.expanduser('~')

seenhashes = set()

files_id = ObjectId()


class DiskReadThread(threading.Thread):

    def __init__ (self, inqueue, mongoconn):
        self.inqueue = inqueue
        self.mongoconn = mongoconn
        threading.Thread.__init__(self)

    def run (self):

        while True:

            workunit = self.inqueue.get()

            mongoconn = self.mongoconn.circuitbackup

            mongohashes = mongoconn.hashes

            mongochunks = mongoconn.chunks

            mongochunks.ensure_index([("files_id", ASCENDING), ("n", ASCENDING)], unique=True)

            workblocks = cStringIO.StringIO(workunit[0])

            blocknum = workunit[1]

            blocks = workunit[2]

            blockdict = {}

            blocklist = []

            for x in xrange(blocks):

                workblock = workblocks.read(blocksize)

                blockhash = hashlib.sha256(workblock).digest()

                basehash = base64.b64encode(blockhash)

                blocklist.append({"files_id": files_id, "n": blocknum, "hash": Binary(blockhash, subtype=0)})

                if not basehash in seenhashes:

                    seenhashes.add(basehash)

                    blockdict[basehash] = workblock

                blocknum += 1


            workblocks.close()

            foundhashset = set()

            if blockdict:

                if mongohashes.find({"_id": { "$in" : blockdict.keys() }}).count() == len(blockdict):
                    foundhashset = blockdict.keys()
                else:
                #if True:

                    outdictlist = []

                    for returndict in mongohashes.find({"_id": { "$in" : blockdict.keys() }}, {"_id": "1"} ):
                        foundhashset.add(returndict['_id'])


                    viewkeys = blockdict.viewkeys()

                    for outhash in (viewkeys - foundhashset):

                        outdict = {}

                        outdict['_id'] = outhash

                        outblock = blockdict[outhash]

                        compressedblock = zlib.compress(outblock)

                        if len(compressedblock) < len(outblock):
                            outdict['c'] = "z"
                            outdict['d'] = Binary(compressedblock, subtype=0)

                        else:
                            outdict['c'] = "n"
                            outdict['d'] = Binary(outblock, subtype=0)

                        outdictlist.append(outdict)

                    if outdictlist:
                        mongoblocks.append(len(outdictlist))

                        mongohashes.insert(outdictlist)

            mongochunks.insert(blocklist)

            self.inqueue.task_done()

class CustomProgressBar(ProgressBar):

    def _need_update(self):

        return True


def main(filein):


    def cleanquit():

        pbar.finish()

        print "waiting for queue"
        inqueue.join()

        print str(block) + " blocks (" + str(block * blocksize) + " bytes) read in total"
        print str(len(seenhashes)) + " unique blocks (" + str(len(seenhashes) * blocksize) + " bytes) seen"
        print str(sum(mongoblocks)) + " blocks written to database\n"
        sys.exit()

    def get_file_size(filename):
        "Get the file size by seeking at end"
        fd= os.open(filename, os.O_RDONLY)
        try:
            return os.lseek(fd, 0, os.SEEK_END)
        finally:
            os.close(fd)



    if chunksize % blocksize != 0:
        print "Error: chunksize must be a multiple of blocksize"
        sys.exit(1)

    filesize = get_file_size(filein)

    inqueue = Queue.Queue(10)

    mongoconn = Connection(host = mongohost, port = 27017)

    mongodb = mongoconn.circuitbackup

    mongofiles = mongodb.files

    mongofiles.ensure_index([("fileName", ASCENDING), ("uploadDate", DESCENDING)])

    mongofiles.insert({"_id": files_id, "chunkSize": blocksize, "length": filesize, "uploadDate": datetime.datetime.utcnow(), "complete": False, "fileName": filein, "hostName": platform.node()})


    start = time.time()

    for i in xrange(5):

        t = DiskReadThread(inqueue, mongoconn)
        t.daemon = True
        t.start()

    widgets = ['Progress: ', Percentage(), ' ', Bar(marker=RotatingMarker()),
               ' ', ETA(), ' ', FileTransferSpeed()]

    pbar = ProgressBar(widgets=widgets, maxval=filesize).start()

    fin = open(filein, 'rb')

    block = 0

    while True:

        try:

            inblock = fin.read(chunksize)
            if not inblock:
                break
            else:

                inblocksize = len(inblock)

                if inblocksize == chunksize or inblocksize % blocksize == 0:
                    blocks = inblocksize / blocksize
                elif inblocksize > blocksize:
                    blocks = inblocksize / blocksize + 1
                else:
                    blocks = 1

                inqueue.put((inblock, block, blocks))

                #if block != 0 and block % (chunksize / blocksize * 10) == 0:

                pbar.update(block * blocksize)

                block += blocks

        except KeyboardInterrupt:
            print "KeyboardInterrupt"
            cleanquit()




    pbar.finish()
    print "waiting for queue"
    inqueue.join()

    mongofiles.update({"_id": files_id}, { "$set" : { "complete" : True } })

    mongoconn.end_request()


    print str(block) + " blocks read from input"

    print str(len(seenhashes)) + " unique blocks (" + str(len(seenhashes) * blocksize) + " bytes) seen"

    print str(sum(mongoblocks)) + " blocks written to database"

    print "seen hashes set consumed " + str(sys.getsizeof(seenhashes)) + " bytes"

    end = time.time()

    elapsed = end - start

    print "elapsed time" + str(elapsed)


if __name__ == '__main__':

    if len(sys.argv) != 2:
        print "Need one argument (filename)"
        sys.exit(1)
    else:
        sys.exit(main(sys.argv[1]))
