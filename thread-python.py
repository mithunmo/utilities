from boto.s3.connection import S3Connection
from boto.s3.bucket import Bucket
from boto.s3.key import Key
import Queue
import threading

conn = S3Connection('xxxx', 'xxxxxxxx')
bucket = Bucket(conn, 'xxxxx')
def worker():
   while True:
       item = q.get()
       print 'processing ' + item + '...\n'
       k = Key(bucket)
       k.key = item
       k.set_contents_from_filename(item)
       q.task_done()

           
q = Queue.Queue()
for i in range(4):
    print "here"
    t = threading.Thread(target=worker)
    t.daemon = True
    t.start()

import glob
files = glob.glob('*.pdf')
for file in files:
   print "using" 
   q.put(file)

q.join()       # block until all tasks are done

quit()
