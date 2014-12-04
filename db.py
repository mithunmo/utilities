from boto.s3.connection import S3Connection
from boto.s3.bucket import Bucket
from boto.s3.key import Key
import MySQLdb
import Queue
import threading
import time

#conn.select_db("mofilm_messages")

conn = S3Connection('xxxxxx', 'xxxxxx')
bucket = Bucket(conn, 'chakra-test')

			
conn = MySQLdb.connect (host = "localhost",
                        user = "root",
                        passwd = "yourpassword"
                       )
conn.select_db("mofilm_content")		
cursor = conn.cursor()
q = Queue.Queue()


def worker():
       item = q.get()
       print 'processing ' + str(item) + '...\n'
       k = Key(bucket)
       k.key = item
       k.set_contents_from_filename(item)
       q.task_done()
       print "Upload done"


while True:
		cursor.execute("SELECT  filename from uploadQueue,movieAssets where status = 'Queued' AND movieAssets.movieID = uploadQueue.movieID AND movieAssets.type = 'Source' ")
		row = cursor.fetchall()
		print row
		if len(row) > 0:
			
			for item in row:
				print item[0]
				q.put(item[0])

			print "Found"
			for i in range(2):
				t = threading.Thread(target=worker)
				t.daemon = True
				t.start()

			q.join()
		else:
			print "Not found"

		cursor.execute("update uploadQueue set status='Sent' where status= 'Queued' ")
		time.sleep(5)						

cursor.close ()
conn.close ()

