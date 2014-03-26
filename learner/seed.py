import csv, pymongo


blacklist = ["torrents.com", "torrents.fm", "torrents.ms"]
count = 0

config = {}
execfile("learn.conf", config)
db_conn = pymongo.MongoClient(config['database_host'],config['database_port'])

with open('alexa.csv', 'rb') as f:
    reader = csv.reader(f)
    for row in reader:
        if "torrent" in row[1] and not any(b == row[1] for b in blacklist):
            print row[1] 
            if not db_conn.torrents.domain.find_one({"_id":row[1]}):
                
                db_conn.torrents.domain.save({"_id":row[1]})
            count += 1
            
print count
