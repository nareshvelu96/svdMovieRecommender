"""
Import sample data for complementary purchase engine
"""

import predictionio
import argparse
import random
import csv
import re
import MySQLdb


#NUMSAMPLES = 1000

def prepare_data():
    c=0
    res = []
    db = MySQLdb.connect(host="localhost",    
                     user="root",         
                     passwd="Naresh123",  
                     db="movies")       
    cur = db.cursor()
    cur.execute("select m.id,m.title, p.name as person_name, r.name as role, g.name as genre, m.year, m.language, m.release_date, m.ratings, m.movieratings, m.musicratings, m.gross, m.budget, m.score from movies.movies as m, movies.persons as p, movies.roles as r, movies.movie_person_role_xref as mpr, movies.genres as g, movies.movie_genre_xref as mgr where m.id = mpr.movie_id and r.id = mpr.role_id and p.id = mpr.person_id and mpr.movie_id = mgr.movie_id and g.id = mgr.genre_id")    
    
    for row in cur.fetchall() :
        c,ic=c+1,0
        data = {}

        for desc in cur.description :
            b=unicode(str(row[ic]), encoding='ascii',errors='replace')
            data[desc[0]] = b.encode('ascii', 'ignore')
            ic=ic+1
        res.append(data)

    res.append(c)
    return res           

def import_events(client, data):
    count = 0
    c=data.pop()
    for el in data[0:c]:
        count += 1
        print "%d / %d" % (count, c)
        client.create_event(
            event="$delete events",
            entity_type="item",
            entity_id=el["id"],
            properties=el)

    print("%s events are imported." % count)
            
        
def main():
    
    parser = argparse.ArgumentParser(
        description="Import sample data for similar items by attributes engine")
    parser.add_argument('--access_key', default='invald_access_key')
    parser.add_argument('--url', default="http://localhost:7070")
    
    args = parser.parse_args()    
    
    client = predictionio.EventClient(access_key=args.access_key, url=args.url, 
        threads=4, qsize=100)
    
    data = prepare_data()

    import_events(client, data)
    
if __name__ == '__main__':
    main()
