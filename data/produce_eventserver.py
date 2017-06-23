"""
Import sample data for complementary purchase engine
"""

import predictionio
import argparse
import random
import csv
import re
import MySQLdb

def prepare_data():
    res, c = [], 0
    db = MySQLdb.connect(host="localhost",    
                     user="root",         
                     passwd="Naresh123",  
                     db="movies")       
    cur = db.cursor()
    cur.execute("SELECT p.id,p.name,mpr.movie_id from movies.persons as p, movies.movies as m, movies.movie_person_role_xref as mpr where mpr.role_id = 1 and mpr.person_id = p.id and mpr.movie_id = m.id")
    actors = [actor for actor in cur.fetchall() if actor != ""]
   
    cur.execute("SELECT p.id,p.name,mpr.movie_id from movies.persons as p, movies.movies as m, movies.movie_person_role_xref as mpr where mpr.role_id = 2 and mpr.person_id = p.id and mpr.movie_id = m.id")
    directors = [director for director in cur.fetchall() if director != ""]
    
    cur.execute("SELECT p.id,p.name,mpr.movie_id from movies.persons as p, movies.movies as m, movies.movie_person_role_xref as mpr where mpr.role_id = 3 and mpr.person_id = p.id and mpr.movie_id = m.id")
    writers = [writer for writer in cur.fetchall() if writer != ""]

    cur.execute("SELECT g.name, m.id FROM movies.movie_genre_xref as mgr, movies.genres as g, movies.movies as m where mgr.movie_id = m.id and g.id = mgr.genre_id")
    genres = [genre for genre in cur.fetchall() if genre != ""]

    cur.execute("select m.title, m.language, m.release_date, m.year, m.id, m.ratings, m.movieratings, m.musicratings, m.gross, m.budget, m.score from movies.movies as m")    
    
    for row in cur.fetchall() :
        c,ic=c+1,0
        data = {}
        
        for desc in cur.description :
            if ic<3:
                b=unicode(str(row[ic]), encoding='ascii',errors='replace')
                data[desc[0]] = b.encode('ascii', 'ignore')
            else:
                if row[ic] != None :
                    data[desc[0]]=row[ic]
                else:
                    data[desc[0]] = 0
            ic=ic+1
        data["genres"] = [unicode(str(genre), encoding='ascii',errors='replace').encode('ascii', 'ignore') for genre in genres if str(data["id"]) == str(genre[1])]
        data["actors"] = [unicode(str(actor), encoding='ascii',errors='replace').encode('ascii', 'ignore') for actor in actors if str(data["id"]) == str(actor[2])]
        data["writers"] = [unicode(str(writer), encoding='ascii',errors='replace').encode('ascii', 'ignore') for writer in writers if str(data["id"]) == str(writer[2])]
        data["directors"] = [unicode(str(director), encoding='ascii',errors='replace').encode('ascii', 'ignore') for director in directors if str(data["id"]) == str(director[2])]
        res.append(data)

    res.append(c)
    return res           

def import_events(client, data):
    count = 0
    c=data.pop()

    for el in data[0:c]:
        count += 1
        print("%d / %d" % (count, c))
        client.create_event(
            event="$set",
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
