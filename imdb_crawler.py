import sqlite3
from requests import get
import re
from bs4 import BeautifulSoup


def cleanString(s):
    return re.sub(r"[^a-zA-Z0-9]+", ' ', s)


class IMDb_crawler():

    # intializing the database
    def __init__(self, dbname):
        self.con = sqlite3.connect(dbname)
        self.c = self.con.cursor()

    def __del__(self):
        self.con.close()

    def commit(self):
        self.con.commit()

    def create_tables(self):
        self.c.execute(
            "create table IF NOT EXISTS movie_data(name TEXT, year INT, rating REAL, summary TEXT, genre TEXT)")
        # to make the search faster
        self.c.execute(
            "create index IF NOT EXISTS imdb_idx on movie_data(name)")

    def is_indexed(self, movie_id):
        try:
            field = self.c.execute(
                'select * from movie_data where name= "{}"'.format(movie_id)).fetchone()
            if field:
                return True
            else:
                return False
        except Exception as e:
            print("{} at {}".format(e, movie_id))

    def add_to_index(self, movie_data):                 # data is dictionary structure
        self.c.execute("insert into movie_data values(?,?,?,?,?)", (
            movie_data['name'], movie_data['year'], movie_data['rating'], movie_data['summary'], movie_data['genre']))

    def crawl(self, limit=100, min_rating=5):
        print("how many movies u wanna crawl: ")
        limit = int(input())
        print("crawling....")
        self.create_tables()
        movies_indexed_so_far = 0
        genres = ("action", "comedy", "mystery", "sci_fi", "adventure",
                  "fantasy", "horror", "animation", "drama", "thriller")
        iteration = 0
        count = 0
        while count < limit:
            for genre in genres:
                if count > limit:
                    break
                c = self.get_webpage(genre, iteration)
                if c == None:
                    continue

                # change to c.read() later on
                soup = BeautifulSoup(c.text, "html.parser")
                data = self.get_movie_data(soup, min_rating)
                for i in range(len(data)):
                    if not self.is_indexed(data[i]['name']):
                        self.add_to_index(data[i])
                        movies_indexed_so_far += 1
                self.commit()
                print("movies_indexed_so_far", movies_indexed_so_far)
                count += 50
            iteration += 1

    def get_webpage(self, genre, iteration):
        try:
            self.url = "http://www.imdb.com/search/title?at=0&genres="+genre + \
                "&sort=moviemeter,asc&start=" + \
                str(iteration*50+1)+"&title_type=feature"
            c = get(self.url)
            return c
        except Exception as e:
            print("error is ", e)
            print("could not open url", self.url)
            return None

    def get_movie_data(self, soup, min_rating):
        movie_containers = soup.find_all(
            'div', class_='lister-item mode-advanced')
        data = []
        for movie in movie_containers:
            try:
                name = cleanString(movie.h3.a.text)
                year = movie.h3.find(
                    'span', class_='lister-item-year text-muted unbold').text
                rating = movie.find(
                    'div', class_='inline-block ratings-imdb-rating')
                if rating:
                    rating = rating.strong.text
                else:
                    rating = "NaN"
                summary = movie.find_all('p', class_='text-muted')[1].text
                genre = movie.find('span', class_="genre").text

                data.append({
                    'name': name,
                    'year': year,
                    'rating': rating,
                    'summary': summary,
                    'genre': genre
                })
            except Exception as e:
                print(e, "at "+name)
                return data

        # print(data)
        return data


crawler = IMDb_crawler('movie_dbase.db')
crawler.crawl()
