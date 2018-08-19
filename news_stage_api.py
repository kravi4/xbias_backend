from newsapi import NewsApiClient
import pymysql
import sys
import hashlib

news_api_key="915217c3b0e343039cc3859ff8445d8a"


# Amazon Aurora Credentials
REGION = 'us-east-2'

rds_host = 'xbias-backend.cytnlddqky1c.us-east-2.rds.amazonaws.com'
username = 'xBias_master'
password = 'Kr19972=14'
db_name = 'xbias_backend'

# Initializing news api 
newsapi = NewsApiClient(api_key=news_api_key)

# Getting the top headlines
top_headlines = newsapi.get_top_headlines(language='en', country='us')

# News Bias - Staging
news_bias_hashing={'the-huffington-post': 0.816257115, 'msnbc': 1.212843498, 'buzzfeed': 1.212843498, 'politico': 14.18510649, 'axios': 19.78161114, 'the-new-york-times': 19.78161114, 'the-washington-post': 19.78161114, 'the-guardian-uk': 19.78161114, 'the-guardian-au': 19.78161114, 'cnn': 26.89414214, 'nbc-news': 35.43436938, 'bbc-news': 35.43436938, 'associated-press': 40.13123399, 'abc-news': 45.01660027, 'abc-news-au': 45.01660027, 'usa-today': 50.0, 'reuters': 54.98339973, 'bloomberg': 54.98339973, 'cbs-news': 54.98339973, 'the-wall-street-journal': 59.86876601, 'time': 59.86876601, 'the-economist': 64.56563062, 'daily-mail': 80.21838886, 'the-hill': 83.20183851, 'national-review': 93.08615797, 'the-washington-times': 94.26758241, 'the-american-conservative': 98.201379, 'fox-news': 99.18374288, 'breitbart-news': 99.63157601}


def input_into_article_table():
	article_hash_entries=[]
	source_ids=[]
	source_names=[]
	author_names=[]
	titles=[]
	descriptions=[]
	urls=[]
	image_urls=[]
	publish_timestamps=[]
	article_bias_scores=[]


	for dic in top_headlines["articles"]:
		if isinstance(dic["source"]["id"], type(None)):
			dic["source"]["id"] = "No Listed Source id"
		if isinstance(dic["source"]["name"], type(None)):
			dic["source"]["name"] = "No Listed Source Name"
		if isinstance(dic["author"], type(None)):
			dic["author"] = "No Listed Author"
		if isinstance(dic["title"], type(None)):
			dic["title"] = "No Listed Title"
		if isinstance(dic["description"], type(None)):
			dic["description"] = "No Listed description"
		if isinstance(dic["url"], type(None)):
			dic["url"] = "No Listed url"
		if isinstance(dic["urlToImage"], type(None)):
			dic["urlToImage"] = "No Listed urlToImage"
		if isinstance(dic["publishedAt"], type(None)):
			dic["publishedAt"] = "No Listed timestamp"


		dic["description"]=dic["description"].replace("'", "''")
		dic["description"]=dic["description"].replace('"', '""')
		dic["title"]=dic["title"].replace("'", "''")
		dic["title"]=dic["title"].replace('"', '""')


		article_hash_string=dic["source"]["name"]+" "+dic["title"]+" "+dic["author"]+" "+dic["publishedAt"]
		article_hash_entries.append(hashlib.sha256(str(article_hash_string).encode()).hexdigest())
		source_ids.append(dic["source"]["id"])
		source_names.append(dic["source"]["name"])
		author_names.append(dic["author"])
		titles.append(dic["title"])
		descriptions.append(dic["description"])
		urls.append(dic["url"])
		image_urls.append(dic["urlToImage"])
		publish_timestamps.append(dic["publishedAt"])

		if dic["source"]["id"] in list(news_bias_hashing.keys()):
			article_bias_scores.append(news_bias_hashing[dic["source"]["id"]])
		else:
			article_bias_scores.append(-1)

	bias_removal=[]
	for i in range(len(article_bias_scores)):
		if article_bias_scores[i]==-1:
			bias_removal.append(i)

	for index in sorted(bias_removal, reverse=True):
		del article_hash_entries[index]
		del source_ids[index]
		del source_names[index]
		del author_names[index]
		del titles[index]
		del descriptions[index]
		del urls[index]
		del image_urls[index]
		del publish_timestamps[index]
		del article_bias_scores[index]




	conn = pymysql.connect(rds_host, user = username, passwd = password, db= db_name, connect_timeout = 10, autocommit=True)
	with conn.cursor() as cur:
		cur.execute("""CREATE TABLE IF NOT EXISTS article_table (article_hash_id VARCHAR(255), source_id TEXT, source_name TEXT, author TEXT, title TEXT, description TEXT, url TEXT, image_url TEXT, publish_timestamp TEXT,  article_bias_score FLOAT);""")
		cur.execute("""SELECT article_hash_id FROM article_table""")
		result = cur.fetchall()

		if (len(result)>0):
			removal_list=[]
			for i in range(len(article_hash_entries)):
				if article_hash_entries[i] in result[i]:
					removal_list.append(i)
			for index in sorted(removal_list, reverse=True):
				del article_hash_entries[index]
				del source_ids[index]
				del source_names[index]
				del author_names[index]
				del titles[index]
				del descriptions[index]
				del urls[index]
				del image_urls[index]
				del publish_timestamps[index]
				del article_bias_scores[index]


		if(len(article_hash_entries)>0):	
			for i in list(range(len(article_hash_entries))):
				cur.execute(
					"""
					INSERT INTO article_table (article_hash_id, source_id, source_name, author, title, description, url, image_url, publish_timestamp, article_bias_score) VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9})
					""".format('"'+str(article_hash_entries[i])+'"', '"' + str(source_ids[i]) + '"', '"' + str(source_names[i]) + '"', '"' + str(author_names[i]) + '"', "'" + str(titles[i]) + "'", "'" +str(descriptions[i]) + "'",  "'" +str(urls[i]) + "'", "'" + str(image_urls[i]) + "'", "'" + str(publish_timestamps[i])+"'", "'" + str(article_bias_scores[i])+"'")
				)
	cur.close()


if __name__ == "__main__":
	input_into_article_table()




