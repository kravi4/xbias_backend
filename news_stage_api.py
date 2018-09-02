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
news_bias_hashing={'the-huffington-post': 0.816257115, 'msnbc': 1.212843498, 'buzzfeed': 1.212843498, 'politico': 14.18510649, 'axios': 19.78161114, 'the-new-york-times': 19.78161114, 'the-washington-post': 19.78161114, 'the-guardian-uk': 19.78161114, 'the-guardian-au': 19.78161114, 'cnn': 26.89414214, 'nbc-news': 35.43436938, 'bbc-news': 35.43436938, 'associated-press': 40.13123399, 'abc-news': 45.01660027, 'abc-news-au': 45.01660027, 'usa-today': 50.0, 'reuters': 54.98339973, 'bloomberg': 54.98339973, 'cbs-news': 54.98339973, 'the-wall-street-journal': 59.86876601, 'time': 59.86876601, 'the-economist': 64.56563062, 'daily-mail': 75.21838886, 'the-hill': 81.20183851, 'national-review': 82.08615797, 'the-washington-times': 83.26758241, 'the-american-conservative': 85.201379, 'fox-news': 90.18374288, 'breitbart-news': 99.63157601}

# News Image hashing
news_image_hashing = {'the-huffington-post': 'https://www.underconsideration.com/brandnew/archives/huffpost_logo.png', 
					  'msnbc': 'https://www.seeklogo.net/wp-content/uploads/2015/08/msnbc-logo-vector-download.jpg',
					  'buzzfeed': 'https://www.everplans.com/sites/default/files/styles/article_header_image/public/buzzfeed-750.jpg?itok=E8sS_n6l',
					  'politico': 'https://static.politico.com/da/f5/44342c424c68b675719324b1106b/politico.jpg',
					  'axios': 'https://pmcvariety.files.wordpress.com/2017/11/axios1.png?w=1000&h=563&crop=1',
					  'the-new-york-times': 'https://www.famouslogos.net/images/new-york-times-logo.jpg',
					  'the-washington-post': 'https://s8637.pcdn.co/wp-content/uploads/2017/03/washington-post-logo-e1490379930525.jpg',
					  'the-guardian-uk': 'http://www.webdo.tn/wp-content/uploads/2018/02/guardian.jpg',
					  'the-guardian-au': 'http://www.webdo.tn/wp-content/uploads/2018/02/guardian.jpg',
					  'cnn': 'https://cdn.logojoy.com/wp-content/uploads/2017/07/cnn-logo-original-hd-png-transparent.png',
					  'nbc-news': 'https://aclion.com/wp-content/uploads/2018/04/NBC-News-Logo.png',
					  'bbc-news': 'https://m.files.bbci.co.uk/modules/bbc-morph-news-waf-page-meta/2.2.2/bbc_news_logo.png',
					  'associated-press': 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0c/Associated_Press_logo_2012.svg/2000px-Associated_Press_logo_2012.svg.png',
					  'abc-news': "https://www.broadcastingcable.com/.image/t_share/MTU0MjAzNTAyODExNDI0MDgw/abc-news-logo-resized-bcjpg.jpg",
					  'abc-news-au': 'https://www.broadcastingcable.com/.image/t_share/MTU0MjAzNTAyODExNDI0MDgw/abc-news-logo-resized-bcjpg.jpg',
					  'usa-today': 'https://3degreesinc.com/wp-content/uploads/2018/05/Color-USA-Today-Logo.jpg',
					  'reuters': 'https://i1.wp.com/www.thedailychronic.net/wp-content/uploads/2014/06/Reuters-logo.jpg',
					  'bloomberg': 'https://www.everplans.com/sites/default/files/styles/article_header_image/public/bloomberg-750.jpg?itok=_eVS4ABN',
					  'cbs-news': 'https://upload.wikimedia.org/wikipedia/commons/5/52/CBS_News_logo.png',
					  'the-wall-street-journal': 'https://www.logosvgpng.com/wp-content/uploads/2018/03/the-wall-street-journal-logo-vector.png',
					  'time': 'https://logos-download.com/wp-content/uploads/2016/05/Time_Magazine_logo_red_bg.png',
					  'the-economist': 'https://logos-download.com/wp-content/uploads/2016/05/The_Economist_logo.png',
					  'daily-mail': 'https://cdn.worldvectorlogo.com/logos/daily-mail.svg',
					  'the-hill': 'https://www.afsc.org/sites/default/files/styles/maxsize/public/images/The%20Hill%20logo.jpg?itok=ogKsb3Jo',
					  'national-review': 'http://www.will-law.org/wp-content/uploads/2018/06/nationalreview-lg.png',
					  'the-washington-times': 'http://www.johnellis.com/wp-content/uploads/2018/03/Washington_Times_John_Ellis_water.png',
					  'the-american-conservative': 'https://ilsr.org/wp-content/uploads/2017/09/The-American-Conservative-logo.png',
					  'fox-news': 'https://vignette.wikia.nocookie.net/logopedia/images/a/a6/Fox-news-logo-1.jpg/revision/latest?cb=20120407163458',
					  'breitbart-news': 'https://www.thewrap.com/wp-content/uploads/2016/11/Breitbart-logo.jpg'
					  }



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
			if dic["source"]["id"] in list(news_bias_hashing.keys()):
				dic["urlToImage"] = news_image_hashing[dic["source"]["id"]]
			else:
				dic["urlToImage"] = "No Listed UrlToImge"
		if isinstance(dic["publishedAt"], type(None)):
			dic["publishedAt"] = "No Listed timestamp"


		dic["description"]=dic["description"].replace("'", "''")
		dic["description"]=dic["description"].replace('"', '""')
		dic["title"]=dic["title"].replace("'", "''")
		dic["title"]=dic["title"].replace('"', '""')


		article_hash_string=dic["source"]["name"]+" "+dic["title"]+" "+dic["author"]
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
		cur.execute("""CREATE TABLE IF NOT EXISTS article_table (article_hash_id VARCHAR(255), source_id TEXT, source_name TEXT, author TEXT, title TEXT, description TEXT, url TEXT, image_url TEXT, publish_timestamp TEXT,  article_bias_score FLOAT, topic TEXT);""")
		cur.execute("""SELECT article_hash_id FROM article_table""")
		result = cur.fetchall()

		final_results=[]
		for i in range(len(result)):
			final_results.append(result[i][0])

		if (len(result)>0):
			removal_list=[]
			for i in range(len(article_hash_entries)):
				if article_hash_entries[i] in final_results:
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




