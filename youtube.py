import streamlit as st
from pymongo import MongoClient
from googleapiclient.discovery import build
import mysql.connector
import pandas as pd

# Youtube connection string
def Api_connect():
	Api_Id = "AIzaSyBtR1rjMSOzeXu9mxSEQ2-R2RPrBN1DR7Y"
	api_service_name ="youtube"
	api_version = "v3"
	youtube = build(api_service_name,api_version,developerKey=Api_Id)
	return youtube

youtube = Api_connect()

#get Channels information
def get_channel_info(channel_id):
	request = youtube.channels().list(
		  part = "snippet,ContentDetails,statistics",
		  id = channel_id
	)
	response = request.execute()
	for i in response ['items']:
			data = dict(Channel_Name = i["snippet"]["title"],
		    Channel_Id = i['id'],
		    Subscribers = i["statistics"]["subscriberCount"],
		    Views = i["statistics"]["viewCount"],
		    Total_Videos = i["statistics"]["videoCount"],
		    Channel_Description = i["snippet"]["description"],
		    Playlist_Id = i["contentDetails"]["relatedPlaylists"]["uploads"]
              )
	return data

# get video_ids
def get_videos_ids(channel_id):

	video_ids = []
	response = youtube.channels().list(id = channel_id,
										part ="contentDetails").execute()
	Playlist_Id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

	next_page_token = None
	while True:
		response1 = youtube.playlistItems().list(
																				part = 'snippet',
																				playlistId = Playlist_Id,
																				maxResults = 50,
																				pageToken = next_page_token).execute()
		for i in range(len(response1['items'])):
			video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId']	)
		next_page_token = response1.get('nextPageToken')

		if next_page_token is None:
			break
	return video_ids

#get video information
def get_video_info(video_ids):
	video_data=[]
	for video_id in video_ids:
		request = youtube.videos().list(part = "snippet,ContentDetails,statistics",
										id = video_id
										)
		response = request.execute()
		for item in response['items']:
			data = dict(Channal_Name = item['snippet']['channelTitle'],
									Channel_Id = item ['snippet']['channelId'],
									Video_Id = item['id'],
									Title = item['snippet']['title'],
									Tags = item['snippet'].get('tags', []),
									Thumbnail = item ['snippet']['thumbnails']['default']['url'],
									Description = item['snippet']['description'],
									Published_Date = item ['snippet']['publishedAt'],
									Duration = item ['contentDetails']['duration'],
									Views = item['statistics'].get('viewCount',0),
									Comments = item['statistics'].get('commentCount', 0),
									Favorite_Count = item ['statistics'].get('likeCount', 0),
									Definition = item ['contentDetails']['definition'],
									Caption_Status = item ['contentDetails']['caption']
									)

			video_data.append(data)
	return video_data

#get comment information

def get_comment_info(video_ids):
  Comment_data = []
  try:
    for video_id in video_ids:
      request = youtube.commentThreads().list(
                                    part = "snippet",
                                    videoId = video_id,
                                    maxResults = 50
                              )
      response = request.execute()
      for item in response["items"]:
        data = dict(Comment_Id = item ['snippet']['topLevelComment']['id'],
                    Video_Id = item ['snippet']['topLevelComment']['snippet']['videoId'],
                    Comment_Text = item ['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_Author = item ['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_Published = item ['snippet']['topLevelComment']['snippet']['publishedAt']
                    )

        Comment_data.append(data)
  except:
    pass

  return Comment_data

# get_playlist_details

def get_playlist_details(channel_id):

  Playlist_data = []

  next_page_token = None

  while True:
    request = youtube.playlists().list(
                                  part = "snippet,contentDetails",
                                  channelId = channel_id,
                                  maxResults = 50,
                                  pageToken = next_page_token
                                        )
    response = request.execute()
    for item in response["items"]:
      data = dict(Playlist_Id = item ['id'],
                  Title = item ['snippet']['title'],
                  Channel_Id = item ['snippet']['channelId'],
                  Channel_Name = item ['snippet']['channelTitle'],
                  PublishedAt = item ['snippet']['publishedAt'],
                  Video_Count = item ['contentDetails']['itemCount']
                  )
      Playlist_data.append(data)
    next_page_token = response.get('nextPageToken')
    if next_page_token is None:
      break

  return Playlist_data

# upload to mongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['youtube_data']

def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_id_details = get_videos_ids(channel_id)
    vi_details = get_video_info(vi_id_details)
    com_details = get_comment_info(vi_id_details)
    


    collection1 = db["channel_details"]
    collection1.insert_one({"Channel_info":ch_details,
                            "Playlist_info":pl_details,
                            "Video_info":vi_details,
                            "Comment_info":com_details
                            })
    return "Upload completed successfully!"

#Table creation for channels,Playlists,Videos,Comments

def channels_table():
    
    config = {
        'user':'root', 'password':'1234',
        'host':'127.0.0.1', 'database':'youtube_data'
            }
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor()

    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    connection.commit()

   
    create_query ='''create table if not exists channels(Channel_Name varchar(100),
                                                        Channel_Id varchar(80) primary key,
                                                        Subscribers bigint,
                                                        Views bigint,
                                                        Total_Videos int,
                                                        Channel_Description text,
                                                        Playlist_Id varchar(80))'''
    cursor.execute(create_query)
    connection.commit()



    ch_list = []
    db = client ["youtube_data"]
    collection1 = db["channel_details"]
    for ch_data in collection1.find({},{"_id":0,"Channel_info":1}):
        ch_list.append(ch_data["Channel_info"])

    df = pd.DataFrame(ch_list)
   

    for index, row in df.iterrows():
        insert_query = '''insert into channels (Channel_Name,
                                                Channel_Id,
                                                Subscribers,
                                                Views,
                                                Total_Videos,
                                                Channel_Description,
                                                Playlist_Id) 
                                                
                                                values (%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        
        try:
            cursor.execute(insert_query,values)
            connection.commit()
        except:
            print("Channel values are already inserted!")

# Video Table creation--- 
def videos_table():

    config = {
        'user':'root', 'password':'1234',
        'host':'127.0.0.1', 'database':'youtube_data'
            }
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor()

    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    connection.commit()

    create_query ='''create table if not exists videos(Channal_Name varchar(100),
                                                    Channel_Id varchar(100),
                                                    Video_Id varchar(50) primary key,
                                                    Title varchar(150),
                                                    Tags text,
                                                    Thumbnail varchar(200),
                                                    Description text,
                                                    Published_Date varchar(100),
                                                    Duration varchar(100),
                                                    Views bigint,
                                                    Comments bigint,
                                                    Favorite_Count int,
                                                    Definition varchar(100),
                                                    Caption_Status varchar(100)
                                                        )'''

    cursor.execute(create_query)
    connection.commit()

    vi_list = []
    db = client["youtube_data"]
    collection1 = db["channel_details"]
    for vi_data in collection1.find({},{"_id":0,"Video_info":1}):
        for i in range(len(vi_data['Video_info'])):
            vi_list.append(vi_data['Video_info'][i])

    df2 = pd.DataFrame(vi_list)

    for index,row in df2.iterrows():
            
            tags_as_str = ' '.join([str(elem) for elem in row['Tags']])
            
            insert_query = '''insert into videos(Channal_Name,
                                                            Channel_Id,
                                                            Video_Id ,
                                                            Title ,
                                                            Tags ,
                                                            Thumbnail ,
                                                            Description ,
                                                            Published_Date ,
                                                            Duration ,
                                                            Views ,
                                                            Comments ,
                                                            Favorite_Count ,
                                                            Definition ,
                                                            Caption_Status ) 
                                                    
                                                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            

            values = (row['Channal_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    tags_as_str,
                    row['Thumbnail'],
                    row['Description'],
                    row['Published_Date'],
                    row['Duration'],
                    row['Views'],
                    row['Comments'],
                    row['Favorite_Count'],
                    row['Definition'],
                    row['Caption_Status'])
            try:
                    connection = mysql.connector.connect(**config)
                    cursor = connection.cursor()
                    cursor.execute(insert_query,values)
                    connection.commit()
            except:
                    print("Video values are already inserted!")
        
#creating Comment Table 
def comments_table():

    config = {
        'user':'root', 'password':'1234',
        'host':'127.0.0.1', 'database':'youtube_data'
            }
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor()

    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    connection.commit()

    create_query ='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Video_Id varchar(50),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published varchar(100)
                                                        )'''

    cursor.execute(create_query)
    connection.commit()

    com_list = []
    db = client["youtube_data"]
    collection1 = db["channel_details"]
    for com_data in collection1.find({},{"_id":0,"Comment_info":1}):
        for i in range(len(com_data['Comment_info'])):
            com_list.append(com_data['Comment_info'][i])

    df3 = pd.DataFrame(com_list)

    for index,row in df3.iterrows():
        insert_query = '''insert into comments(Comment_Id,
                                            Video_Id ,
                                            Comment_Text ,
                                            Comment_Author ,
                                            Comment_Published ) 
                                                
                                                values (%s,%s,%s,%s,%s)'''
    

        values = (row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published'])
        try:
                connection = mysql.connector.connect(**config)
                cursor = connection.cursor()
                cursor.execute(insert_query,values)
                connection.commit()
        except:
                print("Comment values are already inserted!")

def tables():
    channels_table()
    videos_table()
    comments_table()
    return "Data migrated to SQL successfully"
    

#-----------------Streamlit page functions


client = MongoClient('mongodb://localhost:27017/')
db = client['youtube_data']

st.title("YouTube Data Harvesting and Warehousing")

# text box for channel id

channel_id = st.text_input("Enter youtube channel id below:")
if st.button("Extract and upload to MongoDB"):
     ch_ids = []
     db = client['youtube_data']
     col1 = db['channel_details']
     for ch_data in col1.find({},{"_id":0,"Channel_info":1}):
        ch_ids.append(ch_data['Channel_info']['Channel_Id'])
     if channel_id in ch_ids:
        st.success("Channel details of the given channel id already exists")
     else:
        insert = channel_details(channel_id) 
        st.success(insert)

# select box for channel names in mongodb

ch_names = []
db = client['youtube_data']
col1 = db['channel_details']
for ch_data in col1.find({},{"_id":0,"Channel_info":1}):
  ch_names.append(ch_data['Channel_info']['Channel_Name'])

channel_name = st.selectbox("Select Channel",ch_names)


# Button will be here

if st.button("Submit"):
     creation_of_table = tables()
     st.success(creation_of_table)

question = st.selectbox("Select your question",("Choose a question",
                                              "1. All the videos and the channel name",
                                              "2. Channels with most no. of videos",
                                              "3. 10 most viewed videos",
                                              "4. Comments in each videos",
                                              "5. Videos with highest likes",
                                              "6. Likes of all videos",
                                              "7. Views of each channel",
                                              "8. Videos published in the year of 2022",
                                              "9. Avg duration of all videos in each channel",
                                              "10. Videos with highest no. of comments"))

config = {
    'user':'root', 'password':'1234',
    'host':'127.0.0.1', 'database':'youtube_data'
        }
connection = mysql.connector.connect(**config)
cursor = connection.cursor()

if question == "2. Channels with most no. of videos":
    Query2 = '''select * from channels;'''
    #Query2 = '''select channel_name as channelname,total_videos as No_of_videos from channels order by total_videos desc'''
    #cursor.execute(Query2)
    #connection.commit()
    #t2 = cursor.fetchall()
    #df2 = pd.DataFrame(t2, columns=["channel name","No of Videos"])
    #st.write(df2)
    conn = st.connection('mysql',type='sql')
    df = conn.query(Query2,ttl = 600)
    for row in df.itertuples():
         st.write(df)
