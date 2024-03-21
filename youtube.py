import streamlit as st 
from pymongo import MongoClient
from googleapiclient.discovery import build
import mysql.connector
import pandas as pd
from datetime import datetime

# Youtube connection string
def Api_connect():
	Api_Id = "AIzaSyBQesTlK-kUn2d-Otg5trACvjBSmD541Yk"
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
			data = dict(Channel_Name = item['snippet']['channelTitle'],
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

    #drop_query = '''drop table if exists channels'''
    #cursor.execute(drop_query)
    #connection.commit()

   
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
    for ch_data in collection1.find({},{"_id":0,"Channel_info":1}).sort({"_id":-1}).limit(1):
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

# Function for Video table duration column values
            
def duration_conv(str1):
        if str1 == "P0D":
            duration = 0
        else:
                try:
                        hr = str1.split('PT')[1].split('H')[0]
                        min = str1.split('PT')[1].split('H')[1].split('M')[0]
                        sec = str1.split('PT')[1].split('H')[1].split('M')[1].split('S')[0]

                        duration = hr+min+sec
                        return int(duration)
                except:
                        min = str1.split('PT')[1].split('M')[0]
                        sec = str1.split('PT')[1].split('M')[1].split('S')[0]

                        duration = min+sec
                        return int(duration)
                
# Video Table creation--- 
def videos_table():

    config = {
        'user':'root', 'password':'1234',
        'host':'127.0.0.1', 'database':'youtube_data'
            }
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor()

    #drop_query = '''drop table if exists videos'''
    #cursor.execute(drop_query)
    #connection.commit()

    create_query ='''create table if not exists videos(Channel_Name varchar(100),
                                                    Channel_Id varchar(100),
                                                    Video_Id varchar(50) primary key,
                                                    Title varchar(150),
                                                    Tags text,
                                                    Thumbnail varchar(200),
                                                    Description text,
                                                    Published_Date timestamp,
                                                    Duration time,
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
    for vi_data in collection1.find({},{"_id":0,"Video_info":1}).sort({"_id":-1}).limit(1):
        for i in range(len(vi_data['Video_info'])):
            vi_list.append(vi_data['Video_info'][i])

    df2 = pd.DataFrame(vi_list)

    date_format = "%Y-%m-%dT%H:%M:%SZ"

    for index,row in df2.iterrows():

            date_obj = datetime.strptime(row['Published_Date'], date_format)
        
            tags_as_str = ' '.join([str(elem) for elem in row['Tags']])

            duration = duration_conv(row['Duration'])

            
            insert_query = '''insert into videos(Channel_Name,
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
            

            values = (row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    tags_as_str,
                    row['Thumbnail'],
                    row['Description'],
                    date_obj,
                    duration,
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

    #drop_query = '''drop table if exists comments'''
    #cursor.execute(drop_query)
    #connection.commit()

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
    for com_data in collection1.find({},{"_id":0,"Comment_info":1}).sort({"_id":-1}).limit(1):
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

st.title("Welcome To YouTube Data Harvesting and Warehousing")

# text box for channel id

channel_id = st.text_input("Enter youtube channel id below:")

with st.spinner():
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
with st.spinner():
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
cursor = connection.cursor(buffered=True)

# Q - 1
if question == "1. All the videos and the channel name":
    Query1 = '''select Title as videos, Channel_Name as channelname from videos'''
    cursor.execute(Query1)
    connection.commit()
    t1 = cursor.fetchall()
    df1 = pd.DataFrame(t1, columns=["Video Title","Channel Name"])
    st.write(df1)

# Q - 2
    
elif question == "2. Channels with most no. of videos":
    
    Query2 = '''select channel_name as channelname,total_videos as No_of_videos from channels order by total_videos desc'''
    cursor.execute(Query2)
    connection.commit()
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2, columns=["channel name","No of Videos"])
    st.write(df2)

# Q - 3
    
elif question == "3. 10 most viewed videos":
    Query3 = '''select views as Views,channel_name as Channelname, title as videotitle from videos 
                where views is not null order by views desc limit 10'''
    cursor.execute(Query3)
    connection.commit()
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3, columns=["Views","Channel name","Video title"])
    st.write(df3)

# Q - 4
    
elif question == "4. Comments in each videos":
    Query4 = '''select comments as no_comments, title as videotitle from videos 
                where comments is not null'''
    cursor.execute(Query4)
    connection.commit()
    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4, columns=["No of Comments","Video title"])
    st.write(df4)

# Q -5

elif question == "5. Videos with highest likes":
    Query5 = '''select title as videotitle , channel_name as channelname, 
                favorite_count as likes from videos 
                where favorite_count is not null order by favorite_count desc'''
    cursor.execute(Query5)
    connection.commit()
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5, columns=["Video title","Channel Name","Like count"])
    st.write(df5)

# Q -6

elif question == "6. Likes of all videos":
    Query6 = '''select favorite_count as likecount ,title as videotitle 
                from videos'''
    cursor.execute(Query6)
    connection.commit()
    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6, columns=["Like count","Video title"])
    st.write(df6)

# Q -7

elif question == "7. Views of each channel":
    Query7 = '''select channel_name as channelname,views as totalviews from channels'''
    cursor.execute(Query7)
    connection.commit()
    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7, columns=["Channel Name","Total Views"])
    st.write(df7)

# Q -8 

elif question == "8. Videos published in the year of 2022":
    Query8 = '''select channel_name as channelname,title as videotitle,
                published_date as videopublished from videos where extract(year from published_date)=2022'''
    cursor.execute(Query8)
    connection.commit()
    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8, columns=["Channel Name","Video Title","Published_date"])
    st.write(df8)

# Q -9 

elif question ==  "9. Avg duration of all videos in each channel":
    Query9 = '''select channel_name as channelname,AVG(duration)as avgduration from videos group by channel_name'''
    cursor.execute(Query9)
    connection.commit()
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9, columns=["Channel Name","Averge Duration in Seconds"])
    st.write(df9)

# Q -10

elif question ==  "10. Videos with highest no. of comments":
    Query10 = '''select title as videotitle,channel_name as channelname,
                comments as comments from videos where comments is not null 
                order by comments desc'''
    cursor.execute(Query10)
    connection.commit()
    t10 = cursor.fetchall()
    df10 = pd.DataFrame(t10, columns=["Video Title","Channel Name","No of Comments"])
    st.write(df10)
