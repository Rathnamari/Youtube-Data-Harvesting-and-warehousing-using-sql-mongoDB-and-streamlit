import streamlit as st
from googleapiclient.discovery import build
import pymongo
import pandas as pd
import psycopg2
import isodate
import json 


# create a client instance of MongoDB
client = pymongo.MongoClient("mongodb+srv://Rathnamari:Rathnamari@cluster0.oe3fdas.mongodb.net/?retryWrites=true&w=majority")
# create a database or use existing one
db = client["Youtube"]

#connection with postgres sql
import psycopg2
mydb = psycopg2.connect(host = "localhost",user = "postgres",password = "Tkkrathna26@",port = "5432",database = "YT")
mycursor = mydb.cursor()

# Access youtube API
serive_name = "youtube"
version = "v3"
api_key = "AIzaSyA_ZeEJKCnQIPZo0-rqmYR6NbFbhBI5KPQ"
youtube = build("youtube" , "v3" , developerKey = api_key)

# Define a function to convert duration
def duration(data):
    dur = isodate.parse_duration(data)
    sec = dur.total_seconds()
    hours = float(int(sec) / 3600)
    return(hours)

# Define a function to get single channel ID
def channel_details(user_inp):
  chl_id = []
  request = youtube.search().list(
          part = "snippet",
          channelType = "any",
          q = user_inp)
  response = request.execute()
  channel_id =  response["items"][0]["snippet"]["channelId"]
  chl_id.append(channel_id)
  return channel_id
    
# Process channel data
# Extract required information from the channel_data
def channel_full_details(youtube,channel_id):
    chl_dt = []
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
             id= channel_id
             )
    response = request.execute()
    for i in range(len(response["items"])):
      chl_data = dict(Channel_name = response['items'][i]['snippet']['title'],
                    Channel_Id=response['items'][i]["id"],
                    Total_videos = response['items'][i]['statistics']['videoCount'],
                    playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    subscribercount = response['items'][i]['statistics']['subscriberCount'],
                    Views = response['items'][i]['statistics']['viewCount'])
      chl_dt.append(chl_data)
    return chl_data



# Define a function to retrieve  playlist Id
def play_list_id(youtube,channel_id):
  request = youtube.channels().list(
            part="contentDetails",
             id= channel_id
  )
  response = request.execute()

  playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
  return playlist_id

# Define a function to retrieve video IDs from channel playlist
def get_video_ids(youtube,playlist_id):
  vdo_ids = []
  next_page_token = None
  while True:
    request = youtube.playlistItems().list(
              part="snippet",
              playlistId=playlist_id,
              maxResults = 50,
              pageToken = next_page_token
    )
    response = request.execute()
    for i in range(len(response['items'])):
      vdo_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
    if next_page_token is None:
      break
    next_page_token = response.get('nextpageToken')
  return vdo_ids

# Define a function to retrieve video data
def get_video_details(youtube,vdo_ids):
   vd_ids = []
   for i in range(0,len(vdo_ids),50):
     request = youtube.videos().list(
                  part='snippet,statistics,contentDetails',
                  id =','.join(vdo_ids[i:i+50])
     )
     response=request.execute()
     for i in range(len(response['items'])):
       data = dict(channel_name=response['items'][i]['snippet']['channelTitle'],
                channel_id=response['items'][i]['snippet']['channelId'],
                video_id=response['items'][i]['id'],
                Title=response['items'][i]['snippet']['title'],
                video_view = int(response['items'][i]['statistics']['viewCount']),
                Duration=duration(response['items'][i]['contentDetails']['duration']),
                published = response['items'][i]['snippet']['publishedAt'],
                Like_count=int(response['items'][i]['statistics']['likeCount']),
                Dislike_count = int(response['items'][i]['statistics'].get('dislikeCount',0)),
                Comment_count=int(response['items'][i]['statistics']['commentCount']))

       vd_ids.append(data)
   return vd_ids


#Define a function to get full comment details
def get_comments_details(youtube,vdo_ids):
    comment_data = []
    for v in vdo_ids:
      try:
          request = youtube.commentThreads().list(part="snippet,replies",
                                                 videoId=v,
                                                 maxResults = 15)
          response = request.execute()

          for i in range(len(response['items'])):
            data = dict(Comment_id = response['items'][i]['id'],
                        Video_id = response['items'][i]['snippet']['videoId'],
                        Comment_text = response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_author = response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_posted_date = response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'],
                        Like_count = response['items'][i]['snippet']['topLevelComment']['snippet']['likeCount'],
                        Reply_count = response['items'][i]['snippet']['totalReplyCount'])
            comment_data.append(data)
          return comment_data
      
      except:
        pass

#Title for whole project
st.title('**:red[YouTube Data Harvesting And Warehousing Using SQL, MongoDB And Streamlit]**')

#select option from user
option = st.radio(
    "please select any one",
    ["view datas","Migration", "Questions"],
    captions = ["Ready to view datas","migrating option" ,"comparing each channel in different view."])

#create migrating options
if option == "Migration" or option == "view datas":   
  
#Getting channel_names
  ch_name = []
  for i in db.channel.find():
    ch_name.append(i['Channel_name'])      

#getting input from user 
  user_inp = st.selectbox("Select channel", options=ch_name)
    
  try:
      if option == "Migration" and option!= "view datas": 
        migrate_option = st.selectbox('Select any one options',
        ('Migrate to MongoDB','Migrate to SQL'))
        st.button('ENTER')
  except:
        st.error("Ready to migrating information")
      
   
#Define a function to show datas to user
  if option =='view datas' and option!= 'Migration':
        
        try:
          #calling a channel functions to collect information to view     
          channel_id = channel_details(user_inp)
          channel_details = channel_full_details(youtube,channel_id)

          st.write("CHANNEL DETAILS")
          st.write(channel_details)
        
          #calling a video functions to collect information to view
          playlist_id = play_list_id(youtube,channel_id)
          vdo_ids = get_video_ids(youtube,playlist_id)
          video_detail =  get_video_details(youtube,vdo_ids)
        
          st.write("VIDEO DETAILS")
          st.write(video_detail)
         
          #calling a comment function to collect information to view
          cmt_details = get_comments_details(youtube,vdo_ids)
        
          st.write("COMMENT DETAILS")
          st.write(cmt_details)
        except:
           st.error("ready to continue with migration")
             
#if user choice is migration and migrate to MongoDB
if option == "Migration" and migrate_option == "Migrate to MongoDB":    
  if user_inp in ch_name:
      st.info(":red[Data already exists]")
  else: 
      choice = st.radio("Can you enter the data in MongoDB?",['yes','no'])
      submit = st.button("submit")
 
#calling a function  to collect channel details for mongoDB
      channel_id = channel_details(user_inp)
      channel_details = channel_full_details(youtube,channel_id)
    
#create a collection to insert channel datas in mongoDB 
      col = db["channel"]
      col.insert_one(channel_details)

#calling a functions to collect video details for mongoDB 
      playlist_id = play_list_id(youtube,channel_id)
      vdo_ids = get_video_ids(youtube,playlist_id)  
      vdo_details = get_video_details(youtube,vdo_ids)
  
#create a collection to insert video datas in mongoDB
      col1 = db["video"]
      col1.insert_many(vdo_details)

#calling a function to collect comment details for mongoDB
      cmt_details = get_comments_details(youtube,vdo_ids)

#create a collection to insert comment datas in mongoDB
      col2 = db["comment"]
      col2.insert_many(cmt_details)   
      st.info("Data is successfully stored in MongoDB")

#Define a function to create tables
def create_sqlschema():
# create a table to insert channel datas
  query_channels = """create table if not  exists channels(
        Channel_name varchar,
        Channel_Id varchar PRIMARY KEY,
        Total_videos int,
        playlist_id varchar,
        subscribercount int,
        view_count int)"""
  mycursor.execute(query_channels)
  mydb.commit()
    
# create a  table to insert video datas    
  query_videos = """create table if not exists videos(
        channel_name varchar,
        channel_id varchar,
        video_id varchar PRIMARY KEY,
        video_Title varchar,
        video_view int,
        Duration float,
        published  varchar,
        like_count int,
        dislike_count int,
        comment_count int,
        CONSTRAINT fk_playlist FOREIGN KEY(channel_id) REFERENCES channels(channel_id) ON DELETE CASCADE)"""
  mycursor.execute(query_videos)
  mydb.commit()

# create a  table to insert comment datas
  query_comment = """create table if not exists comment(
        comment_id varchar PRIMARY KEY,
        video_id varchar,
        comment_text varchar,
        comment_author varchar,
        commment_posted_date varchar,
        comment_likes int,
        comment_replies int,
        CONSTRAINT fk_video FOREIGN KEY(video_id) REFERENCES videos(video_id) ON DELETE CASCADE)"""
  mycursor.execute(query_comment)
  mydb.commit() 
create_sqlschema()

#Define a function to insert channel datas into table
  
def insert_into_channels():
      col = db.channel
      query = """INSERT INTO channels VALUES(%s,%s,%s,%s,%s,%s)"""
      for i in col.find({'Channel_name':user_inp},{"_id":0}):
        t = tuple(i.values())
        mycursor.execute(query,t)
        mydb.commit()
   
        
#Define a function to insert video datas into table 
def insert_into_video():
      col1 = db.video
      query1 = """INSERT INTO videos VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
      for i in col1.find({'Channel_name':user_inp},{"_id":0}):
        mycursor.execute(query1, tuple(i.values()))
        mydb.commit()
  

#Define a function to insert comment datas into table
def insert_into_comment():
      col = db.video
      col1 = db.comment
      query2 = """INSERT INTO comment VALUES(%s,%s,%s,%s,%s,%s,%s)"""
      for vid in col.find({'Channel_name':user_inp},{'_id' : 0,}):
        for i in col1.find({'Video_id': vid['video_id']},{'_id' : 0}):
          t=tuple(i.values())
          mycursor.execute(query2,t)
          mydb.commit()

#Migrate datas from MongoDB to SQL
if  option == "Migration" and migrate_option == "Migrate to SQL":       

#Define a function to migrate datas from MongoDB to SQL  
  def insert_sql(user_inp):
    try:
  # calling a function  to collect channel details for SQL
      insert_into_channels(user_inp) 
  #calling a function  to collect video details for SQL    
      insert_into_video(user_inp)
  #calling a function  to collect comment details for SQL    
      insert_into_comment(user_inp) 
      st.success("Migrate Successfully.....!")
    except:
      st.error("This channel information is already exists")  
#Calling a function migrate datas from MongoDB to SQL      
  insert_sql(user_inp) 

#comparing each channel in different view based on questions
if option ==  "Questions":
    questions = st.selectbox(
        'what question you want to choose?',
        (    "What are the names of all the videos and their corresponding channels?",
        "Which channels have the most number of videos, and how many videos do they have?",
        "What are the top 10 most viewed videos and their respective channels?",
        "How many comments were made on each video, and what are their corresponding video names?",
        "Which videos have the highest number of likes, and what are their corresponding channel names?",
        "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
        "What is the total number of views for each channel, and what are their corresponding channel names?",
        "What are the names of all the channels that have published videos in the year 2022?",
        "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
        "Which videos have the highest number of comments, and what are their corresponding channel names?"))
    st.write('You selected:', questions)

#Define a function to execute queries
    def execute_query(questions):
        if questions ==  "What are the names of all the videos and their corresponding channels?":
                mycursor.execute("select video_Title , channel_name from videos")
                df = pd.DataFrame(mycursor.fetchall())
                df_re = df.rename(columns={0:'video_Title',1:'channel_name'})
                st.dataframe(df_re)

        elif questions == "Which channels have the most number of videos, and how many videos do they have?":
                mycursor.execute("select channel_name,Total_videos from channels order by Total_videos desc limit 5")
                df = pd.DataFrame(mycursor.fetchall())
                df_re = df.rename(columns={0:'channel_name',1:'Total_videos'})
                st.dataframe(df_re)
        
        elif questions == "What are the top 10 most viewed videos and their respective channels?":
                mycursor.execute("select channel_name ,video_view from videos order by video_view desc limit 10")
                df = pd.DataFrame(mycursor.fetchall())
                df_re = df.rename(columns={0:'channel_name',1:'Viewers'})
                st.dataframe(df_re)

        elif questions =="How many comments were made on each video, and what are their corresponding video names?":
                mycursor.execute("select channel_name, video_Title,comment_count from videos order by channel_name,video_Title" )
                df = pd.DataFrame(mycursor.fetchall())
                df_re = df.rename(columns={0:'channel_name',1:'video_Title',2:'comment_count'})
                st.dataframe(df_re)
        
        elif questions == "Which videos have the highest number of likes, and what are their corresponding channel names?":
                mycursor.execute("select channel_name,video_Title,like_count from videos order by like_count desc limit 5")
                df = pd.DataFrame(mycursor.fetchall())
                df_re = df.rename(columns={0:'channel_name',1:'video_title',2:'like_count'})
                st.dataframe(df_re)         
        
        elif questions == "What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
                mycursor.execute("select channel_name,video_Title,like_count,dislike_count from videos order by channel_name ")
                df = pd.DataFrame(mycursor.fetchall())
                df_re = df.rename(columns={0:'channel_name',1:'video_title',2:'like_count',3:'dislike_count'})
                st.dataframe(df_re)

        elif questions == "What is the total number of views for each channel, and what are their corresponding channel names?":
                mycursor.execute("select channel_name,view_count from channels order by channel_name")
                df = pd.DataFrame(mycursor.fetchall())
                df_re = df.rename(columns={0:'channel_name',1:'view_count'})
                st.dataframe(df_re)

        elif questions == "What are the names of all the channels that have published videos in the year 2022?":
                mycursor.execute("select channel_name,video_Title,published from videos where published like '2022%' order by channel_name ")
                df = pd.DataFrame(mycursor.fetchall())
                df_re = df.rename(columns={0:'channel_name',1:'video_Title',2:'published'})
                st.dataframe(df_re)

        elif questions == "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
                mycursor.execute("select channel_name, avg(duration) from videos group by channel_name")
                df = pd.DataFrame(mycursor.fetchall())
                df_re = df.rename(columns={1:'channel_name',1:'average Duration '})
                st.dataframe(df_re)
       
        elif questions == "Which videos have the highest number of comments, and what are their corresponding channel names?" :
                mycursor.execute("select channel_name,video_Title,comment_count from videos order by comment_count desc limit 10")
                df = pd.DataFrame(mycursor.fetchall())
                df_re = df.rename(columns={0:'channel_name',1:'video_Title',2:'comment_count'})
                st.dataframe(df_re)
                
    execute_query(questions)  
    st.success("Successfully Done",icon="✅")
