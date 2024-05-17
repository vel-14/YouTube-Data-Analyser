from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
from datetime import datetime as dt
from datetime import timedelta
import streamlit as st

def api_connect():
    api_service_name = "youtube"
    api_version = "v3"
    api_key="AIzaSyAFZK0eFhNctrb6Tq7XdIiYcZo3M5r85iA"
   
    youtube =build(api_service_name, api_version,developerKey=api_key )
    return youtube

youtube=api_connect()

def get_channel_info(channel_id):
    request=youtube.channels().list(
                                    part="snippet, statistics, contentDetails",
                                    id=channel_id).execute()
    for i in request["items"]:
        info=dict(channel_name=i["snippet"]["title"],
                channel_id=i["id"],
                subscriber_count=i["statistics"]["subscriberCount"],
                views=i["statistics"]["viewCount"],
                videos=i["statistics"]["videoCount"],
                description=i["snippet"]["description"],
                upload_id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return info

def get_playlist_info(channel_id):

        page_token=""
        playlist_info=[]

        request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=50,
                pageToken=page_token).execute()

        while True:
                for i in request["items"]:
                        data=dict(playlist_id=i["id"],
                                playlist_name=i["snippet"]["title"],
                                channel_id=i["snippet"]["channelId"],
                                channel_name=i["snippet"]["channelTitle"],
                                video_count=i["contentDetails"]["itemCount"],
                                published_at=i["snippet"]["publishedAt"])
                        playlist_info.append(data)
                
                page_token=request.get("nextPageToken")

                if not page_token:
                        break
        return playlist_info

def get_video_ids(channel_id):

    video_ids=[]
    nextPageToken=''

    request=youtube.channels().list(
                                    part="contentDetails",
                                    id=channel_id).execute()

    while True:
        response=youtube.playlistItems().list(
                                            part="snippet",
                                            playlistId=request["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"],
                                            pageToken=nextPageToken,
                                            maxResults=50).execute()
        for i in range(len(response["items"])):
            video_ids.append(response["items"][i]["snippet"]["resourceId"]["videoId"])
        
        nextPageToken=response.get("nextPageToken")

        if not nextPageToken:
            break

    return video_ids

def get_video_info(video_ids):
    video_info=[]
    
    for videoid in video_ids:
        request=youtube.videos().list(
                                    part="snippet,contentDetails,statistics",
                                    id=videoid).execute()
        for i in request["items"]:
            data=dict(channel_name=i["snippet"]["channelTitle"],
                    channel_id=i["snippet"]["channelId"],
                    video_id=i["id"],
                    video_title=i["snippet"]["title"],
                    tags=i["snippet"].get("tags"),
                    thumbnail=i["snippet"]["thumbnails"]["default"]["url"],
                    description=i["snippet"].get("description"),
                    published_at=i["snippet"]["publishedAt"],
                    duration=i["contentDetails"]["duration"],
                    views=i["statistics"].get("viewCount"),
                    likes=i["statistics"].get("likeCount"),
                    comments=i["statistics"].get("commentCount"),
                    favourite_count=i["statistics"]["favoriteCount"],
                    definition=i["contentDetails"]["definition"],
                    caption=i["contentDetails"]["caption"])
            video_info.append(data)

    return video_info

def get_comment_info(video_ids):
    
    comment_info=[]
    try:
        for videoid in video_ids:
            request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=videoid,
                    maxResults=50).execute()

            for i in request["items"]:
                data = dict(comment_id=i["id"],
                            video_id=i["snippet"]["videoId"],
                            comment=i["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                            comment_author=i["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                            published_at=i["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
                comment_info.append(data)
    except:
        pass
    return comment_info

#connecting to mongodb
uri= "mongodb://localhost:27017/"
client=pymongo.MongoClient(uri)
db=client["YouTube_Data_Harvest"]
coll=db["YouTube_Channels"]

#inserting into mongoDB

def insert_into_mongoDB(channel_id):
    ch_info=get_channel_info(channel_id)
    pl_info=get_playlist_info(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_info=get_video_info(vi_ids)
    comm_info=get_comment_info(vi_ids)

    data={"channel_data":ch_info,"playlist_data":pl_info,
          "video_data":vi_info,"comment_data":comm_info}
    
    coll.insert_one(data)
    return "uploading completed"

mydb = mysql.connector.connect(
host="localhost",
user="root",
password="Velcharru@1406",
database="YouTube_Data_Harvesting",
auth_plugin='mysql_native_password')

cursor=mydb.cursor()


def create_channels_table():
  query='''drop table if exists channels'''
  cursor.execute(query)
  mydb.commit()

  create = '''create table if not exists channels(channel_name varchar(100),
                                                    channel_id varchar(100) primary key,
                                                    subscriber_count bigint,
                                                    views bigint,
                                                    videos int,
                                                    description text,
                                                    upload_id varchar(100))'''
  cursor.execute(create)
  mydb.commit()


  db = client["YouTube_Data_Harvest"]
  coll=db["YouTube_Channels"]
  channel=[]
  for ch_data in coll.find({},{"_id":0,"channel_data":1}):
      channel.append(ch_data["channel_data"])
  df=pd.DataFrame(channel)

  for ind,row in df.iterrows():
      insert_query='''insert into channels(channel_name,
                                          channel_id,
                                          subscriber_count,
                                          views,
                                          videos,
                                          description,
                                          upload_id)
                                          values(%s,%s,%s,%s,%s,%s,%s)'''
      values=(row["channel_name"],
              row["channel_id"],
              row["subscriber_count"],
              row["views"],
              row["videos"],
              row["description"],
              row["upload_id"])
      
  
      cursor.execute(insert_query,values)
      mydb.commit()

def create_playlists_table():
    query='''drop table if exists playlists'''
    cursor.execute(query)
    mydb.commit()

    create = '''create table if not exists playlists(playlist_id varchar(100) primary key,
                                                    playlist_name varchar(100),
                                                    channel_id varchar(100),
                                                    channel_name varchar(100),
                                                    video_count int,
                                                    published_at timestamp)'''
    cursor.execute(create)
    mydb.commit()

    db=client["YouTube_Data_Harvest"]
    coll=db["YouTube_Channels"]

    playlist=[]
    for pl_data in coll.find({},{"_id":0,"playlist_data":1}):
        for i in pl_data["playlist_data"]:
            playlist.append(i)
    df=pd.DataFrame(playlist)

    for ind,ser in df.iterrows():

        correct_format= dt.fromisoformat(ser['published_at'].replace('Z','+00:00'))
        published = correct_format.strftime('%Y-%m-%d %H:%M:%S')

        insert_query = ''' insert into playlists(playlist_id,
                                                playlist_name,
                                                channel_id,
                                                channel_name,
                                                video_count,
                                                published_at)
                                                
                                        values(%s,%s,%s,%s,%s,%s)'''
        values = (ser['playlist_id'],
                ser['playlist_name'],
                ser['channel_id'],
                ser['channel_name'],
                ser['video_count'],
                published)

    cursor.execute(insert_query,values)
    mydb.commit()

def create_videos_table():
    query= '''drop table if exists videos'''
    cursor.execute(query)
    mydb.commit()

    create = '''create table if not exists videos(channel_name varchar(200),
                                                channel_id varchar(200),
                                                video_id varchar(200) primary key,
                                                video_title varchar(200),
                                                tags text,
                                                thumbnail varchar(200),
                                                description text,
                                                published_at timestamp,
                                                duration time,
                                                views bigint,
                                                likes bigint,
                                                comments bigint,
                                                favourite_count int,
                                                definition varchar(100),
                                                caption varchar(100))'''
    cursor.execute(create)
    mydb.commit()

    db=client["YouTube_Data_Harvest"]
    coll=db["YouTube_Channels"]

    vids=[]

    for vid_data in coll.find({},{"_id":0,"video_data":1}):
        for i in vid_data['video_data']:
            vids.append(i)

    df = pd.DataFrame(vids)

    def convert_duration(dur):
        dur=dur[2:]

        hour=0
        min=0
        sec=0

        if "H" in dur:
            hour_str,remain=dur.split("H")
            hour=int(hour_str)
            dur=remain
        if "M" in dur:
            min_str,remain = dur.split("M")
            min=int(min_str)
            dur=remain
        if "S" in dur:
            sec_str,remain= dur.split("S")
            sec=int(sec_str)
        
        formated_duration = timedelta(hours=hour,minutes=min,seconds=sec)
        return formated_duration


    for ind,ser in df.iterrows():
        if ser['tags']:
            convert_list= ','.join(str(tag) for tag in ser['tags'])
        else:
            convert_list='' 

        convert_date= dt.fromisoformat(ser["published_at"].replace('Z','+00:00'))
        published_date = convert_date.strftime('%Y-%m-%d %H:%M:%S')
        duration = convert_duration(ser["duration"])
        
        insert_query='''insert into videos(channel_name,
                                            channel_id,
                                            video_id,
                                            video_title,
                                            tags,
                                            thumbnail,
                                            description,
                                            published_at,
                                            duration,
                                            views,
                                            likes,
                                            comments,
                                            favourite_count,
                                            definition,
                                            caption)
                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(ser['channel_name'],
                ser['channel_id'],
                ser['video_id'],
                ser['video_title'],
                convert_list,
                ser['thumbnail'],
                ser['description'],
                published_date,
                duration,
                ser['views'],
                ser['likes'],
                ser['comments'],
                ser['favourite_count'],
                ser['definition'],
                ser['caption'])
        
        cursor.execute(insert_query,values)
        mydb.commit()                          

def create_comments_table():
    query = '''drop table if exists comments'''
    cursor.execute(query)
    mydb.commit()

    create = '''create table if not exists comments(comment_id varchar(100) primary key,
                                                    video_id varchar(100),
                                                    comment text,
                                                    comment_author varchar(200),
                                                    published_at timestamp)'''
    cursor.execute(create)
    mydb.commit()

    db = client['YouTube_Data_Harvest']
    coll = db['YouTube_Channels']

    comments=[]

    for comm_data in coll.find({},{"_id":0,"comment_data":1}):
        for i in comm_data["comment_data"]:
            comments.append(i)
    df = pd.DataFrame(comments)

    for ind,ser in df.iterrows():
        published_data=dt.fromisoformat(ser["published_at"].replace("Z","+00:00"))
        conv_date=published_data.strftime("%Y-%m-%d %H:%M:%S")

        insert_query = '''INSERT INTO comments(comment_id, 
                                            video_id,
                                            comment,
                                            comment_author,
                                            published_at)
                    VALUES (%s, %s, %s, %s, %s)'''
        values = (
        ser["comment_id"],
        ser["video_id"],
        ser["comment"],
        ser["comment_author"],
        conv_date)

        cursor.execute(insert_query,values)
        mydb.commit()

def create_mysql_tables():
    create_channels_table()
    create_playlists_table()
    create_videos_table()
    create_comments_table()
    return "Data transferred Successfully"

def show_channels():
    db = client["YouTube_Data_Harvest"]
    coll=db["YouTube_Channels"]
    channel=[]
    for ch_data in coll.find({},{"_id":0,"channel_data":1}):
        channel.append(ch_data["channel_data"])
    df=st.dataframe(channel)
    return df

def show_playlists():
    db = client["YouTube_Data_Harvest"]
    coll=db["YouTube_Channels"]
    playlist=[]
    for pl_data in coll.find({},{"_id":0,"playlist_data":1}):
        for i in pl_data["playlist_data"]:
            playlist.append(i)
    df=st.dataframe(playlist)
    return df

def show_videos():

    db=client["YouTube_Data_Harvest"]
    coll=db["YouTube_Channels"]

    vids=[]

    for vid_data in coll.find({},{"_id":0,"video_data":1}):
        for i in vid_data['video_data']:
            vids.append(i)

    df = st.dataframe(vids)
    return df

def show_comments():    
    db = client['YouTube_Data_Harvest']
    coll = db['YouTube_Channels']

    comments=[]

    for comm_data in coll.find({},{"_id":0,"comment_data":1}):
        for i in comm_data["comment_data"]:
            comments.append(i)
    df = st.dataframe(comments)
    return df

with st.sidebar:
    st.title(":red[YouTube Data Harvesting and Warehousing]")
    st.header("Here you can obtain data from YouTube channels and get insights about the channels")
channel=st.text_input("Enter the Channel_Id")

if st.button("Harvest Data and store it to MongoDb"):
    channel_id = channel  
    existing_data = coll.find_one({"channel_data.channel_id": channel_id})
    if existing_data:
        st.success("Data already exists for the given channel")
    else:
        insert = insert_into_mongoDB(channel_id)
        st.success(insert)

if st.button("Transfer the data to MySql"):
    tables=create_mysql_tables()
    st.success(tables)

show_table = st.radio("Select the Table",("Channels","Playlists","Videos","Comments"))

if show_table == "Channels":
    show_channels()
elif show_table=="Playlists":
    show_playlists()
elif show_table=="Videos":
    show_videos()
elif show_table == "Comments":
    show_comments()

question = st.selectbox("Select Question",("1.Name of all videos and its channel names",
                                           "2.Channels with most videos and its count",
                                           "3.Top10 most viewed videos and their channels",
                                           "4.Number of comments in each video and its channel name",
                                           "5.Videos with highest number of likes and their channels",
                                           "6.Number of likes and dislikes for each video and its title",
                                           "7.Number of views for each channel and its name",
                                           "8.Name of the channels which published videos on 2022",
                                           "9.Average duration of videos in each channel and its name",
                                           "10.Videos with higest comments and its channel name"))
            
if question=="1.Name of all videos and its channel names":
    query = '''select video_title,channel_name from videos'''
    cursor.execute(query)
    ans1=cursor.fetchall()
    mydb.commit()

    df1=pd.DataFrame(ans1,columns=["Video Title","Channel Name"])
    st.write(df1)
elif question =="2.Channels with most videos and its count":
    query = '''select channel_name,videos from channels order by videos desc'''
    cursor.execute(query)
    ans2=cursor.fetchall()
    mydb.commit()

    df2=pd.DataFrame(ans2,columns=["Channel Name","Video Count"])
    st.write(df2)

elif question=="3.Top10 most viewed videos and their channels":
    query = '''select channel_name,video_title,views from videos where views is not null order by views desc limit 10'''
    cursor.execute(query)
    ans3=cursor.fetchall()
    mydb.commit()

    df3=pd.DataFrame(ans3,columns=["Channel Name","Video Title","Views count"])
    st.write(df3)
elif question == "4.Number of comments in each video and its channel name":
    query = '''select comments,video_title,channel_name from videos where comments is not null'''
    cursor.execute(query)
    ans4=cursor.fetchall()
    mydb.commit()

    df4=pd.DataFrame(ans4,columns=["Comments count","Video Title","Channel Name"])
    st.write(df4)
elif question == "5.Videos with highest number of likes and their channels":
    query = '''select likes,video_title,channel_name from videos where likes is not null order by likes desc'''
    cursor.execute(query)
    ans5=cursor.fetchall()
    mydb.commit()

    df5=pd.DataFrame(ans5,columns=["Likes Count","Video Title","Channel Name"])
    st.write(df5)
elif question == "6.Number of likes and dislikes for each video and its title":
    query = '''select likes,video_title,channel_name from videos where likes is not null'''
    cursor.execute(query)
    ans6=cursor.fetchall()
    mydb.commit()

    df6=pd.DataFrame(ans6,columns=["Likes Count","Video Title","Channel Name"])
    st.write(df6)
elif question == "7.Number of views for each channel and its name":
    query = '''select views,channel_name from channels'''
    cursor.execute(query)
    ans7=cursor.fetchall()
    mydb.commit()

    df7=pd.DataFrame(ans7,columns=["No. of Views","Channel Name"])
    st.write(df7)
elif question == "8.Name of the channels which published videos on 2022":
    query = '''select published_at,video_title,channel_name from videos where extract(year from published_at)=2022'''
    cursor.execute(query)
    ans8=cursor.fetchall()
    mydb.commit()

    df8=pd.DataFrame(ans8,columns=["Published Date","Video Title","Channel Name"])
    st.write(df8)
elif question == "9.Average duration of videos in each channel and its name":
    query = '''select channel_name, sec_to_time(avg(duration)) 
                from videos 
                group by channel_name'''
    cursor.execute(query)
    ans9=cursor.fetchall()
    mydb.commit()

    df9=pd.DataFrame(ans9,columns=["Channel Name","Average Duration"])
    st.write(df9)
elif question=="10.Videos with higest comments and its channel name":
    query = '''select comments,video_title,channel_name 
            from videos
            where comments is not null order by comments desc'''
    cursor.execute(query)
    ans10=cursor.fetchall()
    mydb.commit()

    df10=pd.DataFrame(ans10,columns=["Comments Count","Video Title","Channel Name"])
    st.write(df10)


    



