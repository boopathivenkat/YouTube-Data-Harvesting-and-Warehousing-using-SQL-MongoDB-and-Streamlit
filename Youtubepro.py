import pymongo
import psycopg2
import pandas as pd
import streamlit as st
import time
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build

Api_Id="AIzaSyAr1lXN-Dq_zChCXOD2m0LTbpTHVNl3kPc"
api_service_name = "youtube"
api_version = "v3"
youtube = build(api_service_name,api_version,developerKey=Api_Id)


def get_channel_info(channel_id):
    
    request = youtube.channels().list(
                part = "snippet,contentDetails,Statistics",
                id = channel_id)
            
    response1=request.execute()

    for i in range(0,len(response1["items"])):
        data = dict(
                    Channel_Name = response1["items"][i]["snippet"]["title"],
                    Channel_Id = response1["items"][i]["id"],
                    Subscription_Count= response1["items"][i]["statistics"]["subscriberCount"],
                    Views = response1["items"][i]["statistics"]["viewCount"],
                    Total_Videos = response1["items"][i]["statistics"]["videoCount"],
                    Channel_Description = response1["items"][i]["snippet"]["description"],
                    Playlist_Id = response1["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"],
                    )
        return data
    

def get_playlist_info(channel_id):
    All_data = []
    next_page_token = None
    next_page = True
    while next_page:

        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
            )
        response = request.execute()

        for item in response['items']: 
            data={'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'ChannelId':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']}
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            next_page=False
    return All_data


def get_channel_videos(channel_id):
    video_ids = []

    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list( 
                                           part = 'snippet',
                                           playlistId = playlist_id, 
                                           maxResults = 50,
                                           pageToken = next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids



def get_video_info(video_ids):

    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id= video_id)
        response = request.execute()

        for item in response["items"]:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet']['description'],
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics']['viewCount'],
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data




def get_comment_info(video_ids):

    Comment_Information = []

    for video_id in video_ids:
            try:

                request = youtube.commentThreads().list(
                        part = "snippet",
                        videoId = video_id,
                        maxResults = 50
                        )
                response= request.execute()
                
                for item in response["items"]:
                        comment_information = dict(
                                Comment_Id = item["snippet"]["topLevelComment"]["id"],
                                Video_Id = item["snippet"]["videoId"],                            
                                Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                                Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                        Comment_Information.append(comment_information)


    

            except HttpError as e:
                if e.resp.status == 403:
                    print(f"Comments are disabled for video ID {video_ids}.")
                else:
                    print(f"An error occurred while fetching comments for video ID {video_ids}: {e}")

    return Comment_Information



client = pymongo.MongoClient("mongodb+srv://boopathi:Boo758595@guvi.rozmoe3.mongodb.net/?retryWrites=true&w=majority&appName=Guvi")
db = client["Youtube_Pro"]
coll = db["Youtube_data"]


def mongo_upload(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_info(channel_id)
    vi_ids = get_channel_videos(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)

    coll = db["Youtube_data"]
    coll.insert_one({"channel_information":ch_details,"playlist_information":pl_details,"video_information":vi_details,
                     "comment_information":com_details})
    
    return "Channels all Data upload in Mongo DB completed successfully"





def channels_table():
    mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="758595",
            database= "Youtube",
            port = "5432"
            )
    cursor  = mydb.cursor ()

    create_query = '''
                    create table if not exists channels(Channel_Name varchar(100),
                    Channel_Id varchar(100) primary key, 
                    Subscription_Count bigint, 
                    Views bigint,
                    Total_Videos int,
                    Channel_Description text,
                    Playlist_Id varchar(100))
                    '''
    cursor .execute(create_query)
    mydb.commit()

    ch_list = []
    db = client["Youtube_Pro"]
    coll = db["Youtube_data"]
    for ch_data in coll.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        insert_query = '''insert into channels(Channel_Name,
                                                    Channel_Id,
                                                    Subscription_Count,
                                                    Views,
                                                    Total_Videos,
                                                    Channel_Description,
                                                    Playlist_Id)
                                        values(%s,%s,%s,%s,%s,%s,%s)'''
            

        values =(
                row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
                    
        cursor .execute(insert_query,values)
        mydb.commit()    



def playlists_table():
    mydb = psycopg2.connect(host="localhost",
        user="postgres",
        password="758595",
        database= "Youtube",
        port = "5432"
        )
    cursor  = mydb.cursor ()

    drop_query = "drop table if exists playlists"
    cursor .execute(drop_query)
    mydb.commit()


    create_query = '''create table if not exists playlists(PlaylistId varchar(100) primary key,
                    Title varchar(80), 
                    ChannelId varchar(100), 
                    ChannelName varchar(100),
                    PublishedAt timestamp,
                    VideoCount int
                    )'''
    cursor .execute(create_query)
    mydb.commit()


    db = client["Youtube_Pro"]
    coll = db["Youtube_data"]
    pl_list = []
    for pl_data in coll.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
                pl_list.append(pl_data["playlist_information"][i])
    df = pd.DataFrame(pl_list)

    for index,row in df.iterrows():
        insert_query = '''INSERT into playlists(PlaylistId,
                                                    Title,
                                                    ChannelId,
                                                    ChannelName,
                                                    PublishedAt,
                                                    VideoCount)
                                        VALUES(%s,%s,%s,%s,%s,%s)'''            
        values =(
                row['PlaylistId'],
                row['Title'],
                row['ChannelId'],
                row['ChannelName'],
                row['PublishedAt'],
                row['VideoCount'])
                
        cursor .execute(insert_query,values)
        mydb.commit()    



def videos_table():
    mydb = psycopg2.connect(host="localhost",
        user="postgres",
        password="758595",
        database= "Youtube",
        port = "5432"
        )
    cursor  = mydb.cursor ()

    drop_query = "drop table if exists videos"
    cursor .execute(drop_query)
    mydb.commit()

 
    create_query = '''create table if not exists videos(
                    Channel_Name varchar(150),
                    Channel_Id varchar(100),
                    Video_Id varchar(50) primary key, 
                    Title varchar(150), 
                    Tags text,
                    Thumbnail varchar(225),
                    Description text, 
                    Published_Date timestamp,
                    Duration interval, 
                    Views bigint, 
                    Likes bigint,
                    Comments int,
                    Favorite_Count int, 
                    Definition varchar(10), 
                    Caption_Status varchar(50) 
                    )''' 
                    
    cursor .execute(create_query)             
    mydb.commit()


    vi_list = []
    db = client["Youtube_Pro"]
    coll = db["Youtube_data"]

    for vi_data in coll.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)
        

    for index, row in df2.iterrows():
        insert_query = '''
                    INSERT INTO videos (Channel_Name,
                        Channel_Id,
                        Video_Id, 
                        Title, 
                        Tags,
                        Thumbnail,
                        Description, 
                        Published_Date,
                        Duration, 
                        Views, 
                        Likes,
                        Comments,
                        Favorite_Count, 
                        Definition, 
                        Caption_Status 
                        )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

                '''
        values = (
                    row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    row['Tags'],
                    row['Thumbnail'],
                    row['Description'],
                    row['Published_Date'],
                    row['Duration'],
                    row['Views'],
                    row['Likes'],
                    row['Comments'],
                    row['Favorite_Count'],
                    row['Definition'],
                    row['Caption_Status'])
                                
        cursor .execute(insert_query,values)
        mydb.commit()    


def comments_table():
    mydb = psycopg2.connect(host="localhost",
        user="postgres",
        password="758595",
        database= "Youtube",
        port = "5432"
        )
    cursor  = mydb.cursor ()

    drop_query = "drop table if exists comments"
    cursor .execute(drop_query)
    mydb.commit()

    try:
        create_query = '''CREATE TABLE if not exists comments(Comment_Id varchar(100) primary key,
                        Video_Id varchar(80),
                        Comment_Text text, 
                        Channel_Name varchar(100),                        
                        Comment_Author varchar(150),
                        Comment_Published timestamp)'''
        cursor .execute(create_query)
        mydb.commit()
        
    except:
        st.write("Comments Table already created")

    com_list = []
    db = client["Youtube_Pro"]
    coll = db["Youtube_data"]

    for com_data in coll.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)


    for index, row in df3.iterrows():
            insert_query = '''
                INSERT INTO comments (Comment_Id,
                                        Video_Id ,                                
                                        Comment_Text,
                                        Comment_Author,
                                        Comment_Published)
                VALUES (%s, %s, %s, %s, %s)

            '''
            values = (
                row['Comment_Id'],
                row['Video_Id'],            
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published']
            )
    cursor .execute(insert_query,values)
    mydb.commit()    


def tables_upload():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()

    return "Channels all Data Tables Create in Postgres SQL completed successfully"



def show_channels_table():
    ch_list = []
    db = client["Youtube_Pro"]
    coll = db["Youtube_data"]
    for ch_data in coll.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    channels_table = st.dataframe(ch_list)
    return channels_table

def show_playlists_table():
    db = client["Youtube_Pro"]
    coll = db["Youtube_data"]
    pl_list = []
    for pl_data in coll.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
                pl_list.append(pl_data["playlist_information"][i])
    playlists_table = st.dataframe(pl_list)
    return playlists_table

def show_videos_table():
    vi_list = []
    db = client["Youtube_Pro"]
    coll = db["Youtube_data"]
    for vi_data in coll.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    videos_table = st.dataframe(vi_list)
    return videos_table

def show_comments_table():
    com_list = []
    db = client["Youtube_Pro"]
    coll = db["Youtube_data"]
    for com_data in coll.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    comments_table = st.dataframe(com_list)
    return comments_table



st.balloons()
st.header("YouTube Data Harvesting and Warehousing",divider='rainbow')


with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/b/b8/YouTube_Logo_2017.svg",width=150)
    st.page_link("https://www.youtube.com/", label=":green[Click here! Get Channels Id from YouTube ]")
    st.subheader('', divider='rainbow')

    st.markdown('''
        :blue[Welcome] :gray[To] :orange[YouTube] :green[Data Harvesting] :blue[and] :violet[Warehousing]
        :rainbow[Project] :sunglasses:''')


    multi = ''' _______ YouTube is the world's most popular video-sharing platform, 
    with over 2 billion active users. It is avaluable source of data for businesses,
    researchers, and individuals. This project will demonstrate how toharvest and 
    warehouse YouTube data using SQL, MongoDB, and Streamlit

    :orange[Benefits:]

    1. This approach can be used to collect large amounts of data from YouTube.
    The data can be stored in avariety of ways, including MongoDB and SQL.

    2. The data can be analyzed using a variety of tools, including Streamlit.
    This approach can be used toidentify trends, make predictions, and improve decision-making.


    
    :orange[My Project guidelines:]

    1. Users Giving to the Chennels ID

    2. Retrieving Data From Youtube API

    3. Store the Data In Mongo DB Collection

    4. Migrating to a Postgres SQL warehouse

    5. Data Analysis

    6. SQL Queries
    '''
    st.markdown(multi)

 
    
channel_id = st.text_input("Enter the Channel id")

col1, col2 ,col3= st.columns(3)

if col1.button(":green[Collect and Store data]"):
    ch_ids=[]
    db = client["Youtube_Pro"]
    coll = db["Youtube_data"]
    for ch_data in coll.find({},{"_id":0,"channel_information":1}):
         ch_ids.append(ch_data["channel_information"]["Channel_Id"])
         
    if channel_id in ch_ids:
        st.success(":red[This Channels Id already exists, Please Enter the Other valid Channels ID ...]")

    else:
        insert=mongo_upload(channel_id) 
        st.success(insert) 


client = pymongo.MongoClient("mongodb+srv://boopathi:Boo758595@guvi.rozmoe3.mongodb.net/?retryWrites=true&w=majority&appName=Guvi")
db = client["Youtube_Pro"]
coll = db["Youtube_data"]


mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="758595",
            database= "Youtube",
            port = "5432"
            )
cursor  = mydb. cursor ()

def migrate_to_sql(channel_name):

    channel_data = coll.find_one({"channel_information.Channel_Name": channel_name}, {"_id": 0})
    if channel_data:

        columns = ", ".join(channel_data["channel_information"].keys())
        values_template = ", ".join(["%s"] * len(channel_data["channel_information"]))
        values = tuple(channel_data["channel_information"].values())
        

        cursor.execute(
            f"""
            INSERT INTO channels ({columns})
            VALUES ({values_template})
            """,
            values
        )
        mydb.commit()
        return "Data migrated successfully."
    else:
        return "Given channel details already exists."


all_channels = []
for ch_data in coll.find({}, {"_id": 0, "channel_information.Channel_Name": 1}):
    all_channels.append(ch_data["channel_information"]["Channel_Name"])

uni_channel = st.selectbox("Select the Channel", all_channels)

if col2.button(":blue[Migrate to SQL]"):
    result = migrate_to_sql(uni_channel)
    st.success(result)



def simulate_loading():
    with st.spinner('Page Refreshing ...'):
        time.sleep(0.25) 


if col3.button(':red[Page Refresh]'):
    simulate_loading()
    st.success('Page refresh successfully!')

show_table = st.radio("Choose the Table for view",((":green[Channels]"),(":orange[Playlists]"),(":violet[Videos]"),(":grey[Comments]")))

if show_table ==(":green[Channels]"): 
    show_channels_table()

elif show_table == (":orange[Playlists]"):
    show_playlists_table()

elif show_table == (":violet[Videos]"):
    show_videos_table()

elif show_table == (":grey[Comments]"):
    show_comments_table()



mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="758595",
            database= "Youtube",
            port = "5432"
            )
cursor  = mydb. cursor ()

question=st.selectbox("Select your question",("1. All the videos and the channel name",
                                            "2. channels with most number of videos",
                                            "3. 10 most viewed videos",
                                            "4. comments in each videos",
                                            "5. Videos with higest likes",
                                            "6. likes of all videos",
                                            "7. views of each channel",
                                            "8. videos published in the year of 2022",
                                            "9. average duration of all videos in each channel",
                                            "10. videos with highest number of comments"))

if question=="1. All the videos and the channel name":
    query1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

elif question=="2. channels with most number of videos":
    query2='''select channel_name as channelname,total_videos as no_videos from channels 
                order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)

elif question=="3. 10 most viewed videos":
    query3='''select views as views,channel_name as channelname,title as videotitle from videos 
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)

elif question=="4. comments in each videos":
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df4)

elif question=="5. Videos with higest likes":
    query5='''select title as videotitle,channel_name as channelname,likes as likecount
                from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question=="6. likes of all videos":
    query6='''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)

elif question=="7. views of each channel":
    query7='''select channel_name as channelname ,views as totalviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","totalviews"])
    st.write(df7)

elif question=="8. videos published in the year of 2022":
    query8='''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","published_date","channelname"])
    st.write(df8)

elif question=="9. average duration of all videos in each channel":
    query9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)

elif question=="10. videos with highest number of comments":
    query10='''select title as videotitle, channel_name as channelname,comments as comments from videos where comments is
                not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])
    st.write(df10)

