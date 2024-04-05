#Capstone-1
#YOUTUBE DATA HARVESTING AND WAREHOUSING

from googleapiclient.discovery import build
import pandas as pd
import mysql.connector
import streamlit as st
import re
from datetime import datetime

# Establish connection to YouTube API
def api_connect():
    api_key = "AIzaSyClVOUgAbirQfrqo4f8VIYoamWekZTW2kE"  
    youtube = build("youtube", "v3", developerKey=api_key)
    return youtube

youtube = api_connect()

# Function to fetch channel information
def channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id,
    )
    response = request.execute()
    channel_data = []
    
    for item in response['items']:
        data = {
            'channel_name': item["snippet"]["title"],
            'channel_id': item["id"],
            'subscribers': item["statistics"]["subscriberCount"],
            'views': item["statistics"]["viewCount"],
            'total_views': item["statistics"]["videoCount"],
            'channel_description': item["snippet"]["description"],
            'playlist_id': item["contentDetails"]["relatedPlaylists"]["uploads"]
        }
        channel_data.append(data)

    return channel_data

# Function to fetch video IDs from a channel
def video_id(channel_id):
    try:
        request = youtube.channels().list(
            part="contentDetails",
            id=channel_id
        )
        response = request.execute()
        video_ids = []
        playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

        next_page_token = None
        while True:
            request = youtube.playlistItems().list(
                part="snippet",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response1 = request.execute()
            for item in response1.get("items", []):
                video_ids.append(item["snippet"]["resourceId"]["videoId"])

            next_page_token = response1.get("nextPageToken")

            if next_page_token is None:
                break

        return video_ids

    except KeyError:
        print("Error: Could not retrieve video IDs.")

# Function to convert duration from YouTube API format to HH:MM:SS
def convert_duration(duration):
    regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
    match = re.match(regex, duration)
    if not match:
        return '00:00:00'
    hours, minutes, seconds = match.groups()
    hours = int(hours[:-1]) if hours else 0
    minutes = int(minutes[:-1]) if minutes else 0
    seconds = int(seconds[:-1]) if seconds else 0
    total_seconds = hours * 3600 + minutes * 60 + seconds
    return '{:02d}:{:02d}:{:02d}'.format(int(total_seconds / 3600), int((total_seconds % 3600) / 60), int(total_seconds % 60))
#Function to fetch video information
def video_info(videoids):
    video_data = []

    for video_id in videoids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()

        for j in response["items"]:
            data = {
                "channel_name": j["snippet"]["channelTitle"],
                "channel_Id": j["snippet"]["channelId"],
                "video_id": j["id"],
                "video_title": j["snippet"]["title"],
                "thumbnail": j["snippet"]["thumbnails"]["default"]["url"],
                "description": j["snippet"].get("description"),
                "published_date": j["snippet"]["publishedAt"],
                "duration": convert_duration(j["contentDetails"]["duration"]),  # Call convert_duration function
                "views": j["statistics"].get("viewCount"),
                "likes": j["statistics"].get("likeCount"),
                "comment": j["statistics"].get("commentCount",0),
                "favorite_count": j["statistics"]["favoriteCount"],
                "caption_status": j["contentDetails"]["caption"]
            }
            video_data.append(data)
    return video_data

#Function to fetch playlist information
import pandas as pd

def playlist_info(channelids):
    playlists=[]
    next_page_token=None
    while True:
        request = youtube.playlists().list(part="snippet,contentDetails",
                        channelId=channelids,
                        maxResults=50,
                        pageToken=next_page_token
                        )

        response=request.execute()
        for i in response["items"]:
            data = {
                "playlist_id": i["id"],
                "playlist_title": i["snippet"]["title"],
                "channel_id":i["snippet"]["channelId"],
                "channel_name": i["snippet"]["channelTitle"],
                "published": i["snippet"]["publishedAt"],
                "video_count": i["contentDetails"]["itemCount"]
            }
            playlists.append(data)
        
        next_page_token = response.get("nextPageToken")

        if not next_page_token:
            break

    return playlists

# Function to fetch comments information
def comment_info(video_ids):
    comments = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id
            )
            response = request.execute()

            for i in response["items"]:
                data = {
                    "comment_id": i["snippet"]["topLevelComment"]["id"],
                    "comment_text": i["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                    "video_id": i["snippet"]["topLevelComment"]["snippet"]["videoId"],
                    "comment_author": i["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    "comment_published": i["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                }
                comments.append(data)
    except Exception as e:
        print("Error fetching comments:", e)
    return comments

# Function to establish connection with MySQL database
def connect_to_mysql():
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Sandy@2914",
        auth_plugin="mysql_native_password",
        database='youtube_database'
    )
    return db
# Table creation for channels,playlists,videos,comments
def create_channel_table(cursor):
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS channels (
                          channel_name VARCHAR(100),
                          channel_id VARCHAR(80) PRIMARY KEY,
                          views BIGINT,
                          subscribers BIGINT,
                          video_count BIGINT,
                          channel_description TEXT,
                          playlist_id VARCHAR(80)
                        )'''
        cursor.execute(create_query)
        print("Channel table created successfully.")
    except mysql.connector.Error as err:
        print("Error:", err)

# Function to insert channel data into MySQL table
def insert_channel_data(cursor, db, channel_df):
    channel_sql = "INSERT INTO channels VALUES (%s, %s, %s, %s, %s, %s, %s)"
    for i in range(len(channel_df)):
        cursor.execute(channel_sql, tuple(channel_df.iloc[i]))
        db.commit()
def query_channel(cursor):
    query = "SELECT * FROM channels"
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows

# Table creation for videos
def create_video_table(cursor):
    try:
        # SQL query to create the VIDEO table
        create_query = '''CREATE TABLE IF NOT EXISTS videos(
                            channel_name VARCHAR(100) ,
                            channel_Id VARCHAR(100) primary key,
                            video_id VARCHAR(30),
                            video_title VARCHAR(150),
                            thumbnail  VARCHAR(200),
                            description TEXT,
                            published_date TIMESTAMP,
                            duration VARCHAR(20),
                            views BIGINT,
                            likes BIGINT,
                            comment INT,
                            favorite_count INT,
                            caption_status VARCHAR(50)
                            )'''
                        
        cursor.execute(create_query)
        db.commit()
        print("Videos table created successfully.")
    except mysql.connector.Error as err:
        db.rollback()
        print("Error:", err)
from datetime import datetime
# Function to insert channel data into MySQL table
video_sql = """
    INSERT INTO youtube_database.videos
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
def insert_video_data(cursor, db, video_df):
    try:
        for i in range(len(video_df)):
            # Convert pandas Series to a list
            data_row = video_df.iloc[i].tolist()

            # Convert published datetime to the format expected by MySQL
            published_value = datetime.strptime(video_df.iloc[i]['published_date'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

            # Replace the published value in the data row
            data_row[6] = published_value

            # If comment count is None, replace with 0
            comment_count = data_row[11]
            if comment_count is None:
                comment_count = 0
            else:
                # Convert comment count to integer
                comment_count = int(comment_count)

            cursor.execute(video_sql, tuple(data_row[:-1] + [comment_count]))  # Use all data except the last element, and append comment count

        db.commit()
        print("Videos data inserted into the videos table successfully.")

    except mysql.connector.Error as err:
        db.rollback()
        print("Error:", err)

    finally:
        cursor.close()
        db.close()

def query_video(cursor):
    query = "SELECT * FROM videos"
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows

# Table creation for playlist
def create_playlist_table(cursor):
    try:
        # SQL query to create the playlist table
        create_query = '''CREATE TABLE IF NOT EXISTS playlist (
                            playlist_id VARCHAR(100) PRIMARY KEY,
                            playlist_title VARCHAR(80),
                            channel_id VARCHAR(100),
                            channel_name VARCHAR(100),
                            published TIMESTAMP,
                            video_count INT
                        )'''
        
        cursor.execute(create_query)
        
        db.commit()
        print("Playlist table created successfully.")

    except mysql.connector.Error as err:
        db.rollback()
        print("Error:", err)


# Function to insert playlist data into MySQL table

def insert_playlist_data(cursor, db, playlist_df):
    playlist_sql = """
        INSERT INTO youtube_database.playlist 
        VALUES (%s, %s, %s, %s, %s, %s) 
        ON DUPLICATE KEY UPDATE 
        playlist_title=VALUES(playlist_title), 
        channel_id=VALUES(channel_id), 
        channel_name=VALUES(channel_name), 
        published=VALUES(published), 
        video_count=VALUES(video_count)
    """

    try:
        for i in range(len(playlist_df)):
            # Convert numpy.int32 to standard Python int
            data_row = playlist_df.iloc[i].tolist()
            data_row[5] = int(data_row[5])  # Convert the 6th element (video_count) to int

            # Convert published datetime to the format expected by MySQL
            published_value = datetime.strptime(playlist_df.iloc[i]['published'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

            # Replace the published value in the data row
            data_row[4] = published_value

            cursor.execute(playlist_sql, tuple(data_row))
        db.commit()
        print("Data inserted into the playlist table successfully.")

    except mysql.connector.Error as err:
        db.rollback()
        print("Error:", err)

    finally:
        # Close the cursor and database connection
        cursor.close()
        db.close()

def query_playlist(cursor):
    query = "SELECT * FROM playlist"
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows

# Table creation for comment
def create_comment_table(cursor):
    try:
        # SQL query to create the comment table
        create_query = '''CREATE TABLE IF NOT EXISTS comments (
                            comment_id VARCHAR(100) PRIMARY KEY,
                            comment_text TEXT,
                            video_id VARCHAR(50),
                            comment_author VARCHAR(150),
                            comment_published DATETIME
                        )'''

        cursor.execute(create_query)

        db.commit()
        print("COMMENTS table created successfully.")

    except mysql.connector.Error as err:
        db.rollback()
        print("Error:", err)
# Function to insert playlist data into MySQL table

def insert_comment_data(cursor, db, comment_df):
    from datetime import datetime

    comment_sql = """
        INSERT INTO youtube_database.comments (comment_id, comment_text, video_id, comment_author, comment_published)
        VALUES (%s, %s, %s, %s, %s)
    """

    try:
        for i in range(len(comment_df)):
            # Convert pandas Series to a list
            data_row = comment_df.iloc[i].tolist()

            # Convert published datetime to the format expected by MySQL
            published_value = datetime.strptime(data_row[4], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

            # Replace the published value in the data row
            data_row[4] = published_value

            # Execute the SQL query to insert data into the comments table
            cursor.execute(comment_sql, tuple(data_row[:5]))  # Passing only the first 5 elements

        db.commit()
        print("Comments data inserted into the table successfully.")

    except mysql.connector.Error as err:
        db.rollback()
        print("Error:", err)

    finally:
        cursor.close()
        db.close()

def query_comment(cursor):
    query = "SELECT * FROM comments"
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows


# Streamlit UI
st.title("YouTube Data Harvesting and Warehousing")
st.markdown("<h1 style='font-size:24px;'>YouTube Channel Data Migration</h1>", unsafe_allow_html=True)

#migration for channel
channel_id = st.text_input("Enter the channel ID:")
if st.button("Migrate Channel"):  # Unique key assigned
    if not channel_id:
        st.error("Please enter a valid channel ID.")
    else:
        # Establish connection to MySQL
        db = connect_to_mysql()
        cursor = db.cursor()

        # Create channels table if not exists
        create_channel_table(cursor)

        # Fetch channel data from YouTube API
        channel_data = channel_info(channel_id)

        # Convert to DataFrame
        channel_df = pd.DataFrame(channel_data)

        # Insert channel data into MySQL table
        insert_channel_data(cursor, db, channel_df)

        # Close cursor and database connection
        cursor.close()
        db.close()

#migration for video
if st.button("Migrate Video"):  # Unique key assigned
    if not channel_id:
        st.error("Please enter a valid channel ID.")
    else:
        # Establish connection to MySQL
        db = connect_to_mysql()
        cursor = db.cursor()

        # Create videos table if not exists
        create_video_table(cursor)

        # Fetch video IDs from the channel
        ids = video_id(channel_id)

        # Call the video_info function with the retrieved video IDs
        video_data = video_info(ids)

        # Convert to DataFrame
        video_df = pd.DataFrame(video_data)

        # Insert channel data into MySQL table
        insert_video_data(cursor, db, video_df)

        # Close cursor and database connection
        cursor.close()
        db.close()

# Migration for playlist
if st.button("Migrate Playlist"):  # Unique key assigned
    if not channel_id:
        st.error("Please enter a valid channel ID.")
    else:
        # Establish connection to MySQL
        db = connect_to_mysql()
        cursor = db.cursor()

        # Create playlist table if not exists
        create_playlist_table(cursor)

        # Fetch playlist information from the channel
        playlist_data = playlist_info(channel_id)

        # Convert to DataFrame
        playlist_df = pd.DataFrame(playlist_data)

        # Insert playlist data into MySQL table
        insert_playlist_data(cursor, db, playlist_df)

        # Close cursor and database connection
        cursor.close()
        db.close()


# Migration for comment
if st.button("Migrate comment"):  # Unique key assigned
    if not channel_id:
        st.error("Please enter a valid channel ID.")
    else:
        # Establish connection to MySQL
        db = connect_to_mysql()
        cursor = db.cursor()

        # Create videos table if not exists
        create_video_table(cursor)

        # Fetch video IDs from the channel
        ids = video_id(channel_id)

        # Call the video_info function with the retrieved video IDs
        comment_data = comment_info(ids)

        # Convert to DataFrame
        comment_df = pd.DataFrame(comment_data)

        # Insert channel data into MySQL table
        insert_comment_data(cursor, db, comment_df)

        # Close cursor and database connection
        cursor.close()
        db.close()


# Display selected table
show_table = st.radio("SELECT THE TABLE FOR VIEW", ("CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"))

if show_table == "CHANNELS":
    db = connect_to_mysql()
    cursor = db.cursor()
    create_channel_table(cursor)
    rows = query_channel(cursor)
    if rows:
        st.write("Channels Table:")
        df = pd.DataFrame(rows, columns=['Channel Name', 'Channel ID', 'Views', 'Subscribers', 'Total Views', 'Channel Description', 'Playlist ID'])
        st.write(df)
    else:
        st.write("No data available for channels")
    cursor.close()
    db.close()
elif show_table == "PLAYLISTS":
    db = connect_to_mysql()
    cursor = db.cursor()
    rows = query_playlist(cursor)
    if rows:
        st.write("Playlist Table:")
        df = pd.DataFrame(rows, columns=['playlist_id', 'playlist_title', 'channel_id', 'channel_name', 'published', 'video_count'])
        st.write(df)
    else:
        st.write("No data available for playlists")
    cursor.close()
    db.close()

elif show_table == "VIDEOS":
    db = connect_to_mysql()
    cursor = db.cursor()
    rows = query_video(cursor)
    if rows:
        st.write("Videos Table:")
        df = pd.DataFrame(rows, columns=['Channel Name', 'Channel ID', 'Video ID', 'Video Title', 'Thumbnail', 'Description', 'Published Date', 'Duration', 'Views', 'Likes', 'Comments', 'Favorite Count', 'Caption Status'])
        st.write(df)
    else:
        st.write("No data available for videos")
    cursor.close()
    db.close()
elif show_table == "COMMENTS":
    db = connect_to_mysql()
    cursor = db.cursor()
    rows = query_comment(cursor)
    if rows:
        st.write("Comments Table:")
        df = pd.DataFrame(rows, columns=['comment_id', 'comment_text', 'video_id', 'comment_author', 'comment_published'])
        st.write(df)
    else:
        st.write("No data available for comments")
    cursor.close()
    db.close()
  #---------------------------------------------------------------------------------------------------
  # query for 10 questions:
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Sandy@2914",
    auth_plugin="mysql_native_password",
    database='youtube_database'
)

# Create a cursor object to execute SQL queries
cursor = db.cursor()


question=st.selectbox("select your question",("1.What are the names of all the videos and their corresponding channels?",
                                               "2.Which channels have the most number of videos, and how many videos do they have?",
                                               "3.What are the top 10 most viewed videos and their respective channels?",
                                               "4.How many comments were made on each video, and what are their corresponding video names?",
                                               "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                               "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                               "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                               "8.What are the names of all the channels that have published videos in the year 2022?",
                                               "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                               "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))
if question=="1.What are the names of all the videos and their corresponding channels?":

    cursor = db.cursor()
    query1 = '''SELECT video_title AS Title, channel_name AS Channel FROM videos'''
    cursor.execute(query1)
    q1 = cursor.fetchall()
    df1 = pd.DataFrame(q1, columns=["Title", "Channel"])
    st.write(df1)
elif question == "2.Which channels have the most number of videos, and how many videos do they have?":
    cursor = db.cursor()
    query2 = '''SELECT channel_name as channel, video_count as total_video FROM channels ORDER BY video_count DESC'''
    cursor.execute(query2)
    q2 = cursor.fetchall()
    df2 = pd.DataFrame(q2, columns=["Channel", "video_count"])
    st.write(df2)
elif question == "3.What are the top 10 most viewed videos and their respective channels?":
    cursor = db.cursor()
    query3 = '''SELECT channel_name as channel, video_title as title, views as views FROM videos ORDER BY views DESC LIMIT 10'''
    cursor.execute(query3)
    q3 = cursor.fetchall()
    df3 = pd.DataFrame(q3, columns=["Channel", "video title","views"])
    st.write(df3)

elif question == "4.How many comments were made on each video, and what are their corresponding video names?":
    cursor = db.cursor()
    query4 = '''SELECT channel_name as channel, video_title as title, comment as comments FROM videos where comment is not null'''
    cursor.execute(query4)
    q4 = cursor.fetchall()
    df4 = pd.DataFrame(q4, columns=["Channel", "video title","no.of.comments"])
    st.write(df4)
elif question =="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    cursor = db.cursor()
    query5 = '''SELECT channel_name as channel, video_title as title, likes as likes FROM videos order by likes desc'''
    cursor.execute(query5)
    q5= cursor.fetchall()
    df5 = pd.DataFrame(q5, columns=["Channel", "video title","likes"])
    st.write(df5)
elif question =="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    cursor = db.cursor()
    query6 = '''SELECT video_title as title, likes as likes FROM videos'''
    cursor.execute(query6)
    q6= cursor.fetchall()
    df6= pd.DataFrame(q6, columns=["video title","likes"])
    st.write(df6)
elif question =="7.What is the total number of views for each channel, and what are their corresponding channel names?":
    cursor = db.cursor()
    query7 = '''SELECT channel_name as channel, sum(views) as total_views FROM videos group by channel_name'''
    cursor.execute(query7)
    q7= cursor.fetchall()
    df7= pd.DataFrame(q7, columns=["channel","views"])
    st.write(df7)
elif question == "8.What are the names of all the channels that have published videos in the year 2022?":
    cursor = db.cursor()
    query8 = '''select video_title as video_title,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022'''
    cursor.execute(query8)
    q8 = cursor.fetchall()
    df8 = pd.DataFrame(q8, columns=["video_title","publsihed_data","channel"])
    st.write(df8)

elif question == "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    cursor = db.cursor()
    query9 = '''SELECT channel_name AS channel, SEC_TO_TIME(AVG(TIME_TO_SEC(duration))) AS average_duration FROM videos GROUP BY channel_name'''
    cursor.execute(query9)
    q9 = cursor.fetchall()
    df9 = pd.DataFrame(q9, columns=["Channel", "Average Duration"])
    st.write(df9)

elif question == "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    cursor = db.cursor()
    query10 = '''SELECT video_title as video, channel_name AS channel, comment as comments FROM videos ORDER BY comment DESC'''
    cursor.execute(query10)
    q10 = cursor.fetchall()
    df10 = pd.DataFrame(q10, columns=["video", "Channel", "comments"])
    st.write(df10)

