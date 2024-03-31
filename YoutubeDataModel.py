import streamlit as st
import json
import googleapiclient.discovery

import pandas as pd
from sqlalchemy import create_engine
import pymysql

def page1():
    st.title("Extract/Transform")
    #st.write("This is page 1")
        
    # Get user input (replace "text_input" with the appropriate Streamlit element)
    channel_id = st.text_input('Enter YouTube channel ID:', key="channel_id")

    submit_button = st.button("Submit")
    upload = st.button("Upload Mysql")
    #import googleapiclient.errors

    #scopes = ["https://www.googleapis.com/auth/youtube.readonly"]

    #api = "AIzaSyDQU-ARcDi6AGO6Qu9GFRXA01OyuFCNGPo"
    #id = "UCAEv0ANkT221wXsTnxFnBsQ"
    #api = "AIzaSyChgIS1Kmhgf5jbAfQoGDQL2BXDEyBNwA4"

    api = "AIzaSyDV_PhDq5B4jdt1MCc1DyaOvDf0nRNKtAM"
    id = channel_id

    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    #os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    #client_secrets_file = "YOUR_CLIENT_SECRET_FILE.json"

    #credentials = flow.run_console()
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = api)
        #api_service_name, api_version, credentials=credentials)


    if submit_button:

        try:
            
            print(id)
            request = youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id= channel_id        
            )
            response = request.execute()
            
            channel_id = []
            channel_name = []
            channel_views = []
            channel_description = []
            channel_status = []
            channel_type = []
            
            if response['items']:
                item = response['items'][0]
                channel_id.append(item['id'])
                channel_name.append(item['snippet']['title'])
                channel_views.append(item['statistics']['viewCount'])
                channel_description.append(item['snippet']['description'])
                channel_status.append(item['snippet'].get('status', {}).get('privacyStatus', 'public'))

                playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

                playlists = youtube.playlists().list(part="snippet", channelId=id).execute()
                for playlist in playlists['items']:
                    if playlist['snippet']['title'] == 'Uploads':
                        channel_type.append('User')
                        break
                else:
                    channel_type.append('Other')

                channel_info = {
                    'channel_id': channel_id,
                    'channel_name': channel_name,
                    'channel_views': channel_views,
                    'channel_description': channel_description,
                    'channel_status': channel_status,
                    'channel_type': channel_type
                }
                print("Channel")
                df = pd.DataFrame(channel_info)
                st.write(f"The channel data is: ")
                st.table(df)

                channel1_id = []
                playlist1_id = []
                playlist_name = []
                next_page_token = None

                while True:
                    request = youtube.playlistItems().list(
                        part="snippet",
                        playlistId=playlist_id,
                        pageToken=next_page_token
                    )

                    response = request.execute()
                    
                    video_ids = [item["snippet"]["resourceId"]["videoId"] for item in response["items"]]
                    print(video_ids)
                    print([item["id"] for item in response["items"]])

                    # Get video data and comments for each video ID
                    for video_id in video_ids:
                        
                        video_data = videos(video_id,youtube,id)
                        df2 = pd.DataFrame(video_data)
                        st.write(f"The video data is: ")
                        st.table(df2)
                        
                        comment_data = comments(video_id,youtube)
                        df3 = pd.DataFrame(comment_data)
                        st.write(f"The comment data is: ")
                        st.table(df3)

                        


                    for item in response['items']:        
                        channel1_id.append(item['snippet']['channelId'])
                        playlist1_id.append(item['id'])
                        playlist_name.append(item['snippet']['title'])
                    
                    if 'nextPageToken' in response:
                            next_page_token = response['nextPageToken']
                    else:
                        break

                playlist_info = {
                        "channel_id": channel1_id,
                        "playlist_id": playlist1_id,
                        "playlist_name": playlist_name
                    }   
                st.write(f"The playlist data is: ")
                df1 = pd.DataFrame(playlist_info)
                st.table(df1)
                print("Playlist")
        except Exception as e:
            print("Error occurred while fetching data:", e)
            
    if upload:
        try:
            request = youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id= channel_id        
            )
            response = request.execute()

            channel_id = []
            channel_name = []
            channel_views = []
            channel_description = []
            channel_status = []
            channel_type = []
            if response['items']:
                item = response['items'][0]
                channel_id.append(item['id'])
                channel_name.append(item['snippet']['title'])
                channel_views.append(item['statistics']['viewCount'])
                channel_description.append(item['snippet']['description'])
                channel_status.append(item['snippet'].get('status', {}).get('privacyStatus', 'public'))

                playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

                playlists = youtube.playlists().list(part="snippet", channelId=id).execute()
                for playlist in playlists['items']:
                    if playlist['snippet']['title'] == 'Uploads':
                        channel_type.append('User')
                        break
                else:
                    channel_type.append('Other')

                channel_info = {
                    'channel_id': channel_id,
                    'channel_name': channel_name,
                    'channel_views': channel_views,
                    'channel_description': channel_description,
                    'channel_status': channel_status,
                    'channel_type': channel_type
                }
                df = pd.DataFrame(channel_info)
                st.write(f"The channel data is: ")
                st.table(df)
                print("Channel")
                insert('channel',df)

                channel1_id = []
                playlist1_id = []
                playlist_name = []
                next_page_token = None

                while True:
                    request = youtube.playlistItems().list(
                        part="snippet",
                        playlistId=playlist_id,
                        pageToken=next_page_token,
                        maxResults=100
                    )

                    response = request.execute()
                    
                    video_ids = [item["snippet"]["resourceId"]["videoId"] for item in response["items"]]
                    
                    # Get video data and comments for each video ID
                    print(video_ids)
                    for video_id in video_ids:
                    
                        video_data = videos(video_id,youtube,id )                        
                        df2 = pd.DataFrame(video_data)
                        st.write(f"The video data is: ")
                        st.table(df2)
                        insert('video',df2)

                        
                        
                        comment_data = comments(video_id,youtube)                        
                        df3 = pd.DataFrame(comment_data)
                        st.write(f"The comment data is: ")
                        st.table(df3)
                        insert('comment',df3)
                                


                    for item in response['items']:        
                        channel1_id.append(item['snippet']['channelId'])
                        playlist1_id.append(item['id'])
                        playlist_name.append(item['snippet']['title'])
                    
                    if 'nextPageToken' in response:
                            next_page_token = response['nextPageToken']
                    else:
                        break

                playlist_info = {
                        "channel_id": channel1_id,
                        "playlist_id": playlist1_id,
                        "playlist_name": playlist_name
                    }   
                df1 = pd.DataFrame(playlist_info)
                st.write(f"The playlist data is: ")
                st.table(df1)
                print("Playlist")
                insert('playlist',df1)
        except Exception as e:
            print("Error occurred while fetching data:", e)
            


        

    #QL-LMzBjqG0 - video id
    #UCAEv0ANkT221wXsTnxFnBsQ - channel id
    #PLJKJ4EHc1hykjZWxEndz-thRo4BL1sZYv - playlist id

    #cqRj5oN_yDg
    '''UCI6PMKGv6kdn_yw-8OL5N2g
    UCdyjQQK5BmFaIqpgm2llkrA
    UCwlymt1RfnKtOO-jWzGZPDw
    UCR0S-_0llNzHnCVKQqIVSOg
    UCfhfnQM9t0gLooX0F43Ebuw
    UCpQJOQcph-2lhXJw01khJuQ
    UCbgxSTqPmKDO2ZXCnLBefNw
    UC4djEd5CWMJsIlkrqMxT8WA
    UCq5YobB-Zf9wDh5nD_B6plg
    UC5vh6Cx__kltJVDv5Hwq5Ew'''

def insert(table,df):
    engine = create_engine(f"mysql+pymysql://root:He007#W=Got765ked@127.0.0.1/foundation")
    #engine = create_engine(f"mysql+mysqlconnector://root:He007#W=Got765ked@127.0.0.1/foundation")

    # Define chunk size
    chunk_size = 1000

    # Insert data into MySQL table
    chunk_size = 1000

    # Calculate number of chunks
    num_chunks = -(-len(df) // chunk_size)  # Ceil division

    # Insert data into MySQL table in chunks
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, len(df))
        chunk = df.iloc[start_idx:end_idx]
        chunk.to_sql(name=table, con=engine, if_exists='append', index=False)
    print("Data inserted successfully.")

def comments(comment1_id,youtube):
    comment_id = []
    video_id = []
    comment_text = []
    comment_author = []
    comment_published_date = []
    next_page_token = None

    comment_data = {
            "comment_id": "Comments disabled",
            "video_id": video_id,
            "comment_text": comment_text,
            "comment_author": comment_author,
            "comment_published_date": comment_published_date
        }
    try:
        while True:
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId= comment1_id,
                pageToken=next_page_token,
                maxResults=100 
            )
            response = request.execute()

            for comment in response["items"]:
                comment_id.append(comment["id"])
                video_id.append(comment["snippet"]["videoId"])
                comment_text.append(comment["snippet"]["topLevelComment"]["snippet"]["textDisplay"])
                comment_author.append(comment["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"])
                comment_published_date.append(comment["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

            if 'nextPageToken' in response:
                next_page_token = response['nextPageToken']
            else:
                break

        
        
        
    
    except googleapiclient.errors.HttpError as e:
            error_response = json.loads(e.content)
            if 'commentsDisabled' in error_response.get('error', {}).get('errors', [{}])[0].get('reason', ''):
                print(f"Comments are disabled for the video with ID {comment1_id}")
                comment_id.append("null")
                video_id.append("null")
                comment_text.append("Comments are disabled")
                comment_author.append("null")
                comment_published_date.append("null")
    comment_data = {
            "comment_id": comment_id,
            "video_id": video_id,
            "comment_text": comment_text,
            "comment_author": comment_author,
            "comment_published_date": comment_published_date
        }
    df = pd.DataFrame(comment_data) 
    if df.empty:
        comment_data = {
            "comment_id": ['null'],
            "video_id": [comment1_id],
            "comment_text": ['null'],
            "comment_author": ['null'],
            "comment_published_date": ['null']
        }   
    print("Comments")       
    return comment_data 

def videos(video1_id,youtube,channel_id2):
    print(channel_id2)
    video_id = []
    playlist_id2 = []
    video_name = []
    video_description = []
    published_date = []
    view_count = []
    like_count = []
    dislike_count = []
    favorite_count = []
    comment_count = []
    duration = []
    thumbnail = []
    caption_status = []
    channel_id = []
    print(video1_id)
    
    
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video1_id
    )
    response = request.execute()

    for item in response["items"]:
        snippet = item["snippet"]
        statistics = item["statistics"]
        content_details = item["contentDetails"]
        video_id.append(item["id"])        
        
        playlist_id2.append(snippet.get("playlistId", None))
        video_name.append(snippet["title"])
        video_description.append(snippet.get("description", ""))
        published_date.append(snippet["publishedAt"])
        view_count.append(statistics.get("viewCount", 0))
        like_count.append(statistics.get("likeCount", 0))
        dislike_count.append(statistics.get("dislikeCount", 0))
        favorite_count.append(statistics.get("favoriteCount", 0))
        comment_count.append(statistics.get("commentCount", 0))
        duration.append(content_details["duration"])
        # Check if thumbnails exist before accessing
        if "thumbnails" in snippet:
            thumbnail.append(snippet["thumbnails"]["default"]["url"])
        else:
            thumbnail.append(None)
        caption_status.append(snippet.get("hasCaption", False))
        channel_id.append(channel_id2)
        

    video_info = {
        "video_id": video_id,
        "playlist_id": playlist_id2,
        "video_name": video_name,
        "video_description": video_description,
        "published_date": published_date,
        "view_count": view_count,
        "like_count": like_count,
        "dislike_count": dislike_count,
        "favorite_count": favorite_count,
        "comment_count": comment_count,
        "duration": duration,
        "thumbnail": thumbnail,
        "caption_status": caption_status,
        "channel_id" : channel_id
    }
    print("Video")

    return video_info

def page2():
    st.title("Data retrieval based on queries")
    st.write("Select the below from the dropdown")

    option1 = "1.What are the names of all the videos and their corresponding channels?"
    option2 = "2.Which channels have the most number of videos, and how many videos do they have?"
    option3 = "3.What are the top 10 most viewed videos and their respective channels?"
    option4 = "4.How many comments were made on each video, and what are their corresponding video names?"
    option5 = "5.Which videos have the highest number of likes, and what are their corresponding channel names?"
    option6 = "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names"
    option7 = "7.What is the total number of views for each channel, and what are their corresponding channel names?"
    option8 = "8.What are the names of all the channels that have published videos in the year 2022?"
    option9 = "9.What is the average duration of all videos in each channel, and what are their corresponding channel names"
    option10 = "10.Which videos have the highest number of comments, and what are their corresponding channel names?"



    # Define options for the dropdown
    options = [option1, option2, option3,option4, option5, option6,option7, option8, option9,option10]

    # Add dropdown to the app
    selected_option = st.selectbox('Select an option:', options)

    engine = create_engine(f"mysql+pymysql://root:He007#W=Got765ked@127.0.0.1/foundation")

    if selected_option == option1:

        query = "SELECT v.video_name, c.channel_name FROM video v INNER JOIN channel c ON v.channel_id = c.channel_id"
        df = pd.read_sql_query(query, con=engine)
        # Close the database connection
        # Display the DataFrame as a table in Streamlit
        st.table(df)
    
    elif selected_option == option2:
        query = "SELECT c.channel_name, COUNT(*) AS video_count FROM channel c INNER JOIN video v ON c.channel_id = v.channel_id GROUP BY c.channel_name ORDER BY video_count DESC limit 10"
        df = pd.read_sql_query(query, con=engine)
        st.table(df)

    elif selected_option == option3:
        query = "SELECT v.video_name, c.channel_name, CAST(v.view_count AS UNSIGNED) AS view_count FROM video v INNER JOIN channel c ON v.channel_id = c.channel_id ORDER BY view_count DESC LIMIT 10"
        df = pd.read_sql_query(query, con=engine)
        st.table(df)

    elif selected_option == option4:
        query = "select distinct video_name, view_count from video"
        df = pd.read_sql_query(query, con=engine)
        st.table(df)

    elif selected_option == option5:
        query = "SELECT v.video_name, c.channel_name, v.like_count FROM video v INNER JOIN channel c ON v.channel_id = c.channel_id ORDER BY CAST(v.like_count as unsigned)  DESC LIMIT 10"
        df = pd.read_sql_query(query, con=engine)
        st.table(df)

    elif selected_option == option6:
        query = "SELECT video_name, like_count,dislike_count FROM video"
        df = pd.read_sql_query(query, con=engine)
        st.table(df)

    elif selected_option == option7:
        query = "SELECT c.channel_name, SUM(v.view_count) AS total_views FROM channel c INNER JOIN video v ON c.channel_id = v.channel_id GROUP BY c.channel_name"    
        df = pd.read_sql_query(query, con=engine)
        st.table(df)

    elif selected_option == option8:
        query = "SELECT DISTINCT c.channel_name FROM channel c INNER JOIN video v ON c.channel_id = v.channel_id WHERE YEAR(CAST(v.published_date as date)) = 2022"
        df = pd.read_sql_query(query, con=engine)
        st.table(df)

    elif selected_option == option9:
        query = '''SELECT c.channel_name, AVG( TIME_TO_SEC(
            CONCAT(
                REPLACE(
                    SUBSTRING_INDEX(v.duration, 'PT', -1),
                    'M',
                    '*60+'
                ),
                'S'
            )
        )
    ) AS avg_duration
FROM channel c
INNER JOIN video v ON c.channel_id = v.channel_id
GROUP BY c.channel_name;'''
        df = pd.read_sql_query(query, con=engine)
        st.table(df)

    elif selected_option == option10:
        query = '''SELECT v.video_name, c.channel_name, COUNT(com.comment_id) AS comment_count
FROM video v
INNER JOIN channel c ON v.channel_id = c.channel_id
LEFT JOIN comment com ON v.video_id = com.video_id
GROUP BY v.video_name, c.channel_name
ORDER BY comment_count DESC'''
        df = pd.read_sql_query(query, con=engine)
        st.table(df)

    
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Extract/Transform", "Data Questions"])
if page == "Extract/Transform":
    page1()
elif page == "Data Questions":
    page2()


