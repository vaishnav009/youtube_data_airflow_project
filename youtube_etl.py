import pandas as pd
import os
import googleapiclient.discovery


def get_comment_threads():
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    DEVELOPER_KEY = ""
    comments_list = []
    video_id = 'q8q3OFFfY6c'

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = DEVELOPER_KEY)
    
    request = youtube.commentThreads().list(
        part="snippet, replies",
        videoId=video_id
    )
    response = request.execute()
    comments = []
    for comment in response['items']:
            author = comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
            comment_text = comment['snippet']['topLevelComment']['snippet']['textOriginal']
            publish_time = comment['snippet']['topLevelComment']['snippet']['publishedAt']
            comment_info = {'author': author, 
                    'comment': comment_text, 'published_at': publish_time}
            comments.append(comment_info)
    print(f'Finished processing {len(comments)} comments.')
    comments_list.extend(comments)
    
    while response.get('nextPageToken', None):
        request = youtube.commentThreads().list(
            part='id,replies,snippet',
            videoId=video_id,
            pageToken=response['nextPageToken']
        )
        response = request.execute()
        comments = []
        for comment in response['items']:
                author = comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
                comment_text = comment['snippet']['topLevelComment']['snippet']['textOriginal']
                publish_time = comment['snippet']['topLevelComment']['snippet']['publishedAt']
                comment_info = {'author': author, 
                        'comment': comment_text, 'published_at': publish_time}
                comments.append(comment_info)
        print(f'Finished processing {len(comments)} comments.')
        comments_list.extend(comments)

    comments_df = pd.DataFrame(comments_list)
    comments_df.to_csv("s3://youtube-data-bucket-airflow/youtube_data.csv")
