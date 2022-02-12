from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.tools import argparser
import yaml
import json
import pandas as pd
from collections import defaultdict, Counter
from tqdm import tqdm

def build_crawler(DEVELOPER_KEY):
    YOUTUBE_API_SERVICE_NAME = "youtube"
    YOUTUBE_API_VERSION = "v3"
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY)
    return youtube

def get_id_from_name(channel_name, maxResults=1):
    search_response = youtube.search().list(
        q = channel_name,
        part="snippet",
        maxResults = 1
    ).execute()
    snippet = search_response['items'][0]['snippet']
    check = input(f"Channel name is {snippet['title']}, right? [y/n]")
    if check.lower() in ['n','no', 'x']:
        print('check channel name querry again.')
        return False
    return snippet['channelId']

def get_playlists(channel_id, channels=''):
    print('getting playlists...')
    playlists_list_response = youtube.playlists().list(
        channelId=channel_id,
        part="snippet",
        maxResults = 100
    ).execute()
    output = []
    for playlist in playlists_list_response['items']:
        if channels == 'all':
            playlists = {'playlist_name': playlist['snippet']['title']}
            playlists['id']= playlist['id']
            output.append(playlists)
        else:
            if playlist['snippet']['title'] in channels:
                playlists = {'playlist_name': playlist['snippet']['title']}
                playlists['id']= playlist['id']
                output.append(playlists)
    return output

def get_videoids_from_playlist(playlist_id):
    print('getting videoids...')
    videos = []
    playlistitems_list_response = youtube.playlistItems().list(
        playlistId=playlist_id,
        part="snippet",
        maxResults=100
    ).execute()
    while playlistitems_list_response:
        for item in playlistitems_list_response['items']:
            videos.append(item['snippet']['resourceId']['videoId'])
        if 'nextPageToken' in playlistitems_list_response.keys():
            playlistitems_list_response = youtube.playlistItems().list(
                playlistId=playlist_id,
                part="snippet",
                maxResults=100,
                pageToken = playlistitems_list_response['nextPageToken']
            ).execute()
            for item in playlistitems_list_response['items']:
                videos.append(item['snippet']['resourceId']['videoId'])
        else: 
            break
    return videos

def get_metadata_from_videoids(video_ids):
    video_meta = []
    for idx in range(0, len(video_ids), 50):
        if idx+50 >= len(video_ids):
            video_50 = video_ids[idx:]
        else:
            video_50 = video_ids[idx:idx+50]
        meta_responses = youtube.videos().list(
            part='snippet, statistics', 
            id=video_50
        ).execute()
        for i in meta_responses['items']:
            temp_dict = {"title": i['snippet']['title']}
            temp_dict['id'] = video_ids
            temp_dict.update(i['statistics'])
            temp_dict['comment_text'] = get_comments_from_videoid(video_ids, get_reply=False)
            video_meta.append(temp_dict)
    return video_meta

def get_comments_from_videoid(video_id, get_reply=False):
    if not video_id:
        return ['disabled video']
    comments = []
    try:
        comment_response = youtube.commentThreads().list(
            part='snippet,replies', 
            videoId=video_id, 
            textFormat='plainText',
            maxResults=100
        ).execute()
    except:
        return ['disabled comments']
    while comment_response:
        for item in comment_response['items']:
            comment = item['snippet']['topLevelComment']['snippet']
            comments.append([comment['textDisplay'], comment['likeCount']])
            if get_reply:
                if item['snippet']['totalReplyCount'] > 0:
                    for reply_item in item['replies']['comments']:
                        reply = reply_item['snippet']
                        comments.append([reply['textDisplay'], reply['likeCount']])
        if 'nextPageToken' in comment_response:
            comment_response = youtube.commentThreads().list(
                part='snippet,replies', 
                videoId=video_id, 
                pageToken=comment_response['nextPageToken'], 
                maxResults=100
            ).execute()
            for item in comment_response['items']:
                comment = item['snippet']['topLevelComment']['snippet']
            comments.append([comment['textDisplay'], comment['likeCount']])
            if get_reply:
                if item['snippet']['totalReplyCount'] > 0:
                    for reply_item in item['replies']['comments']:
                        reply = reply_item['snippet']
                        comments.append([reply['textDisplay'], reply['likeCount']])
        else:
            break
    return comments

# def save_files(files, save_name='output'):
#     if output_type == 'json':
#         pass
#     elif output_type == 'csv':
#         pass
#     else:
#         raise 'unsupported output type'

def main(config_path = './config.yaml'):
    with open(config_path, encoding='utf8') as f:
        config = yaml.safe_load(f)
    DEVELOPER_KEY = config['DEVELOPER_KEY']
    channel_names = config['channel_names']
    playlists = config['playlists']
    command = config['top_n']
    if command:
        commands = [f'{key} "{value}"' for key, value in command.items()]
        print('command: ', ', '.join(commands))

    global youtube
    youtube = build_crawler(DEVELOPER_KEY)
    output = []

    # get channel ids
    
    for channel_name in channel_names:
        print(channel_name, 'start')
        channel_dict = {}
        channel_dict['channel_name'] = channel_name
        channel_id = get_id_from_name(channel_name)
        if channel_id == False:
            continue
        channel_dict['id'] = channel_id

        # get channel's playlists ids
        channel_playlists = []
        if playlists == 'uploaded':
            temp_dict = {'playlist_name':'Uploaded Videos'}
            temp_dict['id'] = 'UU' + channel_id[2:]
            channel_playlists.append(temp_dict)
        else:
            temp_playilsts = get_playlists(channel_id)
            channel_playlists = temp_playilsts[:]

        # get video ids from playlist
        for idx, playlist in enumerate(channel_playlists):
            playlist['videos'] = []
            video_list = get_videoids_from_playlist(playlist['id'])
            
            for video_id in tqdm(video_list):
                video_info = get_metadata_from_videoids(video_id)
                playlist['videos'].append(video_info)
            channel_playlists[idx] = playlist
        channel_dict['playlists'] = channel_playlists
        
        output.append(channel_dict)

        with open(f'{channel_name}.json', 'w', encoding="UTF-8") as f:
            json.dump(channel_dict, f, ensure_ascii=False)
            print(f'{channel_name} json dumped')

    with open('output.json', 'w', encoding="UTF-8") as f:
        json.dump(output, f, ensure_ascii=False)
        print('output json dumped')

if __name__ == '__main__':
    main()