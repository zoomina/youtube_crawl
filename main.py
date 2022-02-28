from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.tools import argparser
import yaml
import json
import pandas as pd
from collections import defaultdict, Counter
from tqdm import tqdm

class YoutubeCrawler:
    def __init__(self, config):
        self.channel_names = config['channel_names']
        self.playlists = config['playlists']
        self.save = config['save']

        # build youtube cralwer
        YOUTUBE_API_SERVICE_NAME = "youtube"
        YOUTUBE_API_VERSION = "v3"
        DEVELOPER_KEY = config['DEVELOPER_KEY']
        self.youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY)

    def __len__(self):
        return len(self.channel_names)

    def get_id_from_name(self, channel_name, maxResults=1):
        search_response = self.youtube.search().list(
            q = channel_name,
            part="snippet",
            maxResults = maxResults
        ).execute()
        snippet = search_response['items'][0]['snippet']
        check = input(f"Channel name is {snippet['title']}, right? [y/n]")
        if check.lower() not in ['y', 'yes', 'o']:
            print('check channel name querry again.')
            return False
        return snippet['channelId']

    def get_playlists(self, channel_id, channels=''):
        print('getting playlists...')
        playlists_list_response = self.youtube.playlists().list(
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

    def get_videoids_from_playlist(self, playlist_id):
        print('getting videoids...')
        videos = []
        playlistitems_list_response = self.youtube.playlistItems().list(
            playlistId=playlist_id,
            part="snippet",
            maxResults=100
        ).execute()
        while playlistitems_list_response:
            for item in playlistitems_list_response['items']:
                videos.append(item['snippet']['resourceId']['videoId'])
            if 'nextPageToken' in playlistitems_list_response.keys():
                playlistitems_list_response = self.youtube.playlistItems().list(
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

    def get_metadata_from_videoids(self, video_ids):
        video_meta = []
        for idx in range(0, len(video_ids), 50):
            if idx+50 >= len(video_ids):
                video_50 = video_ids[idx:]
            else:
                video_50 = video_ids[idx:idx+50]
            meta_responses = self.youtube.videos().list(
                part='snippet, statistics', 
                id=video_50
            ).execute()
            for idx, i in enumerate(meta_responses['items']):
                temp_dict = {"title": i['snippet']['title']}
                temp_dict['id'] = video_ids[idx]
                temp_dict['publishedAt'] = i['snippet']['publishedAt']
                temp_dict.update(i['statistics'])
                temp_dict['comment_text'] = self.get_comments_from_videoid(video_ids, get_reply=False)
                video_meta.append(temp_dict)
        return video_meta

    def get_comments_from_videoid(self, video_id, get_reply=False):
        if not video_id:
            return ['disabled video']
        comments = []
        try:
            comment_response = self.youtube.commentThreads().list(
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
                comment_response = self.youtube.commentThreads().list(
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

    def save_file(self, output, save_type='json'):
        if save_type == 'json':
            with open('output.json', 'w', encoding="UTF-8") as f:
                json.dump(output, f, ensure_ascii=False)
                print('output json dumped')
        elif save_type == 'csv':
            # json
            with open('output.json', 'w', encoding="UTF-8") as f:
                json.dump(output, f, ensure_ascii=False)
                print('output json dumped')

            # playlist.csv
            channel_names, channel_ids, playlist_title, playlist_ids  = [], [], [], []
            for channel in output:
                temp_playlists = channel['playlists']
                playlist_title.extend([playlist['playlist_name'] for playlist in temp_playlists])
                temp_ids = [playlist['id'] for playlist in temp_playlists]
                playlist_ids.extend(temp_ids)
                channel_names.extend([channel['channel_name']] * len(temp_ids))
                channel_ids.extend([channel['id']] * len(temp_ids))
            playlist = pd.DataFrame({'channel_name':channel_names, 'channel_ID':channel_ids, 'playlist_title':playlist_title, 'playlist_ID':playlist_ids})
            playlist.to_csv('playlist.csv', index=False)
            print('playlist.csv created')

            # playlist_items.csv
            playlist_title, playlist_ids, video_title, video_ids, video_published = [], [], [], [], []
            for channel in output:
                for playlist in channel['playlists']:
                    for video in playlist['videos']:
                        video_title.append(video['title'])
                        video_ids.append(video['id'])
                        video_published.append(video['publishedAt'])
                        playlist_title.append(playlist['playlist_name'])
                        playlist_ids.append(playlist['id'])
            playlist_items = pd.DataFrame({'playlist_title':playlist_title, 'playlist_ID':playlist_ids, 'video_title':video_title, 'video_ids':video_ids, 'video_published':video_published})
            playlist_items.to_csv('playlist_items.csv', index=False)
            print('playlist_items.csv created')

            # video.csv
            video_title, video_ids, commentCount, viewCount, favoriteCount, likeCount = [], [], [], [], [], []
            for channel in output:
                for playlist in channel['playlists']:
                    for video in playlist['videos']:
                        video_title.append(video['title'])
                        video_ids.append(video['id'])
                        if video.get('commentCount'):
                            commentCount.append(video['commentCount'])
                        else:
                            commentCount.append(0)
                        viewCount.append(video['viewCount'])
                        favoriteCount.append(video['favoriteCount'])
                        likeCount.append(video['likeCount'])
            video = pd.DataFrame({'video_title':video_title, 'video_ids':video_ids, 'commentCount':commentCount, 'viewCount':viewCount, 'favoriteCount':favoriteCount, 'likeCount':likeCount})
            video.to_csv('video.csv', index=False)
            print('video.csv created')

            # commentThreads.csv
            video_title, video_ids, text_display, like_count = [], [], [], []
            for channel in output:
                for playlist in channel['playlists']:
                    for video in playlist['videos']:
                        for comment_like in video['comment_text']:
                            if comment_like == 'disabled comments':
                                text_display.append(comment_like)
                                like_count.append(0)
                            else:
                                comment, like = comment_like
                                text_display.append(comment)
                                like_count.append(like)
                            video_title.append(video['title'])
                            video_ids.append(video['id'])
            commentThreads = pd.DataFrame({'video_title':video_title, 'video_ids':video_ids,'text_display':text_display, 'like_count':like_count})
            commentThreads.to_csv('commentThreads.csv',index=False)
            print('commentThreads.csv created')
        else:
            raise TypeError

    def start(self):
        output = []
        # get channel ids
        for channel_name in self.channel_names:
            print(channel_name, 'start')
            channel_dict = {}
            channel_dict['channel_name'] = channel_name
            channel_id = self.get_id_from_name(channel_name)
            if channel_id == False:
                continue
            channel_dict['id'] = channel_id

            # get channel's playlists ids
            channel_playlists = []
            if self.playlists == 'uploaded':
                temp_dict = {'playlist_name':'Uploaded Videos'}
                temp_dict['id'] = 'UU' + channel_id[2:]
                channel_playlists.append(temp_dict)
            else:
                temp_playilsts = self.get_playlists(channel_id)
                channel_playlists = temp_playilsts[:]

            # get video ids from playlist
            for idx, playlist in enumerate(channel_playlists):
                playlist['videos'] = []
                video_list = self.get_videoids_from_playlist(playlist['id'])
                
                # for video_id in tqdm(video_list):
                video_info = self.get_metadata_from_videoids(video_list)
                playlist['videos'].extend(video_info)
                channel_playlists[idx] = playlist
            channel_dict['playlists'] = channel_playlists
            
            output.append(channel_dict)

        self.save_file(output, self.save)
        print("END")

if __name__ == '__main__':
    config_path = './config.yaml'
    with open(config_path, encoding='utf8') as f:
        config = yaml.safe_load(f)
    
    crawler = YoutubeCrawler(config)
    crawler.start()

    