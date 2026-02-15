import os
import time
import re
from googleapiclient.discovery import build
from supabase import create_client

# 環境変数
YT_API_KEY = os.environ.get("YOUTUBE_API_KEY")
SB_URL = os.environ.get("SUPABASE_URL")
SB_KEY = os.environ.get("SUPABASE_ANON_KEY")

supabase = create_client(SB_URL, SB_KEY)
youtube = build('youtube', 'v3', developerKey=YT_API_KEY)

# API消費カウンター
total_quota_used = 0

def is_japanese(text):
    if not text: return False
    return bool(re.search(r'[ぁ-んァ-ン一-龥]', text))

def get_video_stats(video_ids):
    global total_quota_used
    res = youtube.videos().list(part="statistics", id=",".join(video_ids)).execute()
    total_quota_used += 1  # videos.list は 1ユニット
    return {item['id']: int(item['statistics'].get('viewCount', 0)) for item in res.get('items', [])}

def fetch_and_save_mvs(target_count=1000):
    global total_quota_used
    collected_data = []
    next_page_token = None
    
    search_queries = [
        'official music video "公式"',
        'ミュージックビデオ',
        'MV "official"',
        '邦楽 最新'
    ]
    
    print(f"🚀 邦楽MV収集（目標: {target_count}件）を開始します")

    for q_text in search_queries:
        if len(collected_data) >= target_count:
            break
            
        print(f"🔍 検索クエリ: {q_text}")
        next_page_token = None 

        for i in range(10): 
            if len(collected_data) >= target_count:
                break

            search_res = youtube.search().list(
                q=q_text,
                part="snippet", type="video", regionCode="JP",
                relevanceLanguage="ja", order="date", maxResults=50,
                pageToken=next_page_token
            ).execute()
            
            total_quota_used += 100  # search.list は 100ユニット

            items = search_res.get('items', [])
            if not items: break
            
            video_ids = [item['id']['videoId'] for item in items]
            stats_dict = get_video_stats(video_ids)

            for item in items:
                v_id = item['id']['videoId']
                snippet = item['snippet']
                
                if not (is_japanese(snippet['title']) or is_japanese(snippet['description']) or is_japanese(snippet['channelTitle'])):
                    continue

                collected_data.append({
                    "video_id": v_id,
                    "title": snippet['title'],
                    "description": snippet['description'],
                    "thumbnail_url": snippet['thumbnails']['high']['url'],
                    "published_at": snippet['publishedAt'],
                    "channel_title": snippet['channelTitle'],
                    "view_count": stats_dict.get(v_id, 0),
                    "is_analyzed": False
                })

            next_page_token = search_res.get('nextPageToken')
            print(f"📈 累計取得: {len(collected_data)}件 / 消費API: {total_quota_used}ユニット")
            
            if not next_page_token: break
            time.sleep(0.1)

    # Supabaseへ一括保存
    if collected_data:
        unique_data = list({v['video_id']: v for v in collected_data}.values())[:target_count]
        for i in range(0, len(unique_data), 100):
            batch = unique_data[i:i+100]
            supabase.table("YouTubeMV_Japanese").upsert(batch).execute()
        
        print("-" * 30)
        print(f"✅ 最終結果: {len(unique_data)}件を同期完了")
        print(f"📊 本日の総消費API: {total_quota_used} ユニット")
        print(f"💡 残り推定: {10000 - total_quota_used} ユニット (無料枠内)")
        print("-" * 30)

if __name__ == "__main__":
    fetch_and_save_mvs(1000)
