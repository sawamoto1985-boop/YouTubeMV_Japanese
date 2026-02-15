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

def is_japanese(text):
    if not text: return False
    return bool(re.search(r'[ぁ-んァ-ン一-龥]', text))

def get_video_stats(video_ids):
    res = youtube.videos().list(part="statistics", id=",".join(video_ids)).execute()
    return {item['id']: int(item['statistics'].get('viewCount', 0)) for item in res.get('items', [])}

def fetch_and_save_mvs(target_count=1000):
    collected_data = []
    next_page_token = None
    search_query = 'official music video | "MV" | "ミュージックビデオ"'
    
    print(f"🚀 1000件の邦楽MV収集を開始します")

    # 目標に達するまで最大50回ループ（1回50件取得）
    for i in range(50): 
        if len(collected_data) >= target_count:
            break

        search_res = youtube.search().list(
            q=search_query,
            part="snippet", type="video", regionCode="JP",
            relevanceLanguage="ja", order="date", maxResults=50,
            pageToken=next_page_token
        ).execute()

        video_ids = [item['id']['videoId'] for item in search_res.get('items', [])]
        if not video_ids: break
        
        stats_dict = get_video_stats(video_ids)

        for item in search_res.get('items', []):
            v_id = item['id']['videoId']
            snippet = item['snippet']
            title, desc, channel = snippet['title'], snippet['description'], snippet['channelTitle']
            
            # 日本語フィルタ
            if not (is_japanese(title) or is_japanese(desc) or is_japanese(channel)):
                continue

            collected_data.append({
                "video_id": v_id,
                "title": title,
                "description": desc,
                "thumbnail_url": snippet['thumbnails']['high']['url'],
                "published_at": snippet['publishedAt'],
                "channel_title": channel,
                "view_count": stats_dict.get(v_id, 0),
                "is_analyzed": False
            })
            if len(collected_data) >= target_count: break

        next_page_token = search_res.get('nextPageToken')
        print(f"📈 進捗: {len(collected_data)} / {target_count} (Loop: {i+1})")
        if not next_page_token: break
        time.sleep(0.2) # 負荷軽減

    # Supabaseへ一括保存
    if collected_data:
        for i in range(0, len(collected_data), 100):
            batch = collected_data[i:i+100]
            supabase.table("YouTubeMV_Japanese").upsert(batch).execute()
        print(f"✨ 完了: 合計 {len(collected_data)} 件を同期しました。")

if __name__ == "__main__":
    fetch_and_save_mvs(1000)
