import os
import time
import re
from googleapiclient.discovery import build
from supabase import create_client

# 設定の読み込み
YT_API_KEY = os.environ.get("YOUTUBE_API_KEY")
SB_URL = os.environ.get("SUPABASE_URL")
SB_KEY = os.environ.get("SUPABASE_ANON_KEY")

supabase = create_client(SB_URL, SB_KEY)
youtube = build('youtube', 'v3', developerKey=YT_API_KEY)

def is_japanese(text):
    """ひらがな・カタカナ・漢字が含まれているか判定"""
    if not text: return False
    jp_pattern = re.compile(r'[ぁ-んァ-ン一-龥]')
    return bool(jp_pattern.search(text))

def get_video_stats(video_ids):
    """動画IDから再生回数を取得"""
    res = youtube.videos().list(part="statistics", id=",".join(video_ids)).execute()
    return {item['id']: int(item['statistics'].get('viewCount', 0)) for item in res.get('items', [])}

def fetch_and_save_mvs(target_count=1000):
    collected_data = []
    next_page_token = None
    
    print(f"🚀 邦楽MV収集開始（目標: {target_count}件）")

    while len(collected_data) < target_count:
        search_res = youtube.search().list(
            q="official music video",
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
            title = snippet['title']
            desc = snippet['description']
            
            # 【重要】日本語フィルタ：タイトルか概要欄に日本語がなければスキップ
            if not (is_japanese(title) or is_japanese(desc)):
                continue

            collected_data.append({
                "video_id": v_id,
                "title": title,
                "description": desc,
                "thumbnail_url": snippet['thumbnails']['high']['url'],
                "published_at": snippet['publishedAt'],
                "channel_title": snippet['channelTitle'],
                "view_count": stats_dict.get(v_id, 0),
                "is_analyzed": False
            })

        next_page_token = search_res.get('nextPageToken')
        print(f"📈 現在 {len(collected_data)} 件取得")
        if not next_page_token: break
        time.sleep(0.5)

    # Supabaseへ保存（100件ずつ）
    for i in range(0, len(collected_data), 100):
        batch = collected_data[i:i+100]
        supabase.table("YouTubeMV_Japanese").upsert(batch).execute()
    
    print(f"✨ 完了！ {len(collected_data)} 件を保存しました。")

if __name__ == "__main__":
    fetch_and_save_mvs(1000)
