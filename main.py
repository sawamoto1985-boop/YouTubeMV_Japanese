import os
import time
import re
from datetime import datetime
from googleapiclient.discovery import build
from supabase import create_client

# --- 設定 ---
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")
SB_URL = os.environ.get("SUPABASE_URL")
SB_KEY = os.environ.get("SUPABASE_ANON_KEY")

youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
supabase = create_client(SB_URL, SB_KEY)

def is_japanese(text):
    """ひらがな、カタカナ、漢字が1文字でも含まれているか判定"""
    if not text: return False
    return bool(re.search(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]', text))

def fetch_yearly_mvs(year, count_limit=100):
    print(f"\n📅 {year}年のMVを収集しています...")
    start_time = f"{year}-01-01T00:00:00Z"
    end_time = f"{year}-12-31T23:59:59Z"
    query = "公式 MV -cover -歌ってみた -reaction -切り抜き -LIVE -カラオケ"
    
    videos = []
    next_page_token = None
    
    while len(videos) < count_limit:
        # 1. 検索実行
        search_res = youtube.search().list(
            q=query, part="id", maxResults=50, type="video",
            videoCategoryId="10", relevanceLanguage="ja", regionCode="JP",
            publishedAfter=start_time, publishedBefore=end_time,
            order="viewCount", pageToken=next_page_token
        ).execute()
        
        v_ids = [item['id']['videoId'] for item in search_res.get('items', [])]
        if not v_ids: break

        # 2. 詳細データ取得（再生数・概要欄・長さ）
        details_res = youtube.videos().list(
            id=",".join(v_ids),
            part="snippet,statistics,contentDetails"
        ).execute()

        for item in details_res.get('items', []):
            snippet = item['snippet']
            stats = item.get('statistics', {})
            content_details = item.get('contentDetails', {})
            
            title = snippet['title']
            description = snippet.get('description', '')
            duration = content_details.get('duration', '')

            if is_japanese(title) or is_japanese(description):
                videos.append({
                    "video_id": item['id'],
                    "title": title,
                    "description": description[:1000],
                    "channel_title": snippet['channelTitle'],
                    "thumbnail_url": snippet['thumbnails']['high']['url'],
                    "view_count": int(stats.get('viewCount', 0)),
                    "duration": duration,
                    "published_at": snippet['publishedAt'],
                    "is_analyzed": False
                })
            
            if len(videos) >= count_limit: break
            
        next_page_token = search_res.get('nextPageToken')
        if not next_page_token: break
            
    return videos[:count_limit]

def save_to_supabase(videos):
    new_count = 0
    for v in videos:
        # 重複チェック
        check = supabase.table("YouTubeMV_Japanese").select("video_id").eq("video_id", v["video_id"]).execute()
        if not check.data:
            supabase.table("YouTubeMV_Japanese").insert(v).execute()
            new_count += 1
    print(f"  ✅ {new_count} 件保存完了")

if __name__ == "__main__":
    current_year = datetime.now().year
    # 2011年から今年までループ
    for year in range(2011, current_year + 1):
        try:
            yearly_videos = fetch_yearly_mvs(year, 100)
            save_to_supabase(yearly_videos)
            time.sleep(1) # ここでのエラーを防ぐために適切にtry内に配置
        except Exception as e:
            print(f"  ❌ {year}年の収集エラー: {e}")

    print("\n🎉 全年代の収集が完了しました！")
