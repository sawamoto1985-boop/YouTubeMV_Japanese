import os
import time
from datetime import datetime
from googleapiclient.discovery import build
from supabase import create_client

# --- 設定 ---
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")
SB_URL = os.environ.get("SUPABASE_URL")
SB_KEY = os.environ.get("SUPABASE_ANON_KEY")

youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
supabase = create_client(SB_URL, SB_KEY)

def fetch_yearly_mvs(year, count_limit=100):
    print(f"📅 {year}年のMVを収集しています...")
    
    start_time = f"{year}-01-01T00:00:00Z"
    end_time = f"{year}-12-31T23:59:59Z"
    
    # 除外ワードを徹底して精度を上げる
    query = "official MV -cover -歌ってみた -reaction -切り抜き -LIVE -カラオケ"
    
    videos = []
    next_page_token = None
    
    # 50件ずつ、最大2回ループ（合計100件）
    while len(videos) < count_limit:
        search_response = youtube.search().list(
            q=query,
            part="snippet",
            maxResults=min(50, count_limit - len(videos)),
            type="video",
            videoCategoryId="10",      # Musicカテゴリ固定
            relevanceLanguage="ja",    # 日本語
            regionCode="JP",           # 日本
            publishedAfter=start_time,
            publishedBefore=end_time,
            order="viewCount",         # 再生数順
            pageToken=next_page_token
        ).execute()
        
        for item in search_response['items']:
            v_id = item['id']['videoId']
            snippet = item['snippet']
            
            videos.append({
                "video_id": v_id,
                "title": snippet['title'],
                "channel_title": snippet['channelTitle'],
                "thumbnail_url": snippet['thumbnails']['high']['url'],
                "published_at": snippet['publishedAt'],
                "view_count": 0, # 後で更新するか、とりあえず0
                "is_analyzed": False # これが重要（Gemini判定に回すため）
            })
            
        next_page_token = search_response.get('nextPageToken')
        if not next_page_token:
            break
            
    return videos

def save_to_supabase(videos):
    new_count = 0
    for v in videos:
        # 重複チェック（video_idが既にあるか）
        check = supabase.table("YouTubeMV_Japanese").select("video_id").eq("video_id", v["video_id"]).execute()
        
        if not check.data:
            supabase.table("YouTubeMV_Japanese").insert(v).execute()
            new_count += 1
            
    print(f"  ✅ {new_count} 件の新しい動画を保存しました。")

if __name__ == "__main__":
    current_year = datetime.now().year
    # 2011年（15年前）から今年までループ
    for year in range(2011, current_year + 1):
        try:
            yearly_videos = fetch_yearly_mvs(year, 100)
            save_to_supabase(yearly_videos)
            time.sleep(2) # API制限に優しく
        except Exception as e:
            print(f"  ❌ {year}年の収集に失敗しました: {e}")

    print("\n🎉 全年代の収集作業が完了しました！次は analyze.py を動かしてAI判定をしてください。")
