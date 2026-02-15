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
    if not text:
        return False
    # Unicodeの範囲: ひらがな(\u3040-\u309F)、カタカナ(\u30A0-\u30FF)、漢字(\u4E00-\u9FFF)
    return bool(re.search(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]', text))

def fetch_yearly_mvs(year, count_limit=100):
    print(f"\n📅 {year}年のMVを収集しています...")
    
    start_time = f"{year}-01-01T00:00:00Z"
    end_time = f"{year}-12-31T23:59:59Z"
    
    # 検索キーワードを「公式」などの日本語主体に
    query = "公式 MV -cover -歌ってみた -reaction -切り抜き -LIVE -カラオケ"
    
    videos = []
    next_page_token = None
    
    # 指定件数に達するか、検索結果が尽きるまでループ
    while len(videos) < count_limit:
        search_response = youtube.search().list(
            q=query,
            part="snippet",
            maxResults=50,
            type="video",
            videoCategoryId="10",
            relevanceLanguage="ja",
            regionCode="JP",
            publishedAfter=start_time,
            publishedBefore=end_time,
            order="viewCount",
            pageToken=next_page_token
        ).execute()
        
        for item in search_response['items']:
            snippet = item['snippet']
            title = snippet['title']
            description = snippet['description']
            
            # 【重要】日本語フィルター：タイトルか概要欄に日本語があればOK
            if is_japanese(title) or is_japanese(description):
                videos.append({
                    "video_id": item['id']['videoId'],
                    "title": title,
                    "channel_title": snippet['channelTitle'],
                    "thumbnail_url": snippet['thumbnails']['high']['url'],
                    "published_at": snippet['publishedAt'],
                    "view_count": 0,
                    "is_analyzed": False
                })
            
            if len(videos) >= count_limit:
                break
            
        next_page_token = search_response.get('nextPageToken')
        if not next_page_token:
            break
            
    return videos[:count_limit]

def save_to_supabase(videos):
    new_count = 0
    for v in videos:
        # 重複チェック
        check = supabase.table("YouTubeMV_Japanese").select("video_id").eq("video_id", v["video_id"]).execute()
        
        if not check.data:
            supabase.table("YouTubeMV_Japanese").insert(v).execute()
            new_count += 1
            
    print(f"  ✅ {new_count} 件の国内向け動画を保存しました。")

if __name__ == "__main__":
    # SQLでTRUNCATEした後、これを実行してください
    current_year = datetime.now().year
    for year in range(2011, current_year + 1):
        try:
            yearly_videos = fetch_yearly_mvs(year, 100)
            save_to_supabase(yearly_videos)
            time.sleep(1) # API制限に配慮
        except Exception as e:
            print(f"  ❌ {year}年の収集に失敗しました: {e}")

    print("\n🎉 国内向けMVの収集が完了しました！")
