import os
import time
import re
from googleapiclient.discovery import build
from supabase import create_client

# 環境変数
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")
SB_URL = os.environ.get("SUPABASE_URL")
SB_KEY = os.environ.get("SUPABASE_ANON_KEY")

youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
supabase = create_client(SB_URL, SB_KEY)

def is_japanese(text):
    if not text: return False
    return bool(re.search(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]', text))

def parse_duration(duration_str):
    """ISO8601(PT1M30S)を秒数に変換"""
    m = re.search(r'(\d+)M', duration_str)
    s = re.search(r'(\d+)S', duration_str)
    return (int(m.group(1)) * 60 if m else 0) + (int(s.group(1)) if s else 0)

def fetch_yearly_data(year):
    print(f"\n📅 {year}年の処理を開始します")
    start_time = f"{year}-01-01T00:00:00Z"
    end_time = f"{year}-12-31T23:59:59Z"
    
    # 2. キーワードの組み合わせ (API条件)
    # パターンを考慮したAND/ORクエリ
    query = '(MV "公式") | ("Music Video" "公式") | (MV "Official") | ("Music Video" "Official")'
    
    candidates = []
    total_quota = 0
    
    # API検索 (50件×2回 = 100件)
    next_page_token = None
    for _ in range(2):
        search_res = youtube.search().list(
            q=query, part="id", maxResults=50, type="video",
            videoCategoryId="10", videoDuration="medium", # 1. 音楽ジャンル / 4. Shorts排除
            relevanceLanguage="ja", regionCode="JP", # 3. 日本向け
            publishedAfter=start_time, publishedBefore=end_time,
            order="viewCount", pageToken=next_page_token
        ).execute()
        total_quota += 100
        
        ids = [item['id']['videoId'] for item in search_res.get('items', [])]
        if not ids: break
        
        # 詳細情報をバッチ取得 (1回で50件分)
        details_res = youtube.videos().list(
            id=",".join(ids),
            part="snippet,statistics,contentDetails"
        ).execute()
        total_quota += 1
        
        candidates.extend(details_res.get('items', []))
        next_page_token = search_res.get('nextPageToken')
        if not next_page_token: break

    # --- ロジックフィルタ ---
    filtered_videos = []
    for item in candidates:
        snippet = item['snippet']
        stats = item['statistics']
        duration_str = item['contentDetails']['duration']
        
        view_count = int(stats.get('viewCount', 0))
        duration_sec = parse_duration(duration_str)
        title = snippet['title']
        desc = snippet.get('description', '')

        # ロジック条件: 再生数1万以上 / 90秒以上 / 日本語含有
        if view_count >= 10000 and duration_sec >= 90 and is_japanese(title + desc):
            filtered_videos.append({
                "video_id": item['id'],
                "title": title,
                "description": desc[:1000],
                "channel_title": snippet['channelTitle'],
                "thumbnail_url": snippet['thumbnails']['high']['url'],
                "view_count": view_count,
                "duration": duration_str,
                "published_at": snippet['publishedAt'],
                "is_analyzed": False
            })

    # --- 書き込み処理 (Upsert) ---
    new_records_count = 0
    if filtered_videos:
        # 既存IDをチェックして新規追加分をカウント
        target_ids = [v['video_id'] for v in filtered_videos]
        existing = supabase.table("YouTubeMV_Japanese").select("video_id").in_("video_id", target_ids).execute()
        existing_ids = {r['video_id'] for r in existing.data}
        new_records_count = len([v for v in filtered_videos if v['video_id'] not in existing_ids])

        # バルク・アップサート実行 (一回の通信で完了)
        supabase.table("YouTubeMV_Japanese").upsert(filtered_videos, on_conflict="video_id").execute()

    # ログ出力
    print(f"------------------------------------")
    print(f"対象: {len(candidates)} 件")
    print(f"抽出: {len(filtered_videos)} 件（フィルタ通過）")
    print(f"書き込み実施: {new_records_count} 件（新規追加分）")
    print(f"消費ユニット数: {total_quota} units")
    
    return total_quota

if __name__ == "__main__":
    grand_total_quota = 0
    # 15年前(2011)から現在(2026)まで
    for year in range(2011, 2027):
        try:
            grand_total_quota += fetch_yearly_data(year)
            time.sleep(1) # 回線負荷軽減
        except Exception as e:
            print(f"  ❌ {year}年のエラー: {e}")

    print(f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"🎉 全年代の収集が完了しました！")
    print(f"トータルの消費ユニット数: {grand_total_quota} units")
    print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
