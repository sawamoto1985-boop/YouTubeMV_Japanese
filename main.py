import os
import time
import re
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from supabase import create_client

# 環境変数
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")
SB_URL = os.environ.get("SUPABASE_URL")
SB_KEY = os.environ.get("SUPABASE_ANON_KEY")

youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
supabase = create_client(SB_URL, SB_KEY)

def is_japanese(text):
    """タイトルまたは概要欄に日本語が1文字以上含まれるか判定"""
    if not text: return False
    return bool(re.search(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]', text))

def parse_duration(duration_str):
    """ISO8601形式(PT1M30S)を秒数に変換"""
    m = re.search(r'(\d+)M', duration_str)
    s = re.search(r'(\d+)S', duration_str)
    h = re.search(r'(\d+)H', duration_str)
    return (int(h.group(1)) * 3600 if h else 0) + \
           (int(m.group(1)) * 60 if m else 0) + \
           (int(s.group(1)) if s else 0)

def fetch_yearly_data(year):
    print(f"\n📅 {year}年の処理を開始します")
    start_time = f"{year}-01-01T00:00:00Z"
    end_time = f"{year}-12-31T23:59:59Z"
    
    # 検索クエリ（API側での条件設定）
    query = '(MV "公式") | ("Music Video" "公式") | (MV "Official") | ("Music Video" "Official")'
    
    candidates = []
    quota_spent = 0
    
    try:
        # 1. API検索 (50件×2回 = 最大100件)
        next_page_token = None
        for _ in range(2):
            search_res = youtube.search().list(
                q=query, part="id", maxResults=50, type="video",
                videoCategoryId="10", videoDuration="medium", # 音楽ジャンル / Shorts排除
                relevanceLanguage="ja", regionCode="JP",
                publishedAfter=start_time, publishedBefore=end_time,
                order="viewCount", pageToken=next_page_token
            ).execute()
            quota_spent += 100
            
            ids = [item['id']['videoId'] for item in search_res.get('items', [])]
            if not ids: break
            
            # 2. 動画詳細をバッチ取得
            details_res = youtube.videos().list(
                id=",".join(ids),
                part="snippet,statistics,contentDetails"
            ).execute()
            quota_spent += 1
            candidates.extend(details_res.get('items', []))
            
            next_page_token = search_res.get('nextPageToken')
            if not next_page_token: break

        # 3. ロジックフィルタ (再生数1万以上 / 90秒以上 / 日本語含有)
        filtered_videos = []
        seen_ids = set() # 同一バッチ内での重複排除（21000エラー対策）
        
        for item in candidates:
            v_id = item['id']
            if v_id in seen_ids: continue
            
            snippet = item['snippet']
            stats = item['statistics']
            duration_sec = parse_duration(item['contentDetails']['duration'])
            view_count = int(stats.get('viewCount', 0))
            title = snippet['title']
            desc = snippet.get('description', '')

            if view_count >= 10000 and duration_sec >= 90 and is_japanese(title + desc):
                filtered_videos.append({
                    "video_id": v_id,
                    "title": title,
                    "description": desc[:1000],
                    "channel_title": snippet['channelTitle'],
                    "thumbnail_url": snippet['thumbnails']['high']['url'],
                    "view_count": view_count,
                    "duration": item['contentDetails']['duration'],
                    "published_at": snippet['publishedAt'],
                    "is_analyzed": False
                })
                seen_ids.add(v_id)

        # 4. 書き込み（Upsert）
        new_records_count = 0
        if filtered_videos:
            # 新規追加分をカウントするための照会
            target_ids = [v['video_id'] for v in filtered_videos]
            existing = supabase.table("YouTubeMV_Japanese").select("video_id").in_("video_id", target_ids).execute()
            existing_ids = {r['video_id'] for r in existing.data}
            new_records_count = len([v for v in filtered_videos if v['video_id'] not in existing_ids])

            # バルク・アップサート
            supabase.table("YouTubeMV_Japanese").upsert(filtered_videos, on_conflict="video_id").execute()

        print(f"------------------------------------")
        print(f"対象: {len(candidates)} 件 / 抽出: {len(filtered_videos)} 件")
        print(f"書き込み実施: {new_records_count} 件（新規追加分）")
        print(f"消費ユニット数: {quota_spent} units")
        
        return quota_spent

    except HttpError as e:
        if e.resp.status == 403:
            print(f"⚠️ クォータ上限に達しました。処理を中断します。")
            return "QUOTA_EXCEEDED"
        else:
            print(f"❌ APIエラー: {e}")
            return 0

if __name__ == "__main__":
    grand_total_quota = 0
    for year in range(2011, 2027):
        result = fetch_yearly_data(year)
        
        if result == "QUOTA_EXCEEDED":
            break
        
        grand_total_quota += result
        time.sleep(1) # 短時間の連続アクセス回避

    print(f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"🎉 処理が終了しました")
    print(f"合計消費ユニット数: {grand_total_quota} units")
    print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
