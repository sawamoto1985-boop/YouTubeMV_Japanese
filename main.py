import os
import time
from googleapiclient.discovery import build
from supabase import create_client

# GitHub Secretsから読み込み
YT_API_KEY = os.environ.get("YOUTUBE_API_KEY")
SB_URL = os.environ.get("SUPABASE_URL")
SB_KEY = os.environ.get("SUPABASE_ANON_KEY")

supabase = create_client(SB_URL, SB_KEY)
youtube = build('youtube', 'v3', developerKey=YT_API_KEY)

def get_video_stats(video_ids):
    """動画IDのリストから再生回数を取得"""
    stats_request = youtube.videos().list(
        part="statistics",
        id=",".join(video_ids)
    )
    stats_response = stats_request.execute()
    
    stats_dict = {}
    for item in stats_response.get('items', []):
        stats_dict[item['id']] = int(item['statistics'].get('viewCount', 0))
    return stats_dict

def fetch_and_save_mvs(target_count=100): # 100件に変更
    collected_data = []
    next_page_token = None
    
    print(f"🚀 【テスト実行】邦楽MV収集開始（目標: {target_count}件）")

    while len(collected_data) < target_count:
        search_request = youtube.search().list(
            q="official music video",
            part="snippet",
            type="video",
            regionCode="JP",
            relevanceLanguage="ja",
            order="date",
            maxResults=50, # 1回で最大50件
            pageToken=next_page_token
        )
        search_response = search_request.execute()

        video_ids = [item['id']['videoId'] for item in search_response.get('items', [])]
        if not video_ids:
            break

        stats_dict = get_video_stats(video_ids)

        for item in search_response.get('items', []):
            v_id = item['id']['videoId']
            snippet = item['snippet']
            
            mv_data = {
                "video_id": v_id,
                "title": snippet['title'],
                "description": snippet['description'],
                "thumbnail_url": snippet['thumbnails']['high']['url'],
                "published_at": snippet['publishedAt'],
                "channel_title": snippet['channelTitle'],
                "view_count": stats_dict.get(v_id, 0),
                "is_analyzed": False
            }
            collected_data.append(mv_data)
            if len(collected_data) >= target_count:
                break

        next_page_token = search_response.get('nextPageToken')
        print(f"📈 取得状況: {len(collected_data)} / {target_count}")
        
        if not next_page_token or len(collected_data) >= target_count:
            break
        
        time.sleep(0.5)

    # SupabaseへUpsert
    if collected_data:
        # 100件なので一括で送信
        supabase.table("YouTubeMV_Japanese").upsert(collected_data).execute()
        print(f"✨ テスト完了！{len(collected_data)} 件を保存しました。")

if __name__ == "__main__":
    fetch_and_save_mvs(100)
