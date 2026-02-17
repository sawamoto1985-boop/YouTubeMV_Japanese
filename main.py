import os
import time
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from supabase import create_client

# 環境変数
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")
SB_URL = os.environ.get("SUPABASE_URL")
SB_KEY = os.environ.get("SUPABASE_ANON_KEY")

youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
supabase = create_client(SB_URL, SB_KEY)

# 今回ご指定いただいた新しいプレイリストID
PLAYLIST_IDS = [
    "PLH8SlvExlZpGuGWhKHr6FjKvYJ7zGoc-5", # 1つ目
    "PL_yex3sFlQmUIVO0sWd86NIVe7cSvtGbs", # 2つ目
    "PLiNIFt_GD2-Dzfi2XoHtgsk8eMLfOEwUw"  # 3つ目
]

def fetch_playlist_videos(playlist_id):
    print(f"\n📂 プレイリスト ID: {playlist_id} の取得を開始します")
    
    videos_to_insert = []
    next_page_token = None
    
    try:
        while True:
            # 1. プレイリスト内の動画ID一覧を取得
            res = youtube.playlistItems().list(
                playlistId=playlist_id,
                part="contentDetails,snippet",
                maxResults=50,
                pageToken=next_page_token
            ).execute()
            
            # 動画IDのリストを作成
            video_ids = [item['contentDetails']['videoId'] for item in res.get('items', [])]
            if not video_ids:
                break

            # 2. 動画の詳細情報（再生数、再生時間など）を取得
            stats_res = youtube.videos().list(
                id=",".join(video_ids),
                part="snippet,statistics,contentDetails"
            ).execute()

            for item in stats_res.get('items', []):
                snippet = item['snippet']
                stats = item.get('statistics', {})
                
                videos_to_insert.append({
                    "video_id": item['id'],
                    "title": snippet['title'],
                    "description": snippet.get('description', '')[:1000],
                    "channel_title": snippet['channelTitle'],
                    "thumbnail_url": snippet['thumbnails'].get('high', {}).get('url'),
                    "view_count": int(stats.get('viewCount', 0)) if 'viewCount' in stats else 0,
                    "duration": item['contentDetails']['duration'],
                    "published_at": snippet['publishedAt'],
                    "is_analyzed": False
                })

            next_page_token = res.get('nextPageToken')
            if not next_page_token:
                break

        # 3. Supabaseへの書き込み（upsertなので重複は自動更新）
        if videos_to_insert:
            supabase.table("YouTubeMV_Japanese").upsert(
                videos_to_insert, on_conflict="video_id"
            ).execute()
            print(f"✅ 合計 {len(videos_to_insert)} 件のデータを保存/更新しました")

    except HttpError as e:
        print(f"❌ APIエラーが発生しました: {e}")

if __name__ == "__main__":
    for pl_id in PLAYLIST_IDS:
        fetch_playlist_videos(pl_id)
        time.sleep(1) # API制限を考慮した待機
    print("\n🎉 指定された全プレイリストの同期が完了しました")
