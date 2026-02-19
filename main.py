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

PLAYLIST_IDS = [
    "PLlguE8B_AmTOrSSERku8HjzuMPMEJMDjA",
    "PLIyWtPwrYr7bpfvf7W71MrugiqvSXjSCh",
    "PLiNIFt_GD2-DiggpSS51nmk3GpGIAY0RZ",
    "PLspoBPmaiV2L9JlES1p_4iivYXQdfwGut",
    "PL4fGSI1pDJn5FhDrWnRp2NLzJCoPliNgT",
    "PLIyWtPwrYr7baHUbGwNT7UtupEj8ptb5w",
    "PLB1PuqtbwVQmmso1pFcGvP7wPbw5hGBz6",
    "PL8NVbI3ifBL-eFltStZdscEWwqP37MTLr",
    "PLIyWtPwrYr7aN6ky3ge4_0hhO1cTakEwn",
    "PLNIx6V2GG_KPRwTYZ4jfZ_vuvSPGyR020",
    "PLH8SlvExlZpHM8GWkHxZzC9ioEvwih4h_",
    "PLH8SlvExlZpGtIqqkO99IV9BCn97X9SB0",
    "PL8NVbI3ifBL8e74tgQmmVb2KBmAzdNyWL",
    "PLIkZyCCllTL6pznBNUy808OrJh4ReFnBj",
    "PLSArS342teRCv9vRS9NpufsLTK8c9EpDo",
    "PLkR5H-8BSzoseXsB5Bu5vGYrQSEcN5k2F"
]

def fetch_playlist_videos(playlist_id):
    print(f"\n📂 プレイリスト ID: {playlist_id} の取得を開始します")
    
    # 辞書型を使い、video_idをキーにして重複を自動排除する
    unique_videos = {}
    next_page_token = None
    
    try:
        while True:
            res = youtube.playlistItems().list(
                playlistId=playlist_id,
                part="contentDetails",
                maxResults=50,
                pageToken=next_page_token
            ).execute()
            
            video_ids = [item['contentDetails']['videoId'] for item in res.get('items', [])]
            if not video_ids:
                break

            stats_res = youtube.videos().list(
                id=",".join(video_ids),
                part="snippet,statistics,contentDetails"
            ).execute()

            for item in stats_res.get('items', []):
                v_id = item['id']
                snippet = item['snippet']
                stats = item.get('statistics', {})
                
                # 重複があっても新しい方のデータで上書き（エラーにならない）
                unique_videos[v_id] = {
                    "video_id": v_id,
                    "title": snippet['title'],
                    "description": snippet.get('description', '')[:1000],
                    "channel_title": snippet['channelTitle'],
                    "thumbnail_url": snippet['thumbnails'].get('high', {}).get('url'),
                    "view_count": int(stats.get('viewCount', 0)) if 'viewCount' in stats else 0,
                    "duration": item['contentDetails']['duration'],
                    "published_at": snippet['publishedAt'],
                    "is_analyzed": False
                }

            next_page_token = res.get('nextPageToken')
            if not next_page_token:
                break

        # 辞書の値をリストに変換して書き込み
        final_list = list(unique_videos.values())
        
        if final_list:
            supabase.table("YouTubeMV_Japanese").upsert(
                final_list, on_conflict="video_id"
            ).execute()
            print(f"✅ 合計 {len(final_list)} 件のデータを保存・更新しました")

    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")

if __name__ == "__main__":
    for pl_id in PLAYLIST_IDS:
        fetch_playlist_videos(pl_id)
        time.sleep(1)
    print("\n🎉 全ての処理が完了しました")
