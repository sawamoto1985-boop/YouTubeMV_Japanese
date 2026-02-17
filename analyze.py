import os
import base64
import httpx
import time
import random
from google import genai
from google.genai import types
from supabase import create_client

# 環境変数
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
SB_URL = os.environ.get("SUPABASE_URL")
SB_KEY = os.environ.get("SUPABASE_ANON_KEY")

client = genai.Client(api_key=GEMINI_API_KEY)
supabase = create_client(SB_URL, SB_KEY)

def get_image_base64(url):
    try:
        resp = httpx.get(url, timeout=10.0)
        return base64.b64encode(resp.content).decode("utf-8")
    except: return None

def analyze_videos():
    # 未解析データを20件取得
    res = supabase.table("YouTubeMV_Japanese")\
        .select("video_id, title, description, thumbnail_url, channel_title")\
        .eq("is_analyzed", False)\
        .limit(20).execute()

    if not res.data:
        print("解析対象のデータがありません。")
        return

    videos = res.data
    random.shuffle(videos)

    # 1回の実行で5件処理
    for video in videos[:5]:
        video_id = video['video_id']
        print(f"\n🔍 解析開始: {video['title']}")
        
        img_b64 = get_image_base64(video['thumbnail_url'])
        
        # プロンプト：検索なしでもAIが推論しやすいように調整
        prompt = f"""
        以下のYouTube動画の情報（タイトル、チャンネル名、概要欄、サムネイル画像）から音楽メタデータを抽出してください。
        
        【動画情報】
        タイトル: {video['title']}
        チャンネル名: {video['channel_title']}
        概要欄: {video['description'][:1000]}

        【抽出項目】
        1. singer_name: 歌手の正式名称。
        2. song_title: 純粋な曲名のみ（【MV】やOfficial等の記号は除去）。
        3. tie_up: アニメ、映画、ドラマ等のタイアップ作品名。不明なら「なし」。
        4. is_official_mv: 公式のMusic Video本編であればtrue、それ以外（ライブ、カバー、Shorts、音源のみ等）はfalse。
        """

        try:
            contents = [prompt]
            if img_b64:
                contents.append(types.Part.from_bytes(data=base64.b64decode(img_b64), mime_type="image/jpeg"))

            # Google検索(tools)を外し、純粋な生成AIとして実行
            response = client.models.generate_content(
                model="gemini-2.0-flash",
                contents=contents,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema={
                        "type": "object",
                        "properties": {
                            "singer_name": {"type": "string"},
                            "song_title": {"type": "string"},
                            "tie_up": {"type": "string"},
                            "is_official_mv": {"type": "boolean"}
                        },
                        "required": ["singer_name", "song_title", "tie_up", "is_official_mv"]
                    }
                )
            )

            result = response.parsed
            supabase.table("YouTubeMV_Japanese").update({
                "singer_name": result.singer_name,
                "song_title": result.song_title,
                "tie_up": result.tie_up,
                "is_official_mv": result.is_official_mv,
                "is_analyzed": True
            }).eq("video_id", video_id).execute()
            
            print(f"✅ 解析成功: {result.singer_name} - {result.song_title}")
            print("⏳ 20秒待機中...")
            time.sleep(20)

        except Exception as e:
            if "429" in str(e):
                print("⚠️ クォータ制限中。実行を終了します。")
                return
            else:
                print(f"❌ エラー: {e}")
                time.sleep(5)

if __name__ == "__main__":
    analyze_videos()
