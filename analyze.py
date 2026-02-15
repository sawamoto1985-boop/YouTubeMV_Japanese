import os
import base64
import httpx
import time
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
    """サムネイル画像をURLから取得してBase64に変換"""
    try:
        resp = httpx.get(url, timeout=10.0)
        return base64.b64encode(resp.content).decode("utf-8")
    except Exception as e:
        print(f"  ⚠️ 画像取得失敗: {e}")
        return None

def analyze_videos():
    # 未解析データを取得 (一度に多くやりすぎず10件程度にする)
    res = supabase.table("YouTubeMV_Japanese")\
        .select("video_id, title, description, thumbnail_url, channel_title")\
        .eq("is_analyzed", False)\
        .limit(10).execute()

    if not res.data:
        print("解析対象のデータがありません。")
        return

    for video in res.data:
        video_id = video['video_id']
        print(f"\n🔍 解析中: {video['title']}")
        
        img_b64 = get_image_base64(video['thumbnail_url'])
        
        prompt = f"""
        あなたは日本の音楽業界に精通した専門家です。以下の情報とGoogle検索を使い、正確なデータを抽出してください。

        【動画情報】
        タイトル: {video['title']}
        チャンネル名: {video['channel_title']}
        概要欄: {video['description'][:1000]}

        【抽出ルール】
        1. singer_name: 歌手の正式名称。略称ではなく正式名にすること。
        2. song_title: 純粋な曲名のみ。装飾記号や(Official Video)等は除去すること。
        3. tie_up: タイアップ作品名（アニメ、ドラマ、映画、CM等）。無ければ「なし」。
        4. is_official_mv: 本人・公式によるMusic Video本編ならtrue。それ以外（カバー、ライブ、Shorts等）はfalse。
        """

        try:
            contents = [prompt]
            if img_b64:
                contents.append(types.Part.from_bytes(data=base64.b64decode(img_b64), mime_type="image/jpeg"))

            response = client.models.generate_content(
                model="gemini-2.0-flash",
                contents=contents,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search_retrieval=types.GoogleSearchRetrieval())],
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

            # DBへ反映
            supabase.table("YouTubeMV_Japanese").update({
                "singer_name": result.singer_name,
                "song_title": result.song_title,
                "tie_up": result.tie_up,
                "is_official_mv": result.is_official_mv,
                "is_analyzed": True
            }).eq("video_id", video_id).execute()
            
            print(f"✅ 解析完了: {result.singer_name} - {result.song_title}")
            
            # レートリミット回避のための待機 (15秒)
            print("⏳ 15秒待機します...")
            time.sleep(15)

        except Exception as e:
            if "429" in str(e):
                print("⚠️ レートリミット到達。60秒停止します...")
                time.sleep(60)
            else:
                print(f"❌ 解析エラー ({video_id}): {e}")

if __name__ == "__main__":
    analyze_videos()
