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
    # 未解析データを取得 (ランダムに取得して同じ箇所でのループを防ぐ)
    res = supabase.table("YouTubeMV_Japanese")\
        .select("video_id, title, description, thumbnail_url, channel_title")\
        .eq("is_analyzed", False)\
        .limit(20).execute()

    if not res.data:
        print("解析対象のデータがありません。")
        return

    # 取得したリストをシャッフル
    videos = res.data
    random.shuffle(videos)

    # 1回の実行で最大5件だけ処理（無料枠の安全策）
    for video in videos[:5]:
        video_id = video['video_id']
        print(f"\n🔍 解析開始: {video['title']}")
        
        img_b64 = get_image_base64(video['thumbnail_url'])
        
        prompt = f"""
        日本の音楽情報の特定。Google検索を使用して正確な情報を回答してください。
        タイトル: {video['title']}
        チャンネル: {video['channel_title']}
        概要欄: {video['description'][:800]}

        1. singer_name: 歌手の正式名称（検索で裏取りすること）
        2. song_title: 純粋な曲名（装飾除去）
        3. tie_up: 作品名（アニメ/ドラマ/映画/CM等。検索で特定すること。無ければ「なし」）
        4. is_official_mv: 公式MV本編ならtrue
        """

        try:
            contents = [prompt]
            if img_b64:
                contents.append(types.Part.from_bytes(data=base64.b64decode(img_b64), mime_type="image/jpeg"))

            # Google検索を有効にして実行
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
            supabase.table("YouTubeMV_Japanese").update({
                "singer_name": result.singer_name,
                "song_title": result.song_title,
                "tie_up": result.tie_up,
                "is_official_mv": result.is_official_mv,
                "is_analyzed": True
            }).eq("video_id", video_id).execute()
            
            print(f"✅ 解析成功: {result.singer_name} - {result.song_title}")
            print("⏳ 30秒待機（クォータ保護）...")
            time.sleep(30)

        except Exception as e:
            if "429" in str(e):
                print("⚠️ 現在、Gemini APIの無料枠制限(limit: 0)にかかっています。")
                print("数時間〜1日置いてから再実行してください。")
                return # 429が出たら即終了してActionsを止める
            else:
                print(f"❌ エラー: {e}")
                time.sleep(5)

if __name__ == "__main__":
    analyze_videos()
