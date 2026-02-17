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

def analyze_with_retry(contents, video_id, max_retries=3):
    """指数バックオフによるリトライ処理"""
    for i in range(max_retries):
        try:
            response = client.models.generate_content(
                model="gemini-2.0-flash", # 通らなければ "gemini-1.5-flash" に変更
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
            return response.parsed
        except Exception as e:
            if "429" in str(e) and i < max_retries - 1:
                wait_time = (2 ** i) * 30 + random.uniform(0, 10)
                print(f"⚠️ 制限中... {wait_time:.1f}秒後にリトライします ({i+1}/{max_retries})")
                time.sleep(wait_time)
            else:
                raise e

def analyze_videos():
    res = supabase.table("YouTubeMV_Japanese")\
        .select("video_id, title, description, thumbnail_url, channel_title")\
        .eq("is_analyzed", False)\
        .limit(10).execute()

    if not res.data:
        print("解析対象なし")
        return

    videos = res.data
    random.shuffle(videos)

    for video in videos[:3]: # 1回のRunで3件まで確実に狙う
        video_id = video['video_id']
        print(f"\n🔍 解析開始: {video['title']}")
        
        img_b64 = get_image_base64(video['thumbnail_url'])
        prompt = f"以下の動画の歌手名、曲名、タイアップを特定して。\nタイトル: {video['title']}\n概要: {video['description'][:500]}"

        try:
            contents = [prompt]
            if img_b64:
                contents.append(types.Part.from_bytes(data=base64.b64decode(img_b64), mime_type="image/jpeg"))

            result = analyze_with_retry(contents, video_id)

            supabase.table("YouTubeMV_Japanese").update({
                "singer_name": result.singer_name, "song_title": result.song_title,
                "tie_up": result.tie_up, "is_official_mv": result.is_official_mv,
                "is_analyzed": True
            }).eq("video_id", video_id).execute()
            
            print(f"✅ 解析成功: {result.singer_name}")
            time.sleep(10)

        except Exception as e:
            print(f"❌ 最終エラー: {e}")
            continue

if __name__ == "__main__":
    analyze_videos()
