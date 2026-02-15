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
    try:
        resp = httpx.get(url, timeout=10.0)
        return base64.b64encode(resp.content).decode("utf-8")
    except:
        return None

def analyze_videos():
    # 件数を5件に絞り、確実に1つずつ終わらせる
    res = supabase.table("YouTubeMV_Japanese")\
        .select("video_id, title, description, thumbnail_url, channel_title")\
        .eq("is_analyzed", False)\
        .limit(5).execute()

    if not res.data:
        print("解析対象のデータがありません。")
        return

    for video in res.data:
        video_id = video['video_id']
        print(f"\n🔍 解析開始: {video['title']}")
        
        img_b64 = get_image_base64(video['thumbnail_url'])
        
        # プロンプトの簡略化（負荷軽減）
        prompt = f"""
        日本の音楽情報の特定。
        タイトル: {video['title']}
        チャンネル: {video['channel_title']}
        概要欄: {video['description'][:800]}

        1. singer_name: 正式な歌手名
        2. song_title: 純粋な曲名
        3. tie_up: 作品名（不明なら「なし」）
        4. is_official_mv: 公式MVならtrue
        """

        try:
            contents = [prompt]
            if img_b64:
                contents.append(types.Part.from_bytes(data=base64.b64decode(img_b64), mime_type="image/jpeg"))

            # 最初はGoogle検索なしで試行（リミット対策）
            response = client.models.generate_content(
                model="gemini-2.0-flash",
                contents=contents,
                config=types.GenerateContentConfig(
                    # 検索が必要な場合のみ有効にするように調整（ここでは一旦OFFで安定化）
                    # tools=[types.Tool(google_search_retrieval=types.GoogleSearchRetrieval())], 
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
            
            print(f"✅ 解析成功: {result.singer_name}")
            print("⏳ 冷却期間 (30秒待機)...")
            time.sleep(30)

        except Exception as e:
            if "429" in str(e):
                print("⚠️ 強力なレート制限。今回の実行を終了します。")
                break 
            else:
                print(f"❌ エラー: {e}")
                # エラーが出たものは一旦スキップして次に進めるよう、フラグだけ変えるか検討
                time.sleep(10)

if __name__ == "__main__":
    analyze_videos()
