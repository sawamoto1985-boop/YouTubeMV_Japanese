import os
import base64
import httpx
import json
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
    # 未解析かつ一次抽出を通ったデータを取得
    res = supabase.table("YouTubeMV_Japanese")\
        .select("video_id, title, description, thumbnail_url, channel_title")\
        .eq("is_analyzed", False)\
        .limit(20).execute() # 一回の実行件数は任意に調整してください

    if not res.data:
        print("解析対象のデータがありません。")
        return

    for video in res.data:
        print(f"\n🔍 解析中: {video['title']}")
        
        img_b64 = get_image_base64(video['thumbnail_url'])
        
        prompt = f"""
        あなたは日本の音楽業界に精通したエージェントです。提供された動画情報、サムネイル画像、そしてGoogle検索を駆使して、正確なデータを抽出してください。

        【動画情報】
        タイトル: {video['title']}
        チャンネル名: {video['channel_title']}
        概要欄: {video['description'][:1500]}

        【抽出ルール】
        1. singer_name: 歌手/ユニットの正式名称。略称（例：ミスチル）ではなく正式名（例：Mr.Children）にすること。
        2. song_title: 純粋な曲名のみ。タイトルにある【MV】、Official Video、(Full Ver.)などの装飾記号や文言は徹底的に排除すること。
        3. tie_up: この曲が使われたアニメ、映画、ドラマ、CM等の作品名。概要欄に無ければGoogle検索で特定すること。無ければ「なし」と記載。
        4. is_official_mv: 以下の条件をすべて満たす場合のみ true。
           - 投稿者が本人、所属レーベル、または公式作品チャンネルである。
           - 動画内容がカバー、ライブ、Shorts、ダイジェスト、広告ではない「Music Video」本編であること。
        """

        try:
            # Gemini API呼び出し (Grounding: Google検索有効)
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
            }).eq("video_id", video['video_id']).execute()
            
            print(f"✅ 解析完了: {result.singer_name} - {result.song_title} (Official: {result.is_official_mv})")

        except Exception as e:
            print(f"❌ 解析エラー ({video['video_id']}): {e}")

if __name__ == "__main__":
    analyze_videos()
