import os
import requests
import json
import re
from supabase import create_client
from google import genai

# 環境変数
SB_URL = os.environ.get("SUPABASE_URL")
SB_KEY = os.environ.get("SUPABASE_ANON_KEY")
GEMINI_KEY = os.environ.get("GEMINI_API_KEY")

# クライアント初期化
supabase = create_client(SB_URL, SB_KEY)
client = genai.Client(api_key=GEMINI_KEY)

def extract_json(text):
    try:
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            return json.loads(match.group())
        return json.loads(text)
    except:
        return None

def analyze_and_filter(limit=10):
    # 再生数順に未解析データを取得
    res = supabase.table("YouTubeMV_Japanese") \
        .select("video_id, thumbnail_url, title, channel_title") \
        .eq("is_analyzed", False) \
        .order("view_count", desc=True) \
        .limit(limit) \
        .execute()

    videos = res.data
    if not videos:
        print("✅ 解析待ちの動画はありません。")
        return

    for v in videos:
        print(f"🧐 判定・解析中: {v['title']}")
        
        try:
            img_res = requests.get(v['thumbnail_url'])
            img_data = img_res.content
            
            prompt = f"""
            動画タイトル: {v['title']}
            チャンネル名: {v['channel_title']}
            
            指示:
            1. 「アーティスト本人/レーベル公式のMusic Video」なら true、それ以外（リアクション、歌ってみた、ライブ、切り抜き）は false。
            2. 公式MVの場合のみ、色、季節、時間帯、雰囲気を5つのハッシュタグで。
            
            JSON形式のみで回答:
            {{ "is_official": boolean, "reason": "15文字以内", "tags": ["#タグ1", "#タグ2", "#タグ3", "#タグ4", "#タグ5"] }}
            """

            # モデル名を最新版に固定して実行
            response = client.models.generate_content(
                model="gemini-1.5-flash", 
                contents=[
                    prompt,
                    genai.types.Part.from_bytes(data=img_data, mime_type="image/jpeg")
                ]
            )
            
            result = extract_json(response.text)

            if result:
                supabase.table("YouTubeMV_Japanese").update({
                    "is_official_mv": result.get("is_official", True),
                    "ai_tags": result.get("tags", []),
                    "is_analyzed": True
                }).eq("video_id", v['video_id']).execute()
                
                status = "✅ 採用" if result.get("is_official") else "❌ 却下"
                print(f"  > {status} | 理由: {result.get('reason')}")
            else:
                print(f"  ⚠️ JSON解析失敗: {response.text}")

        except Exception as e:
            print(f"  ⚠️ エラー: {str(e)}")

if __name__ == "__main__":
    analyze_and_filter(10)
