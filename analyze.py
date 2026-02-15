import os
import requests
import json
import re
from supabase import create_client
import google.generativeai as genai

# 環境変数
SB_URL = os.environ.get("SUPABASE_URL")
SB_KEY = os.environ.get("SUPABASE_ANON_KEY")
GEMINI_KEY = os.environ.get("GEMINI_API_KEY")

supabase = create_client(SB_URL, SB_KEY)
genai.configure(api_key=GEMINI_KEY)

def extract_json(text):
    try:
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match: return json.loads(match.group())
        return json.loads(text)
    except: return None

def analyze_and_filter(limit=10):
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

    # 【重要】ここを latest に変えることで、404エラー（モデル未発見）を強制回避します
    model = genai.GenerativeModel('models/gemini-1.5-flash-latest')

    for v in videos:
        print(f"🧐 判定・解析中: {v['title']}")
        try:
            img_res = requests.get(v['thumbnail_url'])
            img_data = img_res.content
            
            prompt = f"動画タイトル: {v['title']}\nチャンネル名: {v['channel_title']}\n\n指示: 1. アーティスト公式MVなら true、それ以外は false。 2. 公式MVの場合のみ、タグを5つ生成。 JSON形式のみで回答: {{ \"is_official\": boolean, \"reason\": \"string\", \"tags\": [\"string\"] }}"

            response = model.generate_content([
                prompt,
                {'mime_type': 'image/jpeg', 'data': img_data}
            ])
            
            result = extract_json(response.text)

            if result:
                supabase.table("YouTubeMV_Japanese").update({
                    "is_official_mv": result.get("is_official", True),
                    "ai_tags": result.get("tags", []),
                    "is_analyzed": True
                }).eq("video_id", v['video_id']).execute()
                
                status = "✅ 採用" if result.get("is_official") else "❌ 却下"
                print(f"  > {status}: {result.get('reason')}")
            else:
                print(f"  ⚠️ JSON解析失敗")

        except Exception as e:
            print(f"  ⚠️ エラー詳細: {str(e)}")

if __name__ == "__main__":
    analyze_and_filter(10)
