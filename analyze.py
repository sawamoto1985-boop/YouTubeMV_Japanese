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

# 初期化
supabase = create_client(SB_URL, SB_KEY)

# 【重要】無料枠のAPIキーで404を回避するための設定
genai.configure(api_key=GEMINI_KEY)

def extract_json(text):
    try:
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match: return json.loads(match.group())
        return json.loads(text)
    except: return None

def analyze_and_filter(limit=5):
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

    # 【重要】モデル名をフルパス「models/gemini-1.5-flash」に固定
    model = genai.GenerativeModel(model_name='models/gemini-1.5-flash')

    for v in videos:
        print(f"🧐 判定中: {v['title']}")
        try:
            img_data = requests.get(v['thumbnail_url']).content
            
            prompt = (
                f"動画タイトル: {v['title']}\n"
                f"チャンネル名: {v['channel_title']}\n\n"
                "指示: アーティスト公式のMusic Videoなら true、それ以外は false。\n"
                "JSON形式で回答: {\"is_official\": boolean, \"tags\": [\"#タグ1\"]}"
            )

            # 解析実行
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
                print(f"  > {status}")
            else:
                print(f"  ⚠️ JSON解析失敗")

        except Exception as e:
            # ここで詳細なエラーを出して原因を完全に特定します
            print(f"  ⚠️ エラー詳細: {str(e)}")

if __name__ == "__main__":
    analyze_and_filter(5)
