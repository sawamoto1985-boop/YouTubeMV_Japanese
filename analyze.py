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
genai.configure(api_key=GEMINI_KEY)
# モデルをここで定義
model = genai.GenerativeModel('gemini-1.5-flash')

def extract_json(text):
    try:
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match: return json.loads(match.group())
        return json.loads(text)
    except: return None

def analyze_and_filter(limit=5): # まずは5件でテスト
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
        print(f"🧐 判定中: {v['title']}")
        try:
            img_data = requests.get(v['thumbnail_url']).content
            
            prompt = (
                f"動画タイトル: {v['title']}\n"
                f"チャンネル名: {v['channel_title']}\n\n"
                "指示: アーティスト公式のMusic Videoなら true、それ以外（リアクション、歌ってみた、ライブ、切り抜き）は false。\n"
                "JSON形式で回答: {\"is_official\": boolean, \"tags\": [\"#タグ1\"]}"
            )

            # 最もシンプルな画像＋テキスト送信
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
                print(f"  > 結果: {result.get('is_official')}")
            else:
                print(f"  ⚠️ 解析失敗")

        except Exception as e:
            print(f"  ⚠️ エラー: {str(e)}")

if __name__ == "__main__":
    analyze_and_filter(5)
