import os
import requests
import json
import re
import base64
from supabase import create_client

# 環境変数
SB_URL = os.environ.get("SUPABASE_URL")
SB_KEY = os.environ.get("SUPABASE_ANON_KEY")
GEMINI_KEY = os.environ.get("GEMINI_API_KEY")

# Supabase初期化
supabase = create_client(SB_URL, SB_KEY)

def extract_json(text):
    try:
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match: return json.loads(match.group())
        return None
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

    # APIの窓口（v1の安定版を直叩き）
    api_url = f"https://generativelanguage.googleapis.com/v1/models/gemini-1.5-flash:generateContent?key={GEMINI_KEY}"

    for v in videos:
        print(f"🧐 判定中: {v['title']}")
        try:
            # 画像をダウンロードしてBase64変換
            img_data = base64.b64encode(requests.get(v['thumbnail_url']).content).decode('utf-8')
            
            # 直接APIに送るデータ（JSON）を作成
            payload = {
                "contents": [{
                    "parts": [
                        {"text": f"動画タイトル: {v['title']}\nチャンネル名: {v['channel_title']}\n指示: アーティスト公式のMusic Videoなら true、それ以外は false。JSON形式のみで回答: {{\"is_official\": boolean, \"tags\": [\"#タグ1\"]}}"},
                        {"inline_data": {"mime_type": "image/jpeg", "data": img_data}}
                    ]
                }]
            }

            # API実行
            response = requests.post(api_url, json=payload, headers={'Content-Type': 'application/json'})
            resp_json = response.json()

            # AIの回答テキストを取り出す
            ai_text = resp_json['candidates'][0]['content']['parts'][0]['text']
            result = extract_json(ai_text)

            if result:
                supabase.table("YouTubeMV_Japanese").update({
                    "is_official_mv": result.get("is_official", True),
                    "ai_tags": result.get("tags", []),
                    "is_analyzed": True
                }).eq("video_id", v['video_id']).execute()
                status = "✅ 採用" if result.get("is_official") else "❌ 却下"
                print(f"  > {status}")
            else:
                print(f"  ⚠️ 解析失敗: {ai_text}")

        except Exception as e:
            print(f"  ⚠️ エラー詳細: {str(e)}")
            if 'resp_json' in locals(): print(f"  ⚠️ APIレスポンス: {resp_json}")

if __name__ == "__main__":
    analyze_and_filter(5)
