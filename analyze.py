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

    # 【重要】無料枠で画像解析を通すための、2026年現在の「唯一の窓口」と「モデル名」
    # gemini-1.5-flash 単体ではなく、バージョン番号を直打ちします
    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-002:generateContent?key={GEMINI_KEY}"

    for v in videos:
        print(f"🧐 判定中: {v['title']}")
        try:
            # 画像をダウンロードしてBase64変換
            img_data = base64.b64encode(requests.get(v['thumbnail_url']).content).decode('utf-8')
            
            payload = {
                "contents": [{
                    "parts": [
                        {"text": f"動画タイトル: {v['title']}\n指示: 公式MVなら true、それ以外は false。JSON: {{\"is_official\": boolean, \"tags\": [\"#タグ\"]}}"},
                        {"inline_data": {"mime_type": "image/jpeg", "data": img_data}}
                    ]
                }]
            }

            response = requests.post(api_url, json=payload, headers={'Content-Type': 'application/json'})
            resp_json = response.json()

            # ここでエラーが出た場合、詳細を即座に表示
            if 'error' in resp_json:
                print(f"  ❌ APIエラー: {resp_json['error']['message']}")
                continue

            ai_text = resp_json['candidates'][0]['content']['parts'][0]['text']
            result = extract_json(ai_text)

            if result:
                supabase.table("YouTubeMV_Japanese").update({
                    "is_official_mv": result.get("is_official", True),
                    "ai_tags": result.get("tags", []),
                    "is_analyzed": True
                }).eq("video_id", v['video_id']).execute()
                print(f"  > ✅ 完了: {result.get('is_official')}")
            else:
                print(f"  ⚠️ JSON解析失敗")

        except Exception as e:
            print(f"  ⚠️ エラー詳細: {str(e)}")

if __name__ == "__main__":
    analyze_and_filter(5)
