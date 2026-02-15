import os
import requests
import json
import re
import base64
from supabase import create_client

# 環境変数（GitHub Secretsから自動取得）
SB_URL = os.environ.get("SUPABASE_URL")
SB_KEY = os.environ.get("SUPABASE_ANON_KEY")
GEMINI_KEY = os.environ.get("GEMINI_API_KEY")

# Supabase初期化
supabase = create_client(SB_URL, SB_KEY)

def extract_json(text):
    """AIの回答からJSONを抽出"""
    try:
        match = re.search(r'\{.*\}', text, re.DOTALL)
        return json.loads(match.group()) if match else None
    except:
        return None

def analyze_and_filter(limit=5):
    # 解析待ちの動画を取得
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

    # 【重要】無料枠で最も安定するエンドポイントとモデル名
    api_url = f"https://generativelanguage.googleapis.com/v1/models/gemini-1.5-flash:generateContent?key={GEMINI_KEY}"

    for v in videos:
        print(f"🧐 判定・解析中: {v['title']}")
        try:
            # 1. 画像のBase64エンコード
            img_data = base64.b64encode(requests.get(v['thumbnail_url']).content).decode('utf-8')
            
            # 2. リクエストの構築
            payload = {
                "contents": [{
                    "parts": [
                        {"text": f"動画タイトル: {v['title']}\nチャンネル名: {v['channel_title']}\n指示: 公式MVなら true、それ以外は false。JSON形式のみで回答: {{\"is_official\": boolean, \"tags\": [\"#タグ\"]}}"},
                        {"inline_data": {"mime_type": "image/jpeg", "data": img_data}}
                    ]
                }]
            }

            # 3. API実行
            response = requests.post(api_url, json=payload, headers={'Content-Type': 'application/json'})
            resp_json = response.json()

            if 'error' in resp_json:
                print(f"  ❌ APIエラー: {resp_json['error']['message']}")
                continue

            # 4. 結果の保存
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
                print(f"  ⚠️ JSON解析失敗")

        except Exception as e:
            print(f"  ⚠️ エラー: {str(e)}")

if __name__ == "__main__":
    analyze_and_filter(5)
