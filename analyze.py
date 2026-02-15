import os
import requests
import json
import base64
import re
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
        return json.loads(match.group()) if match else None
    except:
        return None

def analyze_and_filter(limit=5):
    print(f"📋 未解析の動画を {limit} 件取得します...")
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

    # 【重要】リストにあった最新モデル「gemini-2.5-flash」を指定
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_KEY}"
    headers = {'Content-Type': 'application/json'}

    for v in videos:
        print(f"🧐 解析中: {v['title']}")
        
        try:
            # 画像ダウンロード & Base64変換
            img_data = requests.get(v['thumbnail_url']).content
            b64_img = base64.b64encode(img_data).decode('utf-8')

            # プロンプト
            prompt = (
                f"動画タイトル: {v['title']}\n"
                f"チャンネル名: {v['channel_title']}\n\n"
                "指示:\n"
                "この動画は「アーティスト公式のMusic Video」ですか？\n"
                "Live映像、歌ってみた、切り抜き、リアクション動画は false にしてください。\n"
                "回答は以下のJSON形式のみで出力してください。\n"
                "{\"is_official\": boolean, \"reason\": \"理由を短く\", \"tags\": [\"#雰囲気タグ1\", \"#タグ2\"]}"
            )

            # データ作成
            payload = {
                "contents": [{
                    "parts": [
                        {"text": prompt},
                        {"inline_data": {"mime_type": "image/jpeg", "data": b64_img}}
                    ]
                }]
            }

            # APIリクエスト
            response = requests.post(url, headers=headers, json=payload)
            result = response.json()

            if "error" in result:
                print(f"  ❌ APIエラー: {result['error']['message']}")
                continue

            # 結果保存
            ai_text = result['candidates'][0]['content']['parts'][0]['text']
            json_data = extract_json(ai_text)
            
            if json_data:
                is_official = json_data.get("is_official", False)
                tags = json_data.get("tags", [])
                
                supabase.table("YouTubeMV_Japanese").update({
                    "is_official_mv": is_official,
                    "ai_tags": tags,
                    "is_analyzed": True
                }).eq("video_id", v['video_id']).execute()

                print(f"  > 判定: {'✅ 公式' if is_official else '❌ 対象外'}")
            else:
                print(f"  ⚠️ JSON解析失敗: {ai_text[:50]}...")

        except Exception as e:
            print(f"  ⚠️ システムエラー: {e}")

if __name__ == "__main__":
    analyze_and_filter(5)
