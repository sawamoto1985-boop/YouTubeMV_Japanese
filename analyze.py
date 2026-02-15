import os
import requests
import json
import base64
import re
import time
from supabase import create_client

# 環境変数
SB_URL = os.environ.get("SUPABASE_URL")
SB_KEY = os.environ.get("SUPABASE_ANON_KEY")
GEMINI_KEY = os.environ.get("GEMINI_API_KEY")

supabase = create_client(SB_URL, SB_KEY)

def extract_json(text):
    try:
        match = re.search(r'\{.*\}', text, re.DOTALL)
        return json.loads(match.group()) if match else None
    except:
        return None

def analyze_batch(limit=10):
    """
    未解析データを指定件数だけ処理する関数
    処理した件数を返します（0なら完了）
    """
    print(f"📋 未解析データの検索中...（{limit}件ずつ）")
    
    # 👇 ここが「判定済みを除外する」最強のフィルターです
    res = supabase.table("YouTubeMV_Japanese") \
        .select("video_id, thumbnail_url, title, channel_title") \
        .eq("is_analyzed", False) \
        .order("view_count", desc=True) \
        .limit(limit) \
        .execute()

    videos = res.data
    if not videos:
        return 0  # もう未解析データはない

    # 最新モデルを指定（Gemini 2.5 Flash）
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_KEY}"
    headers = {'Content-Type': 'application/json'}

    for i, v in enumerate(videos):
        print(f"   [{i+1}/{len(videos)}] 🧐 {v['title']}")
        
        try:
            img_data = requests.get(v['thumbnail_url']).content
            b64_img = base64.b64encode(img_data).decode('utf-8')

            prompt = (
                f"動画タイトル: {v['title']}\n"
                f"チャンネル名: {v['channel_title']}\n\n"
                "指示:\n"
                "この動画は「アーティスト公式のMusic Video」ですか？\n"
                "Live映像、歌ってみた、切り抜き、リアクション動画は false にしてください。\n"
                "回答は以下のJSON形式のみで出力してください。\n"
                "{\"is_official\": boolean, \"reason\": \"理由を短く\", \"tags\": [\"#雰囲気タグ1\", \"#タグ2\"]}"
            )

            payload = {
                "contents": [{
                    "parts": [
                        {"text": prompt},
                        {"inline_data": {"mime_type": "image/jpeg", "data": b64_img}}
                    ]
                }]
            }

            response = requests.post(url, headers=headers, json=payload)
            result = response.json()

            if "error" in result:
                print(f"      ❌ APIエラー: {result['error']['message']}")
                time.sleep(5)
                continue

            # 結果保存
            if 'candidates' in result:
                ai_text = result['candidates'][0]['content']['parts'][0]['text']
                json_data = extract_json(ai_text)
                
                if json_data:
                    # ここで is_analyzed を True にすることで、次回の対象から外れます
                    supabase.table("YouTubeMV_Japanese").update({
                        "is_official_mv": json_data.get("is_official", False),
                        "ai_tags": json_data.get("tags", []),
                        "is_analyzed": True 
                    }).eq("video_id", v['video_id']).execute()

                    print(f"      > 判定: {'✅ 公式' if json_data.get('is_official') else '❌ 対象外'}")
                else:
                    print(f"      ⚠️ JSON解析失敗")
            else:
                print(f"      ⚠️ 想定外のエラー: {result}")

        except Exception as e:
            print(f"      ⚠️ システムエラー: {e}")

        # API制限回避のための休憩
        time.sleep(4)
    
    return len(videos)

if __name__ == "__main__":
    # 🔁 全データが終わるまで無限ループで回す設定
    total_processed = 0
    while True:
        count = analyze_batch(10) # 10件ずつ確実に進める
        if count == 0:
            print("\n🎉 すべての解析が完了しました！未解析データはもうありません。")
            break
        total_processed += count
        print(f"🍵 休憩中... (これまでの合計処理数: {total_processed}件)\n")
        time.sleep(10) # バッチ間の長めの休憩
