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

def get_unanalyzed_count():
    """未解析データの総数を取得する"""
    try:
        res = supabase.table("YouTubeMV_Japanese") \
            .select("id", count="exact", head=True) \
            .eq("is_analyzed", False) \
            .execute()
        return res.count
    except:
        return "?"

def analyze_batch(limit=10, session_total=0):
    # 開始前の残り件数をチェック
    remaining = get_unanalyzed_count()
    print(f"\n📋 バッチ開始: {limit}件取得します (DB残り: {remaining}件)")
    
    res = supabase.table("YouTubeMV_Japanese") \
        .select("video_id, thumbnail_url, title, channel_title") \
        .eq("is_analyzed", False) \
        .order("view_count", desc=True) \
        .limit(limit) \
        .execute()

    videos = res.data
    if not videos:
        return 0

    # 安定版の gemini-2.0-flash を使用
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_KEY}"
    headers = {'Content-Type': 'application/json'}

    for i, v in enumerate(videos):
        current_session_count = session_total + (i + 1)
        print(f"   ▶ [{current_session_count}件目] {v['title']}")
        
        try:
            img_data = requests.get(v['thumbnail_url']).content
            b64_img = base64.b64encode(img_data).decode('utf-8')

            prompt = (
                f"動画タイトル: {v['title']}\n"
                f"チャンネル名: {v['channel_title']}\n\n"
                "指示:\n"
                "サムネイルとタイトルから判断して、この動画は「アーティスト公式のMusic Video」ですか？\n"
                "Live映像、歌ってみた、切り抜き、リアクション動画は false にしてください。\n"
                "回答は以下のJSON形式のみで出力してください。\n"
                "{\"is_official\": boolean, \"reason\": \"理由を短く\"}"
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
                msg = result['error']['message']
                print(f"      ❌ APIエラー: {msg}")
                print("      🧊 クールダウン中（60秒待機）...")
                time.sleep(60)
                continue

            if 'candidates' in result:
                ai_text = result['candidates'][0]['content']['parts'][0]['text']
                json_data = extract_json(ai_text)
                
                if json_data:
                    supabase.table("YouTubeMV_Japanese").update({
                        "is_official_mv": json_data.get("is_official", False),
                        "is_analyzed": True 
                    }).eq("video_id", v['video_id']).execute()
                    
                    status = '✅ 公式' if json_data.get('is_official') else '❌ 対象外'
                    reason = json_data.get('reason', '理由なし')
                    print(f"      > 判定: {status} ({reason})")
                else:
                    print(f"      ⚠️ JSON解析失敗")
            else:
                print(f"      ⚠️ 想定外のエラー")

        except Exception as e:
            print(f"      ⚠️ システムエラー: {e}")

        # 進捗表示
        remaining_now = get_unanalyzed_count()
        print(f"      📊 [進捗] 今回の完了: {current_session_count}件 | DB残り: {remaining_now}件")
        
        # 15秒待機（必須）
        print("      ⏳ 待機中(15秒)...")
        time.sleep(15)
    
    return len(videos)

if __name__ == "__main__":
    total_processed_session = 0
    print("🚀 解析プロセスを開始します...")
    
    while True:
        count = analyze_batch(10, total_processed_session)
        if count == 0:
            print("\n🎉 すべての解析が完了しました！未解析データは0件です。")
            break
        
        total_processed_session += count
        print(f"\n🍵 バッチ休憩中... (今回合計: {total_processed_session}件 完了)")
        time.sleep(10)
