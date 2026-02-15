import os
import requests
import json
import re
import base64
from supabase import create_client

# --- 設定（GitHub Secretsから取得） ---
SB_URL = os.environ.get("SUPABASE_URL")
SB_KEY = os.environ.get("SUPABASE_ANON_KEY")
GEMINI_KEY = os.environ.get("GEMINI_API_KEY")

# Supabaseクライアント初期化
supabase = create_client(SB_URL, SB_KEY)

def extract_json(text):
    """AIの回答からJSON部分のみを抽出する"""
    try:
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            return json.loads(match.group())
        return None
    except Exception:
        return None

def analyze_and_filter(limit=5):
    # 解析待ちの動画を再生数順に取得
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

    # 【2026年最新】無料枠の404エラーを回避するエンドポイント（v1beta）
    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_KEY}"

    for v in videos:
        print(f"🧐 判定・解析中: {v['title']}")
        try:
            # 1. サムネイル画像をダウンロードしてBase64に変換
            img_response = requests.get(v['thumbnail_url'])
            img_base64 = base64.b64encode(img_response.content).decode('utf-8')
            
            # 2. AIへの指示（プロンプト）
            prompt = (
                f"動画タイトル: {v['title']}\n"
                f"チャンネル名: {v['channel_title']}\n\n"
                "指示:\n"
                "1. アーティスト本人の公式MVなら true、それ以外（ライブ、カバー、リアクション、切り抜き）は false。\n"
                "2. 公式MVの場合、雰囲気や色を表すタグを5つ生成。\n"
                "必ず以下のJSON形式のみで回答してください:\n"
                "{\"is_official\": boolean, \"reason\": \"string\", \"tags\": [\"string\"]}"
            )

            # 3. APIリクエスト送信
            payload = {
                "contents": [{
                    "parts": [
                        {"text": prompt},
                        {"inline_data": {"mime_type": "image/jpeg", "data": img_base64}}
                    ]
                }]
            }

            response = requests.post(api_url, json=payload, headers={'Content-Type': 'application/json'})
            resp_data = response.json()

            # APIエラーチェック
            if 'error' in resp_data:
                print(f"  ❌ APIエラー: {resp_data['error']['message']}")
                continue

            # 4. 回答の解析とSupabaseの更新
            ai_text = resp_data['candidates'][0]['content']['parts'][0]['text']
            result = extract_json(ai_text)

            if result:
                supabase.table("YouTubeMV_Japanese").update({
                    "is_official_mv": result.get("is_official", True),
                    "ai_tags": result.get("tags", []),
                    "is_analyzed": True
                }).eq("video_id", v['video_id']).execute()
                
                status = "✅ 採用" if result.get("is_official") else "❌ 却下"
                print(f"  > {status} | 理由: {result.get('reason')}")
            else:
                print(f"  ⚠️ JSON解析失敗: {ai_text[:50]}...")

        except Exception as e:
            print(f"  ⚠️ 実行エラー: {str(e)}")

if __name__ == "__main__":
    analyze_and_filter(5)
