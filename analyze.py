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

# クライアント初期化
supabase = create_client(SB_URL, SB_KEY)
genai.configure(api_key=GEMINI_KEY)

# 404エラー対策：モデル名をシンプルに指定
model = genai.GenerativeModel('gemini-1.5-flash')

def extract_json(text):
    """Geminiの回答からJSON部分だけを抜き出す"""
    try:
        # ```json ... ``` の中身を探す
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            return json.loads(match.group())
        return json.loads(text)
    except:
        return None

def analyze_and_filter(limit=10):
    # 未解析の動画を「再生数順」に取得（注目度の高いものから解析）
    res = supabase.table("YouTubeMV_Japanese") \
        .select("video_id, thumbnail_url, title, channel_title") \
        .eq("is_analyzed", False) \
        .order("view_count", descending=True) \
        .limit(limit) \
        .execute()

    videos = res.data
    if not videos:
        print("✅ 解析待ちの動画はありません。")
        return

    for v in videos:
        print(f"🧐 判定・解析中: {v['title']}")
        
        try:
            # サムネイル画像の取得
            img_res = requests.get(v['thumbnail_url'])
            img_data = img_res.content
            
            prompt = f"""
            動画タイトル: {v['title']}
            チャンネル名: {v['channel_title']}
            
            以下の指示に従い、厳格に判定してください。
            1. この動画が「アーティスト本人またはレーベル公式のMusic Video」なら true、
               リアクション動画、歌ってみた、ライブ映像、切り抜きなら false にしてください。
            2. 公式MVの場合のみ、映像から連想される「色、季節、時間帯、雰囲気」を5つのハッシュタグにしてください。
            
            必ず以下のJSON形式のみで回答してください：
            {{
              "is_official": boolean,
              "reason": "判定理由を15文字以内で",
              "tags": ["#タグ1", "#タグ2", "#タグ3", "#タグ4", "#タグ5"]
            }}
            """

            # Geminiに画像とテキストを送信
            response = model.generate_content([
                prompt,
                {"mime_type": "image/jpeg", "data": img_data}
            ])
            
            # JSONの抽出と解析
            result = extract_json(response.text)

            if result:
                # Supabaseを更新
                supabase.table("YouTubeMV_Japanese").update({
                    "is_official_mv": result.get("is_official", True),
                    "ai_tags": result.get("tags", []),
                    "is_analyzed": True
                }).eq("video_id", v['video_id']).execute()
                
                status = "✅ 採用" if result.get("is_official") else "❌ 却下"
                print(f"  > {status} | 理由: {result.get('reason')} | タグ: {result.get('tags')}")
            else:
                print(f"  ⚠️ JSON解析失敗: {v['title']}")

        except Exception as e:
            print(f"  ⚠️ エラー発生: {v['title']} | {str(e)}")

if __name__ == "__main__":
    # テスト用なので10件。運用時は増やしてもOK
    analyze_and_filter(10)
