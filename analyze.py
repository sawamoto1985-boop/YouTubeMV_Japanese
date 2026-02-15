import os
import requests
from supabase import create_client
import google.generativeai as genai

# 環境変数
SB_URL = os.environ.get("SUPABASE_URL")
SB_KEY = os.environ.get("SUPABASE_ANON_KEY")
GEMINI_KEY = os.environ.get("GEMINI_API_KEY")

supabase = create_client(SB_URL, SB_KEY)
genai.configure(api_key=GEMINI_KEY)
model = genai.GenerativeModel('gemini-1.5-flash')

def analyze_and_filter():
    # 未解析の動画を10件取得
    res = supabase.table("YouTubeMV_Japanese") \
        .select("video_id, thumbnail_url, title, channel_title") \
        .eq("is_analyzed", False) \
        .limit(10) \
        .execute()

    videos = res.data
    if not videos:
        print("✅ 解析待ちの動画はありません。")
        return

    for v in videos:
        print(f"🧐 判定中: {v['title']}")
        
        # サムネイル画像の取得
        img_data = requests.get(v['thumbnail_url']).content
        
        # Geminiへのプロンプト（判定とタグ付けを同時に行う）
        prompt = f"""
        動画タイトル: {v['title']}
        チャンネル名: {v['channel_title']}
        
        この動画が「アーティスト本人の公式Music Video」であるかを判定し、以下のJSON形式で回答してください。
        
        {{
          "is_official": true/false,
          "reason": "判定理由（短く）",
          "tags": ["#タグ1", "#タグ2"]
        }}
        
        ※ライブ映像、歌ってみた、カバー、ファンメイド、アニメ本編の切り抜きは false にしてください。
        ※公式MVの場合は、画像から受ける印象（色、季節、雰囲気）をタグにしてください。
        """

        try:
            # 画像とテキストをGeminiに渡す
            response = model.generate_content([
                prompt,
                {"mime_type": "image/jpeg", "data": img_data}
            ])
            
            # 結果の解析（簡易的な抽出）
            import json
            # Geminiの回答からJSON部分を抽出
            result_text = response.text.replace('```json', '').replace('```', '').strip()
            result = json.loads(result_text)

            # Supabaseを更新
            supabase.table("YouTubeMV_Japanese").update({
                "is_official_mv": result.get("is_official", True),
                "ai_tags": result.get("tags", []),
                "is_analyzed": True
            }).eq("video_id", v['video_id']).execute()
            
            status = "✅ 採用" if result.get("is_official") else "❌ 却下"
            print(f"{status}: {v['title']} ({result.get('reason')})")

        except Exception as e:
            print(f"⚠️ エラー: {v['title']} の解析に失敗しました。 {e}")

if __name__ == "__main__":
    analyze_and_filter()
