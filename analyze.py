import os
import time
import random
import json
from groq import Groq
from supabase import create_client

# 環境変数
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
SB_URL = os.environ.get("SUPABASE_URL")
SB_KEY = os.environ.get("SUPABASE_ANON_KEY")

# クライアント初期化
groq_client = Groq(api_key=GROQ_API_KEY)
supabase = create_client(SB_URL, SB_KEY)

def analyze_videos_with_groq():
    # 未解析データを20件取得
    res = supabase.table("YouTubeMV_Japanese")\
        .select("video_id, title, description, channel_title")\
        .eq("is_analyzed", False)\
        .limit(20).execute()

    if not res.data:
        print("解析対象のデータがありません。")
        return

    videos = res.data
    random.shuffle(videos)

    for video in videos[:10]: # Groqは速いので少し多めに回せます
        print(f"\n🔍 Groqで解析中: {video['title']}")
        
        # Llama 3 70B（高性能モデル）を使用
        prompt = f"""
        以下のYouTube動画の情報から、歌手名、曲名、タイアップ情報を特定し、JSON形式で回答してください。
        
        【動画タイトル】: {video['title']}
        【チャンネル名】: {video['channel_title']}
        【概要欄】: {video['description'][:800]}

        【出力フォーマット】
        {{
          "singer_name": "歌手の正式名称",
          "song_title": "純粋な曲名のみ",
          "tie_up": "タイアップ作品名（不明なら「なし」）",
          "is_official_mv": true/false (公式MV本編ならtrue)
        }}
        """

        try:
            completion = groq_client.chat.completions.create(
                model="llama3-70b-8192", # 高精度な70Bモデルを指定
                messages=[
                    {"role": "system", "content": "あなたは日本の音楽業界に詳しい専門家です。必ず指定されたJSON形式のみで回答してください。"},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"}
            )

            # 解析結果のパース
            result = json.loads(completion.choices[0].message.content)

            # Supabaseを更新
            supabase.table("YouTubeMV_Japanese").update({
                "singer_name": result["singer_name"],
                "song_title": result["song_title"],
                "tie_up": result["tie_up"],
                "is_official_mv": result["is_official_mv"],
                "is_analyzed": True
            }).eq("video_id", video['video_id']).execute()
            
            print(f"✅ 解析成功: {result['singer_name']} - {result['song_title']}")
            
            # Groqは短時間の連投に厳しい（RPM制限）ので、3〜5秒ほど待機
            time.sleep(5)

        except Exception as e:
            print(f"❌ エラー: {e}")
            if "rate_limit" in str(e).lower():
                print("⏳ Groqのレート制限に達しました。少し長めに待機します...")
                time.sleep(30)
            continue

if __name__ == "__main__":
    analyze_videos_with_groq()
