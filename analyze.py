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

def analyze_videos():
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

    # 1回の実行で5件ずつ確実に処理
    for video in videos[:5]:
        video_id = video['video_id']
        print(f"\n🔍 Groq(Llama 3.3)で解析開始: {video['title']}")
        
        prompt = f"""
        以下のYouTube動画の情報から音楽メタデータを抽出してください。
        
        【動画情報】
        タイトル: {video['title']}
        チャンネル名: {video['channel_title']}
        概要欄: {video['description'][:800]}

        【出力項目】
        1. singer_name: 歌手の正式名称。
        2. song_title: 純粋な曲名のみ。
        3. tie_up: タイアップ作品名（不明なら「なし」）。
        4. is_official_mv: 公式MV本編ならtrue、ライブやカバーならfalse。
        """

        try:
            # 最新の Llama-3.3-70b-versatile を使用
            completion = groq_client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": "あなたは日本の音楽業界に詳しい専門家です。必ず指定されたJSON形式のみで回答してください。"},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"}
            )

            result = json.loads(completion.choices[0].message.content)

            # Supabaseを更新
            supabase.table("YouTubeMV_Japanese").update({
                "singer_name": result.get("singer_name"),
                "song_title": result.get("song_title"),
                "tie_up": result.get("tie_up"),
                "is_official_mv": result.get("is_official_mv"),
                "is_analyzed": True
            }).eq("video_id", video_id).execute()
            
            print(f"✅ 解析成功: {result.get('singer_name')} - {result.get('song_title')}")
            
            # レート制限回避のため少し長めに待機
            print("⏳ 15秒待機中...")
            time.sleep(15)

        except Exception as e:
            if "429" in str(e):
                print("⚠️ Groqのレート制限（RPM）に達しました。実行を中断します。")
                return
            else:
                print(f"❌ エラー: {e}")
                time.sleep(5)
                continue

if __name__ == "__main__":
    analyze_videos()
