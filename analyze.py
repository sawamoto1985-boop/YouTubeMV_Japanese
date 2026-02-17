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

groq_client = Groq(api_key=GROQ_API_KEY)
supabase = create_client(SB_URL, SB_KEY)

def analyze_videos():
    # 取得件数を20件に増量
    res = supabase.table("YouTubeMV_Japanese")\
        .select("video_id, title, description, channel_title")\
        .eq("is_analyzed", False)\
        .limit(20).execute()

    if not res.data:
        print("解析対象のデータがありません。")
        return

    videos = res.data
    # 毎回違う動画から始まるようにシャッフル
    random.shuffle(videos)

    print(f"📦 合計 {len(videos)} 件の解析を開始します...")

    # ループ件数を最大20件に拡大
    for video in videos[:20]:
        video_id = video['video_id']
        print(f"\n🔍 解析中: {video['title']}")
        
        prompt = f"""
        YouTube動画情報から歌手名、曲名、タイアップを抽出してJSONで答えて。
        タイトル: {video['title']}
        チャンネル名: {video['channel_title']}
        概要欄: {video['description'][:800]}
        """

        try:
            completion = groq_client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": "JSONのみで回答。項目: singer_name, song_title, tie_up, is_official_mv"},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"}
            )

            result = json.loads(completion.choices[0].message.content)

            supabase.table("YouTubeMV_Japanese").update({
                "singer_name": result.get("singer_name"),
                "song_title": result.get("song_title"),
                "tie_up": result.get("tie_up"),
                "is_official_mv": result.get("is_official_mv"),
                "is_analyzed": True
            }).eq("video_id", video_id).execute()
            
            print(f"✅ 成功: {result.get('singer_name')} - {result.get('song_title')}")
            
            # 1分間に詰め込みすぎないよう12秒待機 (1分で5件ペース)
            time.sleep(12)

        except Exception as e:
            if "429" in str(e):
                print("⚠️ レート制限(429)に達しました。ここで終了します。")
                break
            print(f"❌ エラー: {e}")
            continue

if __name__ == "__main__":
    analyze_videos()
