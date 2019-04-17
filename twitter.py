##################################################
# Twitterから二話者間の複数ターンの長文対話を取得
# 実装開始日: 2019/4/8
# 実装完了日: 2019/4/15
# 実行方法: $ python twitter.py
# 備考: 深夜でも1発話ペア/1秒くらいのペースで集まる
# 備考: 中断しても再開時に番号が続きからになりツイート重複もしない
##################################################


# 必要モジュールのインポート
import yaml, os, json, re, time, tweepy, unicodedata
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
import MeCab


# Twitter API の情報を読み込む
config = yaml.load(open("config/twitter.yml", 'rt'))

# 分かち書きするモジュール
tagger = MeCab.Tagger('-Owakati')

# 指定した複数インデックスをリストから削除するラムダ式
dellist = lambda items, indices: [item for idx, item in enumerate(items) if idx not in indices]

# ストリームを再開する際の待機時間
tcpip_delay = 0.25
MAX_TCPIP_TIMEOUT = 16


# 流れるツイートを取得するモジュール群のクラス
class QueueListener(StreamListener):
    def __init__(self):
        super(QueueListener, self).__init__()
        self.queue = []
        self.BATCH_SIZE = 100
        self.turn_cnt = 0
        self.dialog_cnt = 0
        self.tweet_ids = []
        self.cum_time = 0
        self.start_time = time.time()

        self.load_tmp()

        # tweepy関連の情報取得
        cfg_auth = config['twitter_API']
        self.auth = OAuthHandler(cfg_auth['consumer_key'], cfg_auth['consumer_secret'])
        self.auth.set_access_token(cfg_auth['access_token'], cfg_auth['access_token_secret'])
        self.api = tweepy.API(self.auth)


    # 例外処理 ################################
    def on_error(self, status):
        print('ON ERROR:', status)
        self.save_tmp()
        return True

    def on_limit(self, track):
        print('ON LIMIT:', track)
        self.save_tmp()
        return

    def on_exception(self, exception):
        print('ON EXCEPTION:', exception)
        self.save_tmp()
        return

    def on_connect(self):
        print('ON CONNECT')
        return

    def on_disconnect(self, notice):
        print('ON DISCONNECT:', notice.code)
        self.save_tmp()
        return

    def on_timeout(self):
        print('ON TIMEOUT')
        self.save_tmp()
        return True

    def on_warning(self, notice):
        print('ON WARNING:', notice.message)
        self.save_tmp()
        return
    ##########################################


    # 流れてくるツイートをキャッチ
    def on_data(self, data):
        # json形式のデータを読み込む
        raw = json.loads(data)

        # 二話者の複数ターンの対話のみを取得
        if self.on_status(raw) is False:
            return False

        # 対話でない、または、対話の取得に成功したならばTrue
        return True


    # 対話データをキューに一時保存する
    def on_status(self, raw):
        if isinstance(raw.get('in_reply_to_status_id'), int):
            line = [raw['in_reply_to_status_id'], raw['user']['id'], self.del_username(unicodedata.normalize('NFKC', raw['text'])), raw['id']]

            if self.check(line):
                line[2] = self.del_morpheme(self.normalize(line[2]))

                if self.len_check(line[2]):
                    lines = [line]
                    self.queue.append(lines)

                    self.tweet_ids = self.tweet_ids[-1000000:]
                    self.tweet_ids.append(line[3])

                    while len(self.queue) == self.BATCH_SIZE:
                        self.lookup()


    # リプライ先IDからツイートを取得
    def lookup(self):
        ids = []
        for lines in self.queue:
            ids.append(lines[-1][0])

        replys = self.api.statuses_lookup(ids)
        replys_dic = {reply.id_str: [reply.in_reply_to_status_id, reply.user.id, self.del_username(unicodedata.normalize('NFKC', reply.text)), reply.id] for reply in replys}

        dump_idxs = []

        for idx in range(self.BATCH_SIZE):
            interrupte = False
            line = replys_dic.get(str(ids[idx]))

            # リプライ先が本当に存在しているかを確認
            if line:
                # 使用データの候補として適切かを判定
                if self.check(line) and self.talker_check(line, idx) and line[3] not in self.tweet_ids:
                    line[2] = self.del_morpheme(self.normalize(line[2]))

                    # 長さが適切か最終判定
                    if self.len_check(line[2]):
                        self.queue[idx].append(line)
                        self.tweet_ids.append(line[3])

                        # リプライ先がなければ対話のさかのぼりを終了して保存
                        if not isinstance(line[0], int):
                            interrupte = True
                    else:
                        interrupte = True
                else:
                    interrupte = True
            else:
                interrupte = True

            # リプライ探索中断して正常な部分のみを保存
            if interrupte:
                dump_idxs.append(idx)

                # 対話のうち適切な部分だけ保存する
                if len(self.queue[idx]) >= 2:
                    self.dump(self.queue[idx])

        self.queue = dellist(self.queue, dump_idxs)


    # ツイートをファイルに出力保存する
    def dump(self, lines):
        # 時系列順に並べ替え
        lines.reverse()

        # ツイート内容だけのリストを作成
        tweets = [line[2] for line in lines]

        # 進捗確認のためのカウントを進める
        self.turn_cnt += len(tweets) - 1
        self.dialog_cnt += 1

        # ファイルに出力保存
        if not os.path.isdir("data"):
            os.mkdir("data")

        with open("data/twitter.txt", 'a', encoding='utf-8') as f:
            f.write("> " + str(self.dialog_cnt) + " --------------------\n")
            for i, tweet in enumerate(tweets):
                f.write(tweet)
                if i < len(tweets) - 1:
                    f.write("\n")

        with open("data/twitter_many_input.txt", 'a', encoding='utf-8') as f_in,\
        open("data/twitter__many_target.txt", 'a', encoding='utf-8') as f_out:
            for i in range(len(tweets) - 1):
                f_in.write(tweets[i] + "\n")
                f_out.write(tweets[i + 1] + "\n")

        tweets = [tweet.replace("\n", " ").strip() for tweet in tweets]

        with open("data/twitter_input.txt", 'a', encoding='utf-8') as f_in,\
        open("data/twitter_target.txt", 'a', encoding='utf-8') as f_out:
            for i in range(len(tweets) - 1):
                f_in.write(tweets[i] + "\n")
                f_out.write(tweets[i + 1] + "\n")

        # 標準出力に進捗状況を出力
        print("#", end="", flush=True)
        if self.dialog_cnt % 10 == 0:
            print(" (" + str(self.dialog_cnt) + ":" + str(self.turn_cnt) + ") " + ('%.2f' % (self.cum_time + time.time() - self.start_time)) + "[sec]", flush=True)


    # ツイート中からユーザ名を除去
    def del_username(self, tweet):
        tweet = re.sub("(^|\s)(@|＠)(\w+)", "", tweet)
        return tweet


    # 不適切な情報が含まれていないかを判定
    def check(self, line):
        # URLを含む（画像も含む？）
        if re.compile("((ftp|http|https):\/\/(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&amp;%@!&#45;\/]))?)").search(line[2]):
            return False
        # ハッシュタグを含む
        if re.compile("(?:^|[^ーー゛゜々ヾヽぁ-ヶ一-龠a-zA-Z0-9&_/>]+)[#＃]([ー゛゜々ヾヽぁ-ヶ一-龠a-zA-Z0-9_]*[ー゛゜々ヾヽぁ-ヶ一-龠a-zA-Z]+[ー゛゜々ヾヽぁ-ヶ一-龠a-zA-Z0-9_]*)").search(line[2]):
            return False
        # 英数字や特定の記号を含む
        if re.compile("[a-zA-Z0-9_]").search(line[2]):
            return False
        # ツイッター用語を含む
        if re.compile("アイコン|あいこん|トプ|とぷ|ヘッダー|へっだー|たぐ|ツイ|ツイート|ついーと|ふぉろ|フォロ|リプ|りぷ|リツ|りつ|いいね|お気に入り|ふぁぼ|ファボ|リム|りむ|ブロック|ぶろっく|スパブロ|すぱぶろ|ブロ解|ぶろ解|鍵|アカ|垢|タグ|ダイレクトメッセージ|空りぷ|空リプ|巻き込|まきこ|マキコ").search(line[2]):
            return False
        # 絵文字に頻繁に使用される表現を含む
        #if re.compile("ノシ|ﾉｼ|ノノ|ﾉﾉ|ノツ|ﾉﾂ").search(line[2]):
        #    return False
        # 時期の表現を含む
        if re.compile("明日|あした|明後日|あさって|明々後日|明明後日|しあさって|今日|昨日|おととい|きょう|きのう|一昨日|来週|再来週|今年|先週|先々週|今年|去年|来年|おととい|らいしゅう|さらいしゅう|せんしゅう|せんせんしゅう|きょねん|らいねん|ことし|こんねん").search(line[2]):
            return False
        # 漢数字による時の表現を含む
        #if re.compile("[一二三四五六七八九十百千万億兆〇]+(秒|分|時間|日|週|月|[かカヵケヶ]月|年|世紀)").search(line[2]):
        #    return False
        # 漢数字を含む
        if re.compile("[一二三四五六七八九十百千万億兆〇]").search(line[2]):
            return False
        # 特定のネットスラングを含む
        if re.compile("ナカーマ|イキスギ|いきすぎ|スヤァ|すやぁ|うぇーい|ウェーイ|おなしゃす|アザッス|あざっす|ドヤ|どや|ワカリミ|わかりみ").search(line[2]):
            return False
        return True


    # 二話者の交互発話による対話であるかを判定
    def talker_check(self, line, idx):
        if line[1] != self.queue[idx][-1][1] and (len(self.queue[idx]) == 1 or line[1] == self.queue[idx][-2][1]):
            return True
        return False


    # 保存前にツイートに正規化などの加工を行う（この時点で文ごとに改行をしておく）
    def normalize(self, tweet):
        # 括弧の中を消す（そもそも括弧を含むツイートを除外する場合は必要ない）
        tweet = re.sub("\([^笑泣嬉悲驚汗爆渋苦困死楽怒哀呆殴涙藁]+?\)", " ", tweet)

        tweet = re.sub("[^ぁ-んァ-ヶｧ-ｳﾞ一-龠々ー～〜、。！？!?,，.．\r\n]", " ", tweet)

        tweet = re.sub("[,，]", "、", tweet)
        tweet = re.sub("[．.]", "。", tweet)
        tweet = re.sub("〜", "～", tweet)
        tweet = re.sub("、(\s*、)+|。(\s*。)+", "...", tweet)

        tweet = re.sub("!+", "！", tweet)
        tweet = re.sub("！(\s*！)+", "！", tweet)
        tweet = re.sub("\?+", "？", tweet)
        tweet = re.sub("？(\s*？)+", "？", tweet)

        tweet = re.sub("～(\s*～)+", "～", tweet)
        tweet = re.sub("ー(\s*ー)+", "ー", tweet)

        tweet = re.sub("\r\n|\n|\r", "。", tweet)

        tweet += "。"
        tweet = re.sub("[、。](\s*[、。])+", "。", tweet)

        tweet = re.sub("[。、！](\s*[。、！])+", "！", tweet)
        tweet = re.sub("[。、？](\s*[。、？])+", "？", tweet)
        tweet = re.sub("((！\s*)+？|(？\s*)+！)(\s*[！？])*", "!?", tweet)

        for w in ["っ", "笑", "泣", "嬉", "悲", "驚", "汗", "爆", "渋", "苦", "困", "死", "楽", "怒", "哀", "呆", "殴", "涙", "藁"]:
            tweet = re.sub(w + "(\s*" + w + ")+", " " + w + " ", tweet)

        tweet = re.sub("、\s*([笑泣嬉悲驚汗爆渋苦困死楽怒哀呆殴涙藁])\s*。", " \\1。", tweet)
        tweet = re.sub("(。|！|？|!\?)\s*([笑泣嬉悲驚汗爆渋苦困死楽怒哀呆殴涙藁])\s*。", " \\2\\1", tweet)

        tweet = re.sub("、", " 、 ", tweet)
        tweet = re.sub("。", " 。\n", tweet)
        # 、。を除去する場合
        #tweet = re.sub("、", " ", tweet)
        #tweet = re.sub("。", "\n", tweet)

        tweet = re.sub("(\.\s*)+", " ... ", tweet)
        tweet = re.sub("！", " ！\n", tweet)
        tweet = re.sub("？", " ？\n", tweet)
        tweet = re.sub("!\?", " !?\n", tweet)

        tweet = re.sub("\n(\s*[～ー])+", "\n", tweet)

        tweet = re.sub("^([\s\n]*[。、！？!?ー～]+)+", "", tweet)
        tweet = re.sub("(.+?)\\1{3,}", "\\1\\1\\1", tweet)

        return tweet


    # 保存直前のツイートから特定の形態素を除去する
    def del_morpheme(self, tweet):
        lines = tweet.strip().split("\n")
        result = ""

        for line in lines:
            add_result = ""
            morphemes = tagger.parse(line).strip().split()
            for morpheme in morphemes:
                if morpheme not in ["ノ", "ーノ", "ロ", "艸", "屮", "罒", "灬", "彡", "ヮ", "益",\
                "皿", "タヒ", "厂", "厂厂", "啞", "卍", "ノノ", "ノノノ", "ノシ", "ノツ",\
                "癶", "癶癶", "乁", "乁厂", "マ", "んご", "んゴ", "ンゴ", "にき", "ニキ", "ナカ", "み", "ミ"]:
                    if morpheme not in ["つ", "っ"] or add_result != "":
                        add_result += morpheme + " "
            add_result = add_result.strip()
            if add_result not in ["、", "。", "！", "？", "!?", "", "... 。", "... ！", "... ？", "... !?",\
            "人 。", "つ 。", "っ 。", "笑 。", "笑 ！", "笑 ？", "笑 !?"]:
                result += add_result + "\n"

        return result


    # ツイートの長さが適切かどうかを判定する
    def len_check(self, tweet):
        if tweet != "":
            return True
        return False


    # ツイートIDリストや、収集した対話数を一時保存
    def save_tmp(self):
        if not os.path.isdir("tmp"):
            os.mkdir("tmp")

        with open("tmp/cnt.txt", 'w', encoding='utf-8') as f:
            f.write(str(self.dialog_cnt) + "\n")
            f.write(str(self.turn_cnt) + "\n")
            f.write(str(self.cum_time + time.time() - self.start_time) + "\n")

        with open("tmp/id.txt", 'w', encoding='utf-8') as f:
            for tweet_id in self.tweet_ids:
                f.write(str(tweet_id) + "\n")


    # 一時保存したファイルを読み込む
    def load_tmp(self):
        if os.path.isdir("tmp"):
            if os.path.isfile("tmp/cnt.txt"):
                with open("tmp/cnt.txt", 'r', encoding='utf-8') as f:
                    self.dialog_cnt = int(f.readline().strip())
                    self.turn_cnt = int(f.readline().strip())
                    self.cum_time = float(f.readline().strip())

            if os.path.isfile("tmp/id.txt"):
                with open("tmp/id.txt", 'r', encoding='utf-8') as f:
                    tweet_id = f.readline()
                    while tweet_id:
                        self.tweet_ids.append(int(tweet_id.strip()))
                        tweet_id = f.readline()


# ツイッターからコーパスを作成
def get_twitter_corpus():
    while True:
        try:
            # キューリスナー作成
            listener = QueueListener()

            # ストリームを開く
            stream = Stream(listener.auth, listener)

            # ストリームフィルタ（どれかにヒットすれば拾う）
            stream.filter(languages=["ja"], track=['。', '，', '！', '.', '!', ',', '?', '？', '、', '私', '俺', '(', ')', '君', 'あなた'])

        except KeyboardInterrupt:
            listener.save_tmp()
            stream.disconnect()
            break
        except:
            global tcpip_delay
            time.sleep(min(tcpip_delay, MAX_TCPIP_TIMEOUT))
            tcpip_delay += 0.25


if __name__ == '__main__':
    get_twitter_corpus()
