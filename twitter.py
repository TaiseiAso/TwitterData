# coding: utf-8

"""Twitterから二話者間の複数ターンの長文対話を取得"""
__author__ = "Aso Taisei"
__version__ = "1.0.1"
__date__ = "24 Apr 2019"


# 必要モジュールのインポート
import yaml
import os
import json
import re
import time
import unicodedata
import tweepy
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
import MeCab


# 分かち書きするモジュール
tagger = MeCab.Tagger('-Owakati')

# 指定した複数のインデックスに対応する要素をリストから削除するラムダ式
dellist = lambda items, indices: [item for idx, item in enumerate(items) if idx not in indices]

# ストリームを再開する際の待機時間
tcpip_delay = 0.25
MAX_TCPIP_TIMEOUT = 16


class QueueListener(StreamListener):
    """流れるツイートを取得するモジュール群のクラス"""
    def __init__(self, config, api_config):
        """
        コンストラクタ
        @param config 設定ファイル情報
        @param api_config Twitter APIの情報
        """
        super(QueueListener, self).__init__()

        # メンバの初期化
        self.queue = []
        self.BATCH_SIZE = 100
        self.turn_cnt = 0
        self.dialog_cnt = 0
        self.tweet_ids = []
        self.cum_time = 0
        self.start_time = time.time()

        # チェックポイントの読み込み
        self.load_tmp()

        # 保存ファイル名の情報取得
        fn = config['filename']
        self.inp_fn = fn['input_file']
        self.tar_fn = fn['target_file']
        self.dig_fn = fn['dialog_file']

        # 保存するかどうかを取得
        fd = config['dump']
        self.trn_fd = fd['turn_file']
        self.dig_fd = fd['dialog_file']

        # tweepy関連の情報取得
        cfg_auth = api_config['twitter_API']
        self.auth = OAuthHandler(cfg_auth['consumer_key'], cfg_auth['consumer_secret'])
        self.auth.set_access_token(cfg_auth['access_token'], cfg_auth['access_token_secret'])
        self.api = tweepy.API(self.auth)

    # 例外処理 #################################################################
    def on_error(self, status):
        """
        Streamオブジェクトでの処理中にステータスコード200以外が返ってきた際に呼ばれるメソッド
        @param status メッセージ
        @return True: 継続、False: 終了
        """
        print('ON ERROR:', status)
        return self.save_tmp()

    def on_limit(self, track):
        """
        データが流量オーバーした際に呼ばれるメソッド
        @param track メッセージ
        """
        print('ON LIMIT:', track)
        self.save_tmp()
        return

    def on_exception(self, exception):
        """
        Streamオブジェクトでの処理中に例外が発生した際に呼ばれるメソッド
        @param exception メッセージ
        """
        print('ON EXCEPTION:', exception)
        self.save_tmp()
        return

    def on_connect(self):
        """接続した際に呼ばれるメソッド"""
        print('ON CONNECT')
        return

    def on_disconnect(self, notice):
        """
        Twitter側から切断された際に呼ばれるメソッド
        @param notice メッセージ
        """
        print('ON DISCONNECT:', notice.code)
        self.save_tmp()
        return

    def on_timeout(self):
        """
        Streamのコネクションがタイムアウトした際に呼ばれるメソッド
        @return True: 継続、False: 終了
        """
        print('ON TIMEOUT')
        return self.save_tmp()

    def on_warning(self, notice):
        """
        Twitter側からの処理警告がきた際に呼ばれるメソッド
        @param notice メッセージ
        """
        print('ON WARNING:', notice.message)
        self.save_tmp()
        return
    ############################################################################

    def on_data(self, data):
        """
        処理すべきデータが飛んできた際に呼ばれるメソッド
        @param data json形式のツイートデータ
        @return True: 成功、False: 失敗
        """
        # json形式のデータを読み込む
        raw = json.loads(data)

        # 二話者の複数ターンの対話のみを取得
        if not self.on_status(raw):
            return False
        return True

    # 対話データをキューに一時保存する
    def on_status(self, raw):
        """
        ツイートをキューに保存する
        @param raw ツイートデータの構造体
        @return True: 成功、False: 失敗
        """
        if isinstance(raw.get('in_reply_to_status_id'), int):
            line = [raw['in_reply_to_status_id'], raw['user']['id'], self.del_username(unicodedata.normalize('NFKC', raw['text'])), raw['id']]

            if self.check(line[2]):
                line[2] = self.del_morpheme(self.normalize(line[2]))

                if line[2] != "":
                    self.queue.append([line])

                    self.tweet_ids = self.tweet_ids[-1000000:]
                    self.tweet_ids.append(line[3])

                    while len(self.queue) == self.BATCH_SIZE:
                        if not self.lookup():
                            return False
        return True

    def lookup(self):
        """
        キューにあるツイートに対応するリプライ先を一斉に一度だけ取得
        リプライ先がそれ以上存在しないデータはファイルに保存
        @return True: 成功、False: 失敗
        """
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
                if self.check(line[2]) and self.talker_check(line[1], idx) and line[3] not in self.tweet_ids:
                    line[2] = self.del_morpheme(self.normalize(line[2]))

                    # 長さが適切か最終判定
                    if line[2] != "":
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
                if len(self.queue[idx]) >= 2:
                    if not self.dump(self.queue[idx]):
                        return False

        self.queue = dellist(self.queue, dump_idxs)
        return True

    def dump(self, lines):
        """
        対話データをファイルに保存する
        @param lines 保存する対話データ
        @return True: 成功、False: 失敗
        """
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

        if self.dig_fd:
            with open("data/" + self.dig_fn + ".txt", 'a', encoding='utf-8') as f:
                f.write("> " + str(self.dialog_cnt) + " --------------------\n")
                for tweet in tweets:
                    f.write(tweet + "\n")

        if self.trn_fd:
            with open("data/" + self.inp_fn + ".txt", 'a', encoding='utf-8') as f_in,\
            open("data/" + self.tar_fn + ".txt", 'a', encoding='utf-8') as f_tar:
                for i in range(len(tweets) - 1):
                    f_in.write(tweets[i] + "\n")
                    f_tar.write(tweets[i + 1] + "\n")

        # 標準出力に進捗状況を出力
        print("#", end="", flush=True)
        if self.dialog_cnt % 10 == 0:
            print(" (" + self.dig_fn + ": " + str(self.dialog_cnt) + ", " + self.inp_fn + "/" + self.tar_fn + ": " + str(self.turn_cnt) + ") " + ('%.2f' % (self.cum_time + time.time() - self.start_time)) + "[sec]", flush=True)

        return True

    def del_username(self, tweet):
        """
        ツイートデータのテキストからユーザ名を除去
        @param tweet ツイートデータのテキスト
        @return ユーザ名を除去したツイートデータのテキスト
        """
        tweet = re.sub("(^|\s)(@|＠)(\w+)", "", tweet)
        return tweet

    def check(self, tweet):
        """
        ツイートデータのテキストに不適切な情報が含まれていないかを判定
        @param tweet ツイートデータのテキスト
        @return True: 適切、False: 不適切
        """
        # URLを含む（画像も含む？）
        if re.compile("((ftp|http|https):\/\/(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&amp;%@!&#45;\/]))?)").search(tweet):
            return False
        # ハッシュタグを含む
        if re.compile("(?:^|[^ーー゛゜々ヾヽぁ-ヶ一-龠a-zA-Z0-9&_/>]+)[#＃]([ー゛゜々ヾヽぁ-ヶ一-龠a-zA-Z0-9_]*[ー゛゜々ヾヽぁ-ヶ一-龠a-zA-Z]+[ー゛゜々ヾヽぁ-ヶ一-龠a-zA-Z0-9_]*)").search(tweet):
            return False
        # 英数字や特定の記号を含む
        if re.compile("[a-zA-Z0-9_]").search(tweet):
            return False
        # ツイッター用語を含む
        if re.compile("アイコン|あいこん|トプ|とぷ|ヘッダー|へっだー|たぐ|ツイ|ツイート|ついーと|ふぉろ|フォロ|リプ|りぷ|リツ|りつ|いいね|お気に入り|ふぁぼ|ファボ|リム|りむ|ブロック|ぶろっく|スパブロ|すぱぶろ|ブロ解|ぶろ解|鍵|アカ|垢|タグ|ダイレクトメッセージ|空りぷ|空リプ|巻き込|まきこ|マキコ").search(tweet):
            return False
        # 時期の表現を含む
        if re.compile("明日|あした|明後日|あさって|明々後日|明明後日|しあさって|今日|昨日|おととい|きょう|きのう|一昨日|来週|再来週|今年|先週|先々週|今年|去年|来年|おととい|らいしゅう|さらいしゅう|せんしゅう|せんせんしゅう|きょねん|らいねん|ことし|こんねん").search(tweet):
            return False
        # 漢数字を含む
        if re.compile("[一二三四五六七八九十百千万億兆〇]").search(tweet):
            return False
        # 特定のネットスラングを含む
        if re.compile("ナカーマ|イキスギ|いきすぎ|スヤァ|すやぁ|うぇーい|ウェーイ|おなしゃす|アザッス|あざっす|ドヤ|どや|ワカリミ|わかりみ").search(tweet):
            return False
        return True

    def talker_check(self, talker, idx):
        """
        二話者の交互発話による対話であるかを判定
        @param talker 話者ID
        @return True: 適切、False: 不適切
        """
        if talker != self.queue[idx][-1][1] and (len(self.queue[idx]) == 1 or talker == self.queue[idx][-2][1]):
            return True
        return False

    def normalize(self, tweet):
        """
        ツイートデータのテキストに正規化などのフィルタ処理を行う
        @param tweet ツイートデータのテキスト
        @return フィルタ処理を施したツイートデータのテキスト
        """
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

        tweet = re.sub("(\.\s*)+", " ... ", tweet)
        tweet = re.sub("！", " ！\n", tweet)
        tweet = re.sub("？", " ？\n", tweet)
        tweet = re.sub("!\?", " !?\n", tweet)

        tweet = re.sub("\n(\s*[～ー])+", "\n", tweet)

        tweet = re.sub("^([\s\n]*[。、！？!?ー～]+)+", "", tweet)
        tweet = re.sub("(.+?)\\1{3,}", "\\1\\1\\1", tweet)

        return tweet

    def del_morpheme(self, tweet):
        """
        ツイートデータのテキストから特定の形態素を除去する
        @param tweet ツイートデータのテキスト
        @return 特定の形態素を除去したツイートデータのテキスト
        """
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
                result += add_result + " "

        return result.strip()

    def save_tmp(self):
        """
        ツイートIDのリストや、収集した対話数をチェックポイントとして一時保存
        @return True: 成功、False: 失敗
        """
        if not os.path.isdir("tmp"):
            os.mkdir("tmp")

        with open("tmp/cnt.txt", 'w', encoding='utf-8') as f:
            f.write(str(self.dialog_cnt) + "\n")
            f.write(str(self.turn_cnt) + "\n")
            f.write(str(self.cum_time + time.time() - self.start_time) + "\n")

        with open("tmp/id.txt", 'w', encoding='utf-8') as f:
            for tweet_id in self.tweet_ids:
                f.write(str(tweet_id) + "\n")

        return True

    def load_tmp(self):
        """
        チェックポイントを読み込む
        @return True: 成功、False: 失敗
        """
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

        return True


def get_twitter_corpus(config, api_config):
    """
    ツイッターよりコーパスを作成
    @param config 設定ファイル情報
    @param api_config Twitter APIの情報
    """
    while True:
        try:
            # キューリスナー作成
            listener = QueueListener(config, api_config)

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
    # 設定ファイルを読み込む
    config = yaml.load(stream=open("config/config.yml", 'rt'), Loader=yaml.SafeLoader)
    # Twitter API の情報を読み込む
    api_config = yaml.load(stream=open("config/api.yml", 'rt'), Loader=yaml.SafeLoader)

    # ツイート収集開始
    get_twitter_corpus(config, api_config)
