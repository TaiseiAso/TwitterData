# coding: utf-8

"""Twitterから二話者間の複数ターンの長文対話を取得"""
__author__ = "Aso Taisei"
__version__ = "1.0.2"
__date__ = "29 Apr 2019"


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
tagger = MeCab.Tagger('-Ochasen')

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
        inp_fn = fn['input_file']
        tar_fn = fn['target_file']
        dig_fn = fn['dialog_file']
        std_fn = fn['standard_file']
        prt_fn = fn['part_file']

        # 保存ファイルパス
        self.dig_fp = "data/" + dig_fn + ".txt"
        self.dig_std_fp = "data/" + dig_fn + "_" + std_fn + ".txt"
        self.dig_prt_fp = "data/" + dig_fn + "_" + prt_fn + ".txt"
        self.inp_fp = "data/" + inp_fn + ".txt"
        self.tar_fp = "data/" + tar_fn + ".txt"
        self.inp_std_fp = "data/" + inp_fn + "_" + std_fn + ".txt"
        self.tar_std_fp = "data/" + tar_fn + "_" + std_fn + ".txt"
        self.inp_prt_fp = "data/" + inp_fn + "_" + prt_fn + ".txt"
        self.tar_prt_fp = "data/" + tar_fn + "_" + prt_fn + ".txt"

        # 保存するかどうかを取得
        fd = config['dump']
        self.trn_fd = fd['turn_file']
        self.dig_fd = fd['dialog_file']
        self.std_fd = fd['standard_file']
        self.prt_fd = fd['part_file']

        # tweepy関連の情報取得
        cfg_auth = api_config['twitter_API']
        self.auth = OAuthHandler(cfg_auth['consumer_key'], cfg_auth['consumer_secret'])
        self.auth.set_access_token(cfg_auth['access_token'], cfg_auth['access_token_secret'])
        self.api = tweepy.API(self.auth)

        # 進捗を表示するかどうかを取得
        self.progress = config['print_progress']

        # 品詞のトークンを取得
        pt = config['part']
        self.noun_token = pt['noun']
        self.verb_token = pt['verb']
        self.adjective_token = pt['adjective']
        self.adverb_token = pt['adverb']
        self.particle_token = pt['particle']
        self.auxiliary_verb_token = pt['auxiliary_verb']
        self.conjunction_token = pt['conjunction']
        self.prefix_token = pt['prefix']
        self.filler_token = pt['filler']
        self.impression_verb_token = pt['impression_verb']
        self.three_dots_token = pt['three_dots']
        self.phrase_point_token = pt['phrase_point']
        self.reading_point_token = pt['reading_point']
        self.other_token = pt['other']

        # 標準形が存在しない場合の文字列
        self.unk_standard = config['unknown_standard']

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

    def on_status(self, raw):
        """
        ツイートをキューに保存する
        @param raw ツイートデータの構造体
        @return True: 成功、False: 失敗
        """
        if isinstance(raw.get('in_reply_to_status_id'), int):
            tweet = [raw['in_reply_to_status_id'], raw['user']['id'], self.del_username(unicodedata.normalize('NFKC', raw['text'])), raw['id']]

            if self.check(tweet[2]):
                tweet[2], standard, part = self.del_morpheme(self.normalize(tweet[2]))

                if tweet[2] != "":
                    self.queue.append([[tweet, standard, part]])

                    self.tweet_ids = self.tweet_ids[-1000000:]
                    self.tweet_ids.append(tweet[3])

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
        for dialogs in self.queue:
            ids.append(dialogs[-1][0][0])

        replys = self.api.statuses_lookup(ids)
        replys_dic = {reply.id_str: [reply.in_reply_to_status_id, reply.user.id, self.del_username(unicodedata.normalize('NFKC', reply.text)), reply.id] for reply in replys}

        dump_idxs = []

        for idx in range(self.BATCH_SIZE):
            interrupte = False
            tweet = replys_dic.get(str(ids[idx]))

            # リプライ先が本当に存在しているかを確認
            if tweet:
                # 使用データの候補として適切かを判定
                if self.check(tweet[2]) and self.talker_check(tweet[1], idx) and tweet[3] not in self.tweet_ids:
                    tweet[2], standard, part = self.del_morpheme(self.normalize(tweet[2]))

                    # 長さが適切か最終判定
                    if tweet[2] != "":
                        self.queue[idx].append([tweet, standard, part])
                        self.tweet_ids.append(tweet[3])

                        # リプライ先がなければ対話のさかのぼりを終了して保存
                        if not isinstance(tweet[0], int):
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

    def dump(self, dialog):
        """
        対話データをファイルに保存する
        @param dialog 保存する対話データ
        @return True: 成功、False: 失敗
        """
        # 時系列順に並べ替え
        dialog.reverse()

        # ツイート内容だけのリストを作成
        tweets = [[tweet[0][2], tweet[1], tweet[2]] for tweet in dialog]

        # 進捗確認のためのカウントを進める
        self.turn_cnt += len(tweets) - 1
        self.dialog_cnt += 1

        # ファイルに出力保存
        if not os.path.isdir("data"):
            os.mkdir("data")

        if self.dig_fd:
            with open(self.dig_fp, 'a', encoding='utf-8') as f:
                for tweet in tweets:
                    f.write(tweet[0] + "\n")
                f.write("\n")

            if self.std_fd:
                with open(self.dig_std_fp, 'a', encoding='utf-8') as f:
                    for tweet in tweets:
                        f.write(tweet[1] + "\n")
                    f.write("\n")

            if self.prt_fd:
                with open(self.dig_prt_fp, 'a', encoding='utf-8') as f:
                    for tweet in tweets:
                        f.write(tweet[2] + "\n")
                    f.write("\n")

        if self.trn_fd:
            with open(self.inp_fp, 'a', encoding='utf-8') as f_in,\
            open(self.tar_fp, 'a', encoding='utf-8') as f_tar:
                for i in range(len(tweets) - 1):
                    f_in.write(tweets[i][0] + "\n")
                    f_tar.write(tweets[i + 1][0] + "\n")

            if self.std_fd:
                with open(self.inp_std_fp, 'a', encoding='utf-8') as f_in,\
                open(self.tar_std_fp, 'a', encoding='utf-8') as f_tar:
                    for i in range(len(tweets) - 1):
                        f_in.write(tweets[i][1] + "\n")
                        f_tar.write(tweets[i + 1][1] + "\n")

            if self.prt_fd:
                with open(self.inp_prt_fp, 'a', encoding='utf-8') as f_in,\
                open(self.tar_prt_fp, 'a', encoding='utf-8') as f_tar:
                    for i in range(len(tweets) - 1):
                        f_in.write(tweets[i][2] + "\n")
                        f_tar.write(tweets[i + 1][2] + "\n")

        # 標準出力に進捗状況を出力
        if self.progress:
            print("#", end="", flush=True)
            if self.dialog_cnt % 10 == 0:
                self.log()

        return True

    def log(self):
        """収集したツイート数や経過時間を出力"""
        print(" (dialog: " + str(self.dialog_cnt) + ", turn: " + str(self.turn_cnt) + ") " + ('%.2f' % (self.cum_time + time.time() - self.start_time)) + "[sec]", flush=True)

    def del_username(self, text):
        """
        ツイートデータのテキストからユーザ名を除去
        @param text ツイートデータのテキスト
        @return ユーザ名を除去したツイートデータのテキスト
        """
        text = re.sub("(^|\s)(@|＠)(\w+)", "", text)
        return text

    def check(self, text):
        """
        ツイートデータのテキストに不適切な情報が含まれていないかを判定
        @param text ツイートデータのテキスト
        @return True: 適切、False: 不適切
        """
        # URLを含む（画像も含む？）
        if re.compile("((ftp|http|https):\/\/(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&amp;%@!&#45;\/]))?)").search(text):
            return False
        # ハッシュタグを含む
        if re.compile("(?:^|[^ーー゛゜々ヾヽぁ-ヶ一-龠a-zA-Z0-9&_/>]+)[#＃]([ー゛゜々ヾヽぁ-ヶ一-龠a-zA-Z0-9_]*[ー゛゜々ヾヽぁ-ヶ一-龠a-zA-Z]+[ー゛゜々ヾヽぁ-ヶ一-龠a-zA-Z0-9_]*)").search(text):
            return False
        # 英数字や特定の記号を含む
        if re.compile("[a-zA-Z0-9_]").search(text):
            return False
        # ツイッター用語を含む
        if re.compile("アイコン|あいこん|トプ|とぷ|ヘッダー|へっだー|たぐ|ツイ|ツイート|ついーと|ふぉろ|フォロ|リプ|りぷ|リツ|りつ|いいね|お気に入り|ふぁぼ|ファボ|リム|りむ|ブロック|ぶろっく|スパブロ|すぱぶろ|ブロ解|ぶろ解|鍵|アカ|垢|タグ|ダイレクトメッセージ|空りぷ|空リプ|巻き込|まきこ|マキコ").search(text):
            return False
        # 時期の表現を含む
        if re.compile("明日|あした|明後日|あさって|明々後日|明明後日|しあさって|今日|昨日|おととい|きょう|きのう|一昨日|来週|再来週|今年|先週|先々週|今年|去年|来年|おととい|らいしゅう|さらいしゅう|せんしゅう|せんせんしゅう|きょねん|らいねん|ことし|こんねん").search(text):
            return False
        # 漢数字を含む
        if re.compile("[一二三四五六七八九十百千万億兆〇]").search(text):
            return False
        # 特定のネットスラングを含む
        if re.compile("ナカーマ|イキスギ|いきすぎ|スヤァ|すやぁ|うぇーい|ウェーイ|おなしゃす|アザッス|あざっす|ドヤ|どや|ワカリミ|わかりみ").search(text):
            return False
        return True

    def talker_check(self, talker, idx):
        """
        二話者の交互発話による対話であるかを判定
        @param talker 話者ID
        @param idx キュー内の対話ID
        @return True: 適切、False: 不適切
        """
        if talker != self.queue[idx][-1][0][1] and (len(self.queue[idx]) == 1 or talker == self.queue[idx][-2][0][1]):
            return True
        return False

    def normalize(self, text):
        """
        ツイートデータのテキストに正規化などのフィルタ処理を行う
        @param text ツイートデータのテキスト
        @return フィルタ処理を施したツイートデータのテキスト
        """
        text = re.sub("\([^笑泣嬉悲驚汗爆渋苦困死楽怒哀呆殴涙藁]+?\)", " ", text)
        text = re.sub("[^ぁ-んァ-ヶｧ-ｳﾞ一-龠々ー～〜、。！？!?,，.．\r\n]", " ", text)

        text = re.sub("[,，]", "、", text)
        text = re.sub("[．.]", "。", text)
        text = re.sub("〜", "～", text)
        text = re.sub("、(\s*、)+|。(\s*。)+", "...", text)

        text = re.sub("!+", "！", text)
        text = re.sub("！(\s*！)+", "！", text)
        text = re.sub("\?+", "？", text)
        text = re.sub("？(\s*？)+", "？", text)

        text = re.sub("～(\s*～)+", "～", text)
        text = re.sub("ー(\s*ー)+", "ー", text)

        text = re.sub("\r\n|\n|\r", "。", text)

        text += "。"
        text = re.sub("[、。](\s*[、。])+", "。", text)

        text = re.sub("[。、！](\s*[。、！])+", "！", text)
        text = re.sub("[。、？](\s*[。、？])+", "？", text)
        text = re.sub("((！\s*)+？|(？\s*)+！)(\s*[！？])*", "!?", text)

        for w in ["っ", "笑", "泣", "嬉", "悲", "驚", "汗", "爆", "渋", "苦", "困", "死", "楽", "怒", "哀", "呆", "殴", "涙", "藁"]:
            text = re.sub(w + "(\s*" + w + ")+", " " + w + " ", text)

        text = re.sub("、\s*([笑泣嬉悲驚汗爆渋苦困死楽怒哀呆殴涙藁])\s*。", " \\1。", text)
        text = re.sub("(。|！|？|!\?)\s*([笑泣嬉悲驚汗爆渋苦困死楽怒哀呆殴涙藁])\s*。", " \\2\\1", text)

        text = re.sub("、", " 、 ", text)
        text = re.sub("。", " 。\n", text)

        text = re.sub("(\.\s*)+", " ... ", text)
        text = re.sub("！", " ！\n", text)
        text = re.sub("？", " ？\n", text)
        text = re.sub("!\?", " !?\n", text)

        text = re.sub("\n(\s*[～ー])+", "\n", text)

        text = re.sub("^([\s\n]*[。、！？!?ー～]+)+", "", text)
        text = re.sub("(.+?)\\1{3,}", "\\1\\1\\1", text)

        return text

    def del_morpheme(self, text):
        """
        ツイートデータのテキストから特定の形態素を除去する
        @param text ツイートデータのテキスト
        @return 特定の形態素を除去したツイートデータのテキスト
        """
        lines = text.strip().split("\n")
        result, standard, part = "", "", ""

        for line in lines:
            add_result, add_standard, add_part = "", "", ""
            node = tagger.parseToNode(line)

            while node:
                feature = node.feature.split(',')
                if feature[0] == "BOS/EOS":
                    node = node.next
                    continue

                if node.surface in ["ノ", "ーノ", "ロ", "艸", "屮", "罒", "灬", "彡", "ヮ", "益",\
                "皿", "タヒ", "厂", "厂厂", "啞", "卍", "ノノ", "ノノノ", "ノシ", "ノツ",\
                "癶", "癶癶", "乁", "乁厂", "マ", "んご", "んゴ", "ンゴ", "にき", "ニキ", "ナカ", "み", "ミ"]:
                    node = node.next
                    continue

                if node.surface in ["つ", "っ"] and add_result == "":
                    node = node.next
                    continue

                if feature[0] == "名詞":
                    token = self.noun_token
                elif feature[0] == "動詞":
                    token = self.verb_token
                elif feature[0] == "形容詞":
                    token = self.adjective_token
                elif feature[0] == "副詞":
                    token = self.adverb_token
                elif feature[0] == "助詞":
                    token = self.particle_token
                elif feature[0] == "助動詞":
                    token = self.auxiliary_verb_token
                elif feature[0] == "接続詞":
                    token = self.conjunction_token
                elif feature[0] == "接頭詞":
                    token = self.prefix_token
                elif feature[0] == "フィラー":
                    token = self.filler_token
                elif feature[0] == "感動詞":
                    token = self.impression_verb_token
                elif node.surface == "...":
                    token = self.three_dots_token
                elif node.surface in ["。", "！", "？", "!?"]:
                    token = self.phrase_point_token
                elif node.surface == "、":
                    token = self.reading_point_token
                else:
                    token = self.other_token

                add_result += node.surface + " "
                if re.compile("[^ぁ-んァ-ヶｧ-ｳﾞ一-龠々ー～]").search(feature[6]):
                    add_standard += node.surface + " "
                elif token in [self.three_dots_token, self.phrase_point_token, self.reading_point_token]:
                    add_standard += node.surface + " "
                elif feature[6] == "*":
                    add_standard += self.unk_standard + " "
                else:
                    add_standard += feature[6] + " "
                add_part += token + " "

                node = node.next

            if add_result.strip() not in ["、", "。", "！", "？", "!?", "", "... 。", "... ！", "... ？", "... !?",\
            "人 。", "つ 。", "っ 。", "笑 。", "笑 ！", "笑 ？", "笑 !?"]:
                result += add_result
                standard += add_standard
                part += add_part

        return result.strip(), standard.strip(), part.strip()

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
            listener.log()
            listener.save_tmp()
            stream.disconnect()
            break
        except:
            global tcpip_delay
            time.sleep(min(tcpip_delay, MAX_TCPIP_TIMEOUT))
            tcpip_delay += 0.25


if __name__ == '__main__':
    # 設定ファイルを読み込む
    config = yaml.load(stream=open("config/config.yml", 'rt', encoding='utf-8'), Loader=yaml.SafeLoader)
    # Twitter API の情報を読み込む
    api_config = yaml.load(stream=open("config/api.yml", 'rt'), Loader=yaml.SafeLoader)

    # ツイート収集開始
    get_twitter_corpus(config, api_config)
