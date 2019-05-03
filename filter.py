# coding: utf-8

"""収集した対話データをフィルタリング"""
__author__ = "Aso Taisei"
__version__ = "1.0.1"
__date__ = "29 Apr 2019"


# 必要なモジュールをインポート
import os
import yaml
import re
import MeCab


class TweetFilter():
    """収集したツイートをフィルタリングするためのクラス"""
    def __init__(self, config):
        """
        コンストラクタ
        @param config 設定ファイルの情報
        """
        # 保存ファイル名の情報取得
        fn = config['filename']
        inp_fn = fn['input_file']
        tar_fn = fn['target_file']
        dig_fn = fn['dialog_file']
        std_fn = fn['standard_file']
        prt_fn = fn['part_file']

        # 保存ファイルパス
        self.dig_fp = dig_fn + ".txt"
        self.dig_std_fp = dig_fn + "_" + std_fn + ".txt"
        self.dig_prt_fp = dig_fn + "_" + prt_fn + ".txt"
        self.inp_fp = inp_fn + ".txt"
        self.tar_fp = tar_fn + ".txt"
        self.inp_std_fp = inp_fn + "_" + std_fn + ".txt"
        self.tar_std_fp = tar_fn + "_" + std_fn + ".txt"
        self.inp_prt_fp = inp_fn + "_" + prt_fn + ".txt"
        self.tar_prt_fp = tar_fn + "_" + prt_fn + ".txt"

        # フィルタリング内容を取得
        fi = config['filter']
        fi_len = fi['length']
        self.len_min = fi_len['len_min']
        self.len_max = fi_len['len_max']
        self.sent_len_min = fi_len['sent_len_min']
        self.sent_len_max = fi_len['sent_len_max']
        self.sent_min = fi_len['sent_min']
        self.sent_max = fi_len['sent_max']
        self.turn_min = fi_len['turn_min']
        self.turn_max = fi_len['turn_max']
        self.len_diff = fi_len['len_diff']

        fi_dp = fi['dump']
        self.dump_list = [
            fi_dp['noun'], fi_dp['verb'], fi_dp['adjective'],
            fi_dp['adverb'], fi_dp['particle'], fi_dp['auxiliary_verb'],
            fi_dp['conjunction'], fi_dp['prefix'], fi_dp['filler'],
            fi_dp['impression_verb'], fi_dp['three_dots'], fi_dp['phrase_point'],
            fi_dp['reading_point'], fi_dp['other']
        ]

        fi_ex = fi['exist']
        self.exist_list = [
            fi_ex['noun'], fi_ex['verb'], fi_ex['adjective'],
            fi_ex['adverb'], fi_ex['particle'], fi_ex['auxiliary_verb'],
            fi_ex['conjunction'], fi_ex['prefix'], fi_ex['filler'],
            fi_ex['impression_verb'], fi_ex['three_dots'], fi_ex['phrase_point'],
            fi_ex['reading_point']
        ]

        # 品詞のトークンを取得
        pt = config['part']
        self.token_list = [
            pt['noun'], pt['verb'], pt['adjective'],
            pt['adverb'], pt['particle'], pt['auxiliary_verb'],
            pt['conjunction'], pt['prefix'], pt['filler'],
            pt['impression_verb'], pt['three_dots'], pt['phrase_point'],
            pt['reading_point'], pt['other']
        ]

    def text_check(self, text):
        """
        テキストをフィルタリングする
        @param text 空白で分かち書きされたテキスト
        @return True: 適切、False: 不適切
        """
        text = text.strip()
        words = text.split()
        if len(words) < self.len_min or self.len_max < len(words):
            return False

        sents = [sent for sent in re.split("。|！|？|!\?", text)]
        if len(sents) < self.sent_min or self.sent_max < len(sents):
            return False

        for i, sent in enumerate(sents):
            if i == len(sents) - 1 and sent == "":
                continue
            words = sent.split()
            if len(words) + 1 < self.sent_len_min or self.sent_len_max < len(words) + 1:
                return False

        return True

    def part_check(self, part):
        """
        品詞列をフィルタリングする
        @param part 品詞列
        @return True: 適切、False: 不適切
        """
        parts = part.strip().split()
        for exist, token in zip(self.exist_list, self.token_list):
            if exist and token not in parts:
                return False
        return True

    def diff_check(self, inp, tar):
        """
        入力文と目標文の長さの差が一定以下であるかどうかを判定する
        @param inp 入力文
        @param tar 目標文
        @return True: 適切、False: 不適切
        """
        if abs(len(inp.split()) - len(tar.split())) <= self.len_diff:
            return True
        return False

    def del_part(self, text, standard, part):
        """
        指定した品詞のみを除去する
        @param text 空白で分かち書きされたテキスト
        @param standard 標準形/表層形のテキスト
        @param part 品詞列
        @return 品詞除去されたテキスト
        @return 品詞除去された標準形/表層形のテキスト
        @return 品詞除去された品詞列
        """
        result_text, result_standard, result_part = "", "", ""
        words, standards, parts = text.strip().split(), standard.strip().split() if standard else None, part.strip().split()

        if standards:
            for word, standard, part in zip(words, standards, parts):
                if part in self.token_list:
                    part_idx = self.token_list.index(part)
                    if self.dump_list[part_idx]:
                        result_text += word + " "
                        result_standard += standard + " "
                        result_part += part + " "
        else:
            for word, part in zip(words, parts):
                if part in self.token_list:
                    part_idx = self.token_list.index(part)
                    if self.dump_list[part_idx]:
                        result_text += word + " "
                        result_part += part + " "

        return result_text.strip() + "\n", result_standard.strip() + "\n" if standards else None, result_part.strip() + "\n"

    def turn_filtering(self):
        """入出力コーパスをフィルタリングする"""
        cnt, cnt_ = 0, 0

        # ファイルを開く
        f_inp = open("data/" + self.inp_fp, 'r', encoding='utf-8')
        f_tar = open("data/" + self.tar_fp, 'r', encoding='utf-8')
        f_inp_fi = open("filtered/" + self.inp_fp, 'w', encoding='utf-8')
        f_tar_fi = open("filtered/" + self.tar_fp, 'w', encoding='utf-8')

        if os.path.isfile("data/" + self.inp_std_fp) and os.path.isfile("data/" + self.tar_std_fp):
            std = True
            f_inp_std = open("data/" + self.inp_std_fp, 'r', encoding='utf-8')
            f_tar_std = open("data/" + self.tar_std_fp, 'r', encoding='utf-8')
            f_inp_std_fi = open("filtered/" + self.inp_std_fp, 'w', encoding='utf-8')
            f_tar_std_fi = open("filtered/" + self.tar_std_fp, 'w', encoding='utf-8')
        else:
            std = False

        if os.path.isfile("data/" + self.inp_prt_fp) and os.path.isfile("data/" + self.tar_prt_fp):
            prt = True
            f_inp_prt = open("data/" + self.inp_prt_fp, 'r', encoding='utf-8')
            f_tar_prt = open("data/" + self.tar_prt_fp, 'r', encoding='utf-8')
            f_inp_prt_fi = open("filtered/" + self.inp_prt_fp, 'w', encoding='utf-8')
            f_tar_prt_fi = open("filtered/" + self.tar_prt_fp, 'w', encoding='utf-8')
        else:
            prt = False

        # ファイルから読み込む
        line_inp = f_inp.readline()
        line_tar = f_tar.readline()
        line_inp_std = f_inp_std.readline() if std else None
        line_tar_std = f_tar_std.readline() if std else None
        line_inp_prt = f_inp_prt.readline() if prt else None
        line_tar_prt = f_tar_prt.readline() if prt else None

        while line_inp:
            cnt += 1

            # 指定した品詞を除外
            if prt:
                line_inp, line_inp_std, line_inp_prt = self.del_part(line_inp, line_inp_std if std else None, line_inp_prt)
                line_tar, line_tar_std, line_tar_prt = self.del_part(line_tar, line_tar_std if std else None, line_tar_prt)

            # 適切なデータであるか判定
            if self.text_check(line_inp) and self.text_check(line_tar) and self.diff_check(line_inp, line_tar) and\
            (not prt or (self.part_check(line_inp_prt) and self.part_check(line_tar_prt))):
                cnt_ += 1

                # ファイルに書き込む
                f_inp_fi.write(line_inp)
                f_tar_fi.write(line_tar)
                if std:
                    f_inp_std_fi.write(line_inp_std)
                    f_tar_std_fi.write(line_tar_std)
                if prt:
                    f_inp_prt_fi.write(line_inp_prt)
                    f_tar_prt_fi.write(line_tar_prt)

            # ファイルから読み込む
            line_inp = f_inp.readline()
            line_tar = f_tar.readline()
            line_inp_std = f_inp_std.readline() if std else None
            line_tar_std = f_tar_std.readline() if std else None
            line_inp_prt = f_inp_prt.readline() if prt else None
            line_tar_prt = f_tar_prt.readline() if prt else None

        # ファイルを閉じる
        f_inp.close()
        f_tar.close()
        f_inp_fi.close()
        f_tar_fi.close()
        if std:
            f_inp_std.close()
            f_tar_std.close()
            f_inp_std_fi.close()
            f_tar_std_fi.close()
        if prt:
            f_inp_prt.close()
            f_tar_prt.close()
            f_inp_prt_fi.close()
            f_tar_prt_fi.close()

        print(self.inp_fp + "/" + self.tar_fp + ": " + str(cnt) + " -> " + str(cnt_))

    def dialog_filtering(self):
        """対話コーパスをフィルタリングする"""
        cnt, cnt_ = 0, 0

        # ファイルを開く
        f_dig = open("data/" + self.dig_fp, 'r', encoding='utf-8')
        f_dig_fi = open("filtered/" + self.dig_fp, 'w', encoding='utf-8')

        if os.path.isfile("data/" + self.dig_std_fp):
            std = True
            f_dig_std = open("data/" + self.dig_std_fp, 'r', encoding='utf-8')
            f_dig_std_fi = open("filtered/" + self.dig_std_fp, 'w', encoding='utf-8')
        else:
            std = False

        if os.path.isfile("data/" + self.dig_prt_fp):
            prt = True
            f_dig_prt = open("data/" + self.dig_prt_fp, 'r', encoding='utf-8')
            f_dig_prt_fi = open("filtered/" + self.dig_prt_fp, 'w', encoding='utf-8')
        else:
            prt = False

        # ファイルから読み込む
        line_dig = f_dig.readline()
        line_dig_std = f_dig_std.readline() if std else None
        line_dig_prt = f_dig_prt.readline() if prt else None

        min, max, queue, buf = 0, 0, [], []

        while line_dig:
            dump = False

            if line_dig == "\n":
                cnt += 1
                dump, min, buf = True, -1, []
            else:
                # 指定した品詞を除外
                if prt:
                    line_dig, line_dig_std, line_dig_prt = self.del_part(line_dig, line_dig_std if std else None, line_dig_prt)

                # 適切なデータであるか判定
                if self.text_check(line_dig) and (not prt or self.part_check(line_dig_prt)):
                    length = len(line_dig.split())
                    if min == -1:
                        min = max = length
                        queue.append([line_dig, line_dig_std, line_dig_prt])
                    else:
                        if min > length:
                            min = length
                        elif max < length:
                            max = length

                        if max - min <= self.len_diff:
                            queue.append([line_dig, line_dig_std, line_dig_prt])
                        else:
                            dump, min, max, buf = True, length, length, [[line_dig, line_dig_std, line_dig_prt]]
                else:
                    dump, min, buf = True, -1, []

            # ファイルから読み込む
            line_dig = f_dig.readline()
            line_dig_std = f_dig_std.readline() if std else None
            line_dig_prt = f_dig_prt.readline() if prt else None

            if not line_dig:
                dump = True

            # ファイルに書き込む
            if dump:
                len_queue = len(queue)
                if 1 < len_queue and self.turn_min < len_queue and len_queue - 1 <= self.turn_max:
                    cnt_ += 1

                    for tweet in queue:
                        f_dig_fi.write(tweet[0])
                        if std:
                            f_dig_std_fi.write(tweet[1])
                        if prt:
                            f_dig_prt_fi.write(tweet[2])

                    f_dig_fi.write("\n")
                    if std:
                        f_dig_std_fi.write("\n")
                    if prt:
                        f_dig_prt_fi.write("\n")
                queue = buf

        # ファイルを閉じる
        f_dig.close()
        f_dig_fi.close()
        if std:
            f_dig_std.close()
            f_dig_std_fi.close()
        if prt:
            f_dig_prt.close()
            f_dig_prt_fi.close()

        print(self.dig_fp + ": " + str(cnt) + " -> " + str(cnt_))

    def filtering(self):
        """保存してあるすべてのコーパスをフィルタリングする"""
        # dataフォルダがなければ終了
        if not os.path.isdir("data"):
            print("no data folder")
            return

        # filteredフォルダがなければ作成する
        if not os.path.isdir("filtered"):
            os.mkdir("filtered")

        # filteredフォルダ内のファイルをすべて削除
        for root, _, files in os.walk("filtered", topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))

        filtered = False

        # 入出力コーパスをフィルタリングする
        if os.path.isfile("data/" + self.inp_fp) and os.path.isfile("data/" + self.tar_fp):
            filtered = True
            self.turn_filtering()

        # 対話コーパスをフィルタリングする
        if os.path.isfile("data/" + self.dig_fp):
            filtered = True
            self.dialog_filtering()

        if not filtered:
            print("no filtered file")


def filtering_twitter_corpus(config):
    """
    収集した対話データをフィルタリング
    @param config 設定ファイルの情報
    """
    f = TweetFilter(config)
    f.filtering()


if __name__ == '__main__':
    # 設定ファイルを読み込む
    config = yaml.load(stream=open("config/config.yml", 'rt', encoding='utf-8'), Loader=yaml.SafeLoader)

    # フィルタ処理開始
    filtering_twitter_corpus(config)
