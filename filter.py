# coding: utf-8

"""収集した対話データを長さでフィルタリング"""
__author__ = "Aso Taisei"
__version__ = "1.0.0"
__date__ = "24 Apr 2019"


# 必要なモジュールをインポート
import os
import yaml
import re


# データ内の不必要な部分にマッチングするパターン
rm = re.compile("^>")


def check(line, fi):
    """
    テキストをフィルタ処理して適切かどうかを判定する
    @param line テキスト
    @param fi 設定ファイルのフィルタ処理内容の情報
    @return True: 適切、False: 不適切
    """
    line_ = line.strip()

    sents = line_.split()
    if len(sents) < fi['len_min'] or fi['len_max'] < len(sents):
        return False

    sents = [sent for sent in re.split("。|！|？|!\?", line_) if sent != ""]

    if len(sents) < fi['sent_min'] or fi['sent_max'] < len(sents):
        return False

    for sent in sents:
        words = sent.split()
        if len(words) + 1 < fi['sent_len_min'] or fi['sent_len_max'] < len(words) + 1:
            return False

    return True


def diff_check(inp, tar, fi):
    """
    入力文と出力文の長さの差が一定以下であるかどうかを判定する
    @param inp 入力文
    @param tar 出力文
    @param fi 設定ファイルのフィルタ処理内容の情報
    @return True: 適切、False: 不適切
    """
    if abs(len(inp.split()) - len(tar.split())) <= fi['len_diff']:
        return True
    return False


def filtering(config):
    """
    保存してあるすべてのコーパスにフィルタ処理を施す
    @param config 設定ファイル情報
    """
    fn = config['filename']
    fi = config['filter']

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

    # 入力ファイルと出力ファイルのペアをフィルタリング
    if os.path.isfile("data/" + fn['input_file'] + ".txt") and os.path.isfile("data/" + fn['target_file'] + ".txt"):
        filtered = True
        cnt, cnt_ = 0, 0

        with open("data/" + fn['input_file'] + ".txt", 'r', encoding='utf-8') as f_in,\
        open("data/" + fn['target_file'] + ".txt", 'r', encoding='utf-8') as f_tar,\
        open("filtered/" + fn['input_file'] + ".txt", 'w', encoding='utf-8') as f_in_filtered,\
        open("filtered/" + fn['target_file'] + ".txt", 'w', encoding='utf-8') as f_tar_filtered:

            line_in = f_in.readline()
            line_tar = f_tar.readline()

            while line_in and line_tar:
                cnt += 1
                if check(line_in, fi) and check(line_tar, fi) and diff_check(line_in, line_tar, fi):
                    cnt_ += 1
                    f_in_filtered.write(line_in)
                    f_tar_filtered.write(line_tar)

                line_in = f_in.readline()
                line_tar = f_tar.readline()

        print(fn['input_file'] + "/" + fn['target_file'] + ": " + str(cnt) + " -> " + str(cnt_))

    # 入力か出力の一方のファイルしかない場合は単体でフィルタリング
    else:
        file = None
        if os.path.isfile("data/" + fn['input_file'] + ".txt"):
            file = fn['input_file']
        elif os.path.isfile("data/" + fn['target_file'] + ".txt"):
            file = fn['target_file']

        if file:
            filtered = True
            cnt, cnt_ = 0, 0

            with open("data/" + file + ".txt", 'r', encoding='utf-8') as f,\
            open("filtered/" + file + ".txt", 'w', encoding='utf-8') as f_filtered:

                line = f.readline()

                while line:
                    cnt += 1
                    if check(line, fi):
                        cnt_ += 1
                        f_filtered.write(line)

                    line = f.readline()

            print(file + ": " + str(cnt) + " -> " + str(cnt_))

    # 複数ターン対話をフィルタリング
    if os.path.isfile("data/" + fn['dialog_file'] + ".txt"):
        filtered = True
        cnt, cnt_ = 0, 0

        with open("data/" + fn['dialog_file'] + ".txt", 'r', encoding='utf-8') as f,\
        open("filtered/" + fn['dialog_file'] + ".txt", 'w', encoding='utf-8') as f_filtered:

            min, max = 0, 0
            line = f.readline()
            queue = []
            buf = []

            while line:
                dump = False

                if rm.search(line):
                    cnt += 1
                    dump = True
                    min = -1
                    buf = []
                elif check(line, fi):
                    length = len(line.split())
                    if min == -1:
                        min = max = length
                        queue.append(line)
                    else:
                        if min > length:
                            min = length
                        elif max < length:
                            max = length
                        if max - min <= fi['len_diff']:
                            queue.append(line)
                        else:
                            dump = True
                            min = max = length
                            buf = [line]
                else:
                    dump = True
                    min = -1
                    buf = []

                line = f.readline()
                if not line:
                    dump = True

                if dump:
                    if fi['turn_min'] < len(queue) and len(queue) - 1 <= fi['turn_max']:
                        cnt_ += 1
                        f_filtered.write("> " + str(cnt_) + " --------------------\n")
                        for tweet in queue:
                            f_filtered.write(tweet)
                    queue = buf

        print(fn['dialog_file'] + ": " + str(cnt) + " -> " + str(cnt_))

    if not filtered:
        print("no filtered file")


if __name__ == '__main__':
    # 設定ファイルを読み込む
    config = yaml.load(stream=open("config/config.yml", 'rt'), Loader=yaml.SafeLoader)

    # フィルタ処理開始
    filtering(config)
