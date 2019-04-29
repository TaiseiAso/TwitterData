# TwitterData
***
## 概要
日本語話者による複数ターン対話をTwitterからリアルタイムに収集するツール。

## 要件
- python3
- pyyaml
- tweepy
- MeCab
- Twitter API key を取得
    - consumer_key
    - consumer_secret
    - access_token
    - access_token_secret

## 使い方
1. Twitter API key を取得し、"config/" に以下のようなファイル "api.yml" を作成する。
    ```
    twitter_API:
      consumer_key        : xxxxx
      consumer_secret     : xxxxx
      access_token        : xxxxx
      access_token_secret : xxxxx
    ```

2. ツイートの収集を開始すると、 "data/" に対話データが蓄積されていく。
    ```
    $ python twitter.py
    ```
    様々な正規化処理を施し、形態素解析をした対話データが保存される。ツイートとそれに対するリプライツイートが二つのファイルに分かれて蓄積保存される他、複数ターン対話がまとめて一つのファイルに蓄積保存される。
    各データを標準形/表層形に変換したファイルと、品詞分類したファイルを保存することもできる。
    保存されるファイル名は、 "config/config.yml" に保存した名前になる。

3. Ctrl+C で安全に終了でき、一時保存ファイル "tmp.txt" が "tmp/" に自動的に保存される。
これによりツイートの収集を再開しても、重複がなくなり、データ数や経過時間のカウントも引き継がれる。

4. 収集した対話データをフィルタリングした対話データが、 "filtered/" に保存される。
    ```
    $ pyton filter.py
    ```
    フィルタリングの内容は、 "config/config.yml" を編集することで変更できる。
    フィルタリング後に以下のように表示される。
    ```
    (ファイル名): (フィルタリング前のデータ数) -> (フィルタリング後のデータ数)
    ```

5. 一時保存ファイルを含めた、収集したすべての対話データを削除する。
    ```
    $ python clear.py
    ```

## 設定
"config/config.yml" の各値の説明は以下の通り。

- filename: 保存される各ファイルの名前 (文字列)
    - input_file: ツイート
    - target_file: input_fileのツイートに対するリプライツイート
    - dialog_file: 複数ターン対話
    - standard_file: 標準形/表層形に変換したファイルに付与する文字列
    - part_file: 品詞分類したファイルに付与する文字列
- dump: 各ファイルを保存するかどうかのフラグ (on/off)
    - turn_file: input_file と target_file
    - dialog_file: dialog_file
    - standard_file: 標準形/表層形に変換したファイル
    - part_file: 品詞分類したファイル
- filter: フィルタリングの内容
    - length: 長さに関する制限 (整数)
        - len_min: 全体の長さの最小
        - len_max: 全体の長さの最大
        - sent_len_min: 文単位の長さの最小
        - sent_len_max: 文単位の長さの最大
        - sent_min: 含まれる文の数の最小
        - sent_max: 含まれる文の数の最大
        - turn_min: 取得する複数ターン対話の最小ターン数
        - turn_max: 取得する複数ターン対話の最大ターン数
        - len_diff: ツイートとそれに対するリプライツイートの長さの差の最大
    - dump: 保存する品詞 (part_fileが存在する場合のみ適用) (on/off)
        - noun: 名詞
        - verb: 動詞
        - adjective: 形容詞
        - adverb: 副詞
        - particle: 助詞
        - auxiliary_verb: 助動詞
        - conjunction: 接続詞
        - prefix: 接頭詞
        - filler: フィラー
        - impression_verb: 感動詞
        - three_dots: ドット
        - phrase_point: 句点
        - reading_point: 読点
        - other: その他
    - exist: 存在しなければならない品詞 (part_fileが存在する場合のみ適用) (on/off)
        - noun: 名詞
        - verb: 動詞
        - adjective: 形容詞
        - adverb: 副詞
        - particle: 助詞
        - auxiliary_verb: 助動詞
        - conjunction: 接続詞
        - prefix: 接頭詞
        - filler: フィラー
        - impression_verb: 感動詞
        - three_dots: ドット
        - phrase_point: 句点
        - reading_point: 読点
- print_progress: 進捗バーを出力する (on/off)
- unknown_standard: 標準形/表層形が存在しない場合に割り当てる文字列 (文字列)
- part: part_fileの各品詞に割り当てる文字列 (文字列)
    - noun: 名詞
    - verb: 動詞
    - adjective: 形容詞
    - adverb: 副詞
    - particle: 助詞
    - auxiliary_verb: 助動詞
    - conjunction: 接続詞
    - prefix: 接頭詞
    - filler: フィラー
    - impression_verb: 感動詞
    - three_dots: ドット
    - phrase_point: 句点
    - reading_point: 読点
    - other: その他

ただし「ターン数」とは、例えば A->B という二人の話者による対話を「1ターン」とし、
A->B->A は「2ターン」とカウントする。
また、複数ターン対話における len_diff は最短の文と最長の文の長さの差の最大となる。
