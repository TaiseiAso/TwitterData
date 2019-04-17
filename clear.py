##################################################
# ファイル群を初期化する
# 実装開始日: 2019/4/11
# 実装完了日: 2019/4/11
# 実行方法: $ python clear.py
# 備考: 収集した対話データも削除するので注意
##################################################

import os

for top in ["data", "tmp"]:
    for root, _, files in os.walk(top, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
