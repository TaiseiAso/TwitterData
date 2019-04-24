# coding: utf-8

"""収集した対話データやチェックポイントを削除"""
__author__ = "Aso Taisei"
__version__ = "1.0.1"
__date__ = "23 Apr 2019"


# 必要なモジュールをインポート
import os


# dataフォルダとtmpフォルダとfilteredフォルダの中のファイルをすべて削除
for top in ["data", "tmp", "filtered"]:
    for root, _, files in os.walk(top, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
