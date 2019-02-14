#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : pureoym
# @Contact : pureoym@163.com
# @TIME    : 2019/2/14 9:23
# @File    : gov_report.py
# Copyright 2017 pureoym. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ========================================================================
import jieba
import jieba.analyse
import pandas as pd


def find_top_words(input_path, stopwords_path, top_k):
    """
    从文件中获取高频词
    :param input_path: 输入文件的地址
    :param stopwords_path: 停用词列表，停用词不出现在高频词列表中
    :param top_k: 取前K个词
    :return: 高频词列表
    """
    doc = open(input_path, 'r', encoding='utf16').read()
    jieba.analyse.set_stop_words(stopwords_path)
    words = jieba.analyse.extract_tags(doc, topK=top_k)
    items = []
    for word in words:
        items.append((word, doc.count(word)))
    items.sort(key=lambda x: x[1], reverse=True)
    print(items)
    return items


def save_result(items, output_path):
    """
    保存结果
    :param items:
    :param output_path:
    :return:
    """
    df = pd.DataFrame(items, columns=['word', 'count'], index=None)
    df.to_csv(output_path)
    print('结果保存至%s' % output_path)


if __name__ == '__main__':
    stopwords_path = '/application/search/gov_report/stop_words.txt'
    years = ['2014', '2015', '2016', '2017', '2018']
    top_k = 120
    for year in years:
        input_path = '/application/search/gov_report/%s.txt' % year
        output_path = '/application/search/gov_report/output_%s.csv' % year
        items = find_top_words(input_path, stopwords_path, top_k)
        save_result(items, output_path)
