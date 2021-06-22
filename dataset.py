import torch
import numpy as np
import torch.nn as nn
from torch.utils.data import Dataset
from transformers import BertTokenizer
from tqdm import tqdm

class CNewsDataset(Dataset):
    def __init__(self, filename):
        # self.labels = ['体育', '娱乐', '家居', '房产', '教育', '时尚', '时政', '游戏', '科技', '财经']
        self.labels = [-2, -1, 0, 1, 2]
        self.tokenizer = BertTokenizer.from_pretrained('bert-base-chinese')
        self.input_ids = []
        self.token_type_ids = []
        self.attention_mask = []
        self.label_id = []
        self.load_data1(filename)
        
    def load_data(self, filename):
        print('loading data from:', filename)
        with open(filename, 'r', encoding='utf-8') as wf:
            lines = wf.readlines()
        for line in tqdm(lines):
            print(line)
            rate = 0
            # lineList = line.strip().split(',')
            # lineSize = len(lineList)
            # text = ""
            # if lineSize > 12:
            #     textList = lineList[3:-8]
            #     text = ",".join(textList)
            #     if lineList[-2]:
            #         rate = int(float(lineList[-2]))
            # else:
            #     _, _, _, text, _, _, _, _, _, tp, rate1, _ = line.strip().split(',')
            #     if rate1:
            #         rate = int(float(rate1))
            _, _, _, text, _, _, _, _, _, tp, rate1, _ = line.strip().split(',')
            if rate1:
                rate = int(float(rate1))
            label_id = self.labels.index(rate)
            token = self.tokenizer(text, add_special_tokens=True, padding='max_length', truncation=True, max_length=256)
            self.input_ids.append(np.array(token['input_ids']))
            self.token_type_ids.append(np.array(token['token_type_ids']))
            self.attention_mask.append(np.array(token['attention_mask']))
            self.label_id.append(label_id)


    def load_data1(self, filename):
        print('loading data from:', filename)
        with open(filename, 'r', encoding='utf-8') as wf:
            lines = wf.readlines()
        for line in tqdm(lines):
            rate = 0
            _, _, title, text, up, down, _, _ = line.strip().split(',')
            upanddown = int(up) + int(down)
            if upanddown > 0:
                upRate = float(up) / upanddown
                if upRate > 0.9:
                    rate = 2
                elif upRate > 0.6:
                    rate = 1
                elif upRate > 0.45:
                    rate = 0
                elif upRate > 0.15:
                    rate = -1
                else:
                    rate = -2
            label_id = self.labels.index(rate)
            token = self.tokenizer(title, add_special_tokens=True, padding='max_length', truncation=True, max_length=256)
            self.input_ids.append(np.array(token['input_ids']))
            self.token_type_ids.append(np.array(token['token_type_ids']))
            self.attention_mask.append(np.array(token['attention_mask']))
            self.label_id.append(label_id)

    def __getitem__(self, index):
        return self.input_ids[index], self.token_type_ids[index], self.attention_mask[index], self.label_id[index]

    def __len__(self):
        return len(self.input_ids)

