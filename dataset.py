import torch
import numpy as np
import torch.nn as nn
from torch.utils.data import Dataset
from transformers import BertTokenizer
from tqdm import tqdm

class CNewsDataset(Dataset):
    def __init__(self, filename):
        # self.labels = ['体育', '娱乐', '家居', '房产', '教育', '时尚', '时政', '游戏', '科技', '财经']
        self.labels = [-3, -2, -1, 0, 1, 2]
        self.tokenizer = BertTokenizer.from_pretrained('bert-base-chinese')
        self.input_ids = []
        self.token_type_ids = []
        self.attention_mask = []
        self.label_id = []
        self.load_data(filename)
        
    def load_data(self, filename):
        print('loading data from:', filename)
        with open(filename, 'r', encoding='utf-8') as wf:
            lines = wf.readlines()
        for line in tqdm(lines):
            lineList = line.strip().split(',')
            lineSize = len(lineList)
            text = ""
            rate = 0
            if lineSize > 12:
                textList = lineList[3:-8]
                text = ",".join(textList)
                if lineList[-2]:
                    rate = int(float(lineList[-2]))
            else:
                _, _, _, text, _, _, _, _, _, tp, rate1, _ = line.strip().split(',')
                if rate1:
                    rate = int(float(rate1))
            label_id = self.labels.index(rate)
            token = self.tokenizer(text, add_special_tokens=True, padding='max_length', truncation=True, max_length=256)
            self.input_ids.append(np.array(token['input_ids']))
            self.token_type_ids.append(np.array(token['token_type_ids']))
            self.attention_mask.append(np.array(token['attention_mask']))
            self.label_id.append(label_id)

    def __getitem__(self, index):
        return self.input_ids[index], self.token_type_ids[index], self.attention_mask[index], self.label_id[index]

    def __len__(self):
        return len(self.input_ids)
