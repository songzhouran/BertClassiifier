import torch
from model import BertClassifier
from transformers import BertTokenizer, BertConfig
from tqdm import tqdm
from scraper import SpiderBitcoin86


if __name__ == '__main__':
    # newEventFile = '/Users/a/project/twitterscraper/csv/bitcoin86new.csv'
    # predictFile = '/Users/a/project/twitterscraper/csv/bitcoin86pred.csv'
    # modelFile = '/Users/a/Documents/best_model.pkl'
    newEventFile = '/home/songzhouran/py/bert/BertClassifier/data/bitcoin86new.csv'
    predictFile = '/home/songzhouran/py/bert/BertClassifier/data/bitcoin86pred.csv'
    modelFile = '/home/songzhouran/py/bert/BertClassifier/models/best_model.pkl'
    labels = [-2, -1, 0, 1, 2]
    bert_config = BertConfig.from_pretrained('bert-base-chinese')
    model = BertClassifier(bert_config, len(labels))
    model.load_state_dict(torch.load(modelFile, map_location=torch.device('cpu')))
    tokenizer = BertTokenizer.from_pretrained('bert-base-chinese')
    with open(predictFile, 'a', encoding='utf-8') as predW:
        with open(newEventFile, 'r', encoding='utf-8') as wf:
            lines = wf.readlines()
        for line in tqdm(lines):
            lineList = line.strip().split(',')
            lineSize = len(lineList)
            text = ""
            rate = 0
            if lineSize > 8:
                textList = lineList[3:-5]
                text = ",".join(textList)
            else:
                _, _, _, text, _, _, _, _ = lineList
            tp = SpiderBitcoin86().getTradePair(text)
            if len(tp) > 0:
                token = tokenizer(text, add_special_tokens=True, padding='max_length', truncation=True, max_length=256)
                input_ids = token['input_ids']
                attention_mask = token['attention_mask']
                token_type_ids = token['token_type_ids']

                input_ids = torch.tensor([input_ids], dtype=torch.long)
                attention_mask = torch.tensor([attention_mask], dtype=torch.long)
                token_type_ids = torch.tensor([token_type_ids], dtype=torch.long)

                predicted = model(
                    input_ids,
                    attention_mask,
                    token_type_ids,
                )
                pred_label = torch.argmax(predicted, dim=1)
                newLine = f'{line},{tp},{labels[pred_label]}\n'
                predW.write(newLine)

                if labels[pred_label] != 0:
                    print('Label:', labels[pred_label], " text:", text)
