import os
from abc import abstractmethod
import torch
import pickle
import json
import numpy as np
import boto3

from .core.base_ner import BaseNER


class BertModel(BaseNER):
    def __init__(self, model_dir, loc):
        self.model, self.tokenizer = self.source_model(model_dir, loc)

    def source_model(self, model_dir, loc):
        print('model dir: ', model_dir)
        s3 = boto3.resource('s3')
        obj = s3.Object(model_dir, os.path.join(loc, 'model.json'))
        body = obj.get()['Body'].read()
        model_dict = json.loads(body)
        indx2val = {v: k for k, v in model_dict.items()}
        print('indx2val11 ', indx2val)

        model = pickle.loads(s3.Bucket(model_dir).Object(os.path.join(loc, 'bert_model.pkl')).get()['Body'].read())

        print('model loaded')
        model.eval()
        print('loading tklizer')
        tokenizer = pickle.loads(s3.Bucket(model_dir).Object(os.path.join(loc, 'tokenizer.pkl')).get()['Body'].read())

        print('loaded tklizer')
        return {"model": model, "indx2val": indx2val}, tokenizer

    @abstractmethod
    def preprocess(self, list_of_blobs):
        '''return processed blob'''
        pass

    def predict(self, text, delimiter=". "):
        res = []
        for line in text.split(delimiter):
            if line != "":
                line = line.replace(". ", "")
                line = "[CLS] " + line + " [SEP]"

                tokens = self.tokenizer.tokenize(line)
                input_ids = self.tokenizer.convert_tokens_to_ids(tokens)
                input_ids = torch.tensor([input_ids])
                model_predictions = self.predict_from_ids(input_ids, tokens, line)
                res.extend(list(set(model_predictions)))
        res = self.list_to_dict(res)
        res = self.get_corrected_index(res, text)
        return res

    def merge_bert_token_fragments(self, tokens, label_indices, mapper):
        # Bert breaks unseen words into multiple fragments with ## prefix
        new_tokens, new_labels = [], []
        # merge tokens.
        for token, label_idx in zip(tokens, label_indices[0]):
            # clean unknown tokens
            if token.startswith("##"):
                new_tokens[-1] = new_tokens[-1] + token[2:]
            else:
                new_labels.append(mapper[label_idx])
                new_tokens.append(token)
        return new_labels, new_tokens

    def predict_from_ids(self, input_ids, tokens, text):
        model_predictions = []
        mapper = self.model['indx2val']
        model = self.model['model']
        with torch.no_grad():
            output = model(input_ids)
        label_indices = np.argmax(output.to('cpu').numpy(), axis=2)

        new_tokens, new_labels = self.merge_bert_token_fragments(tokens, label_indices, mapper)
        predictions = list(zip(new_tokens, new_labels))
        print('before postprocess: ', predictions, ' , txt: ', text)
        predictions = self.postprocess(predictions,text)
        model_predictions.extend(predictions)
        model_predictions = list(set(model_predictions))
        model_predictions = [(wrd.lower(), ent.lower(),ent1,ent2) for wrd, ent, ent1,ent2 in model_predictions]

        return list(set(model_predictions))

    def __call__(self, text):
        return self.predict(text.lower())

