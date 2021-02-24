import spacy
from .core.base_ner import BaseNER


class SpacyModel(BaseNER):
    def __init__(self, model_name="en_core_web_lg"):
        self.spacy_model = spacy.load(model_name)
        self.SPACY_NER_MAPPER = {"gpe": "loc"}

    def predict(self, text):
        res = []
        doc = self.spacy_model(text)
        for entity in doc.ents:
            nth_word = entity.start
            start_index = len(" ".join(text.split()[0:nth_word]))
            end_index = start_index + len(entity.text)
            if entity.label_ in self.SPACY_NER_MAPPER.keys():
                res.append((entity.text.lower(), self.SPACY_NER_MAPPER[entity.label_.lower()], start_index, end_index))
            else:
                res.append((entity.text.lower(), entity.label_.lower(), start_index, end_index))

        # since loc are gpe in spacy model
        res = [(k, 'loc', start_index, end_index) if v == 'gpe' else (k, v, start_index, end_index)
               for k, v, start_index, end_index in res]

        # spacy considers time range in years also as date
        for k, v, start_index, end_index in res:
            if (v == 'date') and ('year' in k):
                res.remove((k, v, start_index, end_index))

        res = self.list_to_dict(res)
        return res

    def __call__(self, text):
        return self.predict(text)
