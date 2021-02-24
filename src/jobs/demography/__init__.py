
from .model.classes.spacy_ner import SpacyModel
from .model.classes.bert_ner import BertModel
import csv
import os
import base64

class DemographyJobContext:
    def __init__(self):
        self.spacymodel = SpacyModel(model_name="en_core_web_lg")
        self.bertmodel = BertModel(model_dir='hawk-models', loc='prod/ner/demography/version1.0')

spacymodel = SpacyModel(model_name="en_core_web_lg")
bertmodel = BertModel(model_dir='hawk-models', loc='prod/ner/demography/version1.0')
def analyze(sc):
    #TODO Move all configurations to a config file instead of hardcoding here.
    print("Running Demography prediction")
    input_path = 's3a://hawk-dataset/input/predict.csv'
    input_path_small = 's3a://hawk-dataset/input/predict_small.csv'
    aws_access_key_id = '<AWS ACCESS KEY >'
    aws_secret_key = '<AWS SECRET KEY>'
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key_id)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
                                      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
    sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
    rdd = sc.textFile(input_path)\
            .map(lambda line: line.split(",")) \
            .filter(lambda line: len(line) > 1) \
            .map(lambda line: (line[0], line[:-1])) \
            .collect()
    print('type', type(rdd))
    rdd = sc.textFile(input_path)
    rdd = rdd.mapPartitions(lambda x: csv.reader(x))
    print(type(rdd))
    header = rdd.first()
    rdd = rdd.filter(lambda x: x != header)
    predcition = rdd.map(lambda d: {'text': base64.b64decode(d[0]).decode("utf-8"), 'pii': list(map(lambda x: x.strip(), d[1].split(',')))})\
                    .map(lambda item: predict(item['text'], item['pii']))
    predcition.saveAsTextFile('s3a://hawk-job/output')


def predict(blob, piis_to_detect):
    print(os.getcwd())
    spacy_res = spacymodel(blob)
    bert_res = bertmodel(blob)
    predicted_piis = bertmodel.merge_two_dict(spacy_res, bert_res)
    detected_piis, prediction_details = remove_unwanted_piis(predicted_piis, piis_to_detect)
    prediction_details = bertmodel.result_to_output_payload(prediction_details)
    print('prediction ', prediction_details)
    return detected_piis, prediction_details


def remove_unwanted_piis(predicted_piis, piis_to_detect):
    """
    :param predicted_piis: dict
    :param piis_to_detect: list[str] list of piis
    :return:
    detected_piis : list[str] list of piis detected in the text
    prediction_details: dict
    """
    detected_piis = []
    prediction_details = {}
    for piis in piis_to_detect:
        if piis in predicted_piis:
            prediction_details[piis] = predicted_piis[piis]
            detected_piis.append(piis)
    return detected_piis, prediction_details





