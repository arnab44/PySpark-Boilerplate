from abc import abstractmethod
import re
import pdb

class BaseNER:
    @abstractmethod
    def preprocess(self, blob):
        '''return processed blob'''
        pass

    @abstractmethod
    def predict(self, blob):
        pass
    
    def count_leading_space(self,x):
      count =0 
      for letter in x:
        if letter == " ": count+=1
        else: break
      return count

    def postprocess(self, predictions,text):
        """cleans the predicted tokens by replaceing B,I and removing O"""
        entities = []
        current_entity = ""  # combination of B, I into single entity
        current_entity_type = None
        index = 0
        for entity, word in predictions:
            print('entity: ', entity, ' word: ', word)
            n_white_space = self.count_leading_space(text[index+len(current_entity):])
            print('n_white_space ', n_white_space)
            if current_entity == "":
                matched_index = self.get_matching_index(text[index:],word)[0]
                index += matched_index[0]
            # reset...
            # no previous entity and no current entity
            if entity.upper() == 'O' :
              if current_entity == "": continue
              #previous entity but no current entity
              else :
                entities.append((current_entity, current_entity_type, index, index + len(current_entity)))
                index = index + len(current_entity)
                current_entity = ""
                current_entity_type = None
            else:
                # prev entity and current entity are same
                if current_entity_type == entity.split("-")[1]:
                    #put exact white space as is in actual text
                    current_entity = current_entity + (" ") * n_white_space + word
                # no prev entity but current entity
                elif current_entity == "":
                    current_entity_type = entity.split("-")[1]
                    current_entity = word
                # different prev and current entity
                else:
                    entities.append((current_entity, current_entity_type, index, index + len(current_entity_type)))
                    index = index + len(current_entity)
                    current_entity_type = entity.split("-")[1]
                    current_entity = word

        if current_entity != "":
            entities.append((current_entity, current_entity_type, index, index + len(current_entity)))
        return entities

    def result_to_output_payload(self,result):
      for ent_type in result:
        result[ent_type] = [{"word":ent_tuple[0], "start_index":ent_tuple[1],"end_index":ent_tuple[2]} 
                            for ent_tuple in result[ent_type]]
      return result

    def list_to_dict(self, prediction_list):
        """
        :param prediction_list: list in the form [[word,entity,start_index,end_index]]
        :return: dict in the form {"ent" : [(word:str, start_index:int, end_index:int)]
        """
        new_dict = {}
        for word, entity, start_index, end_index in prediction_list:
            entity = entity.lower()
            if entity not in new_dict:
                new_dict[entity] = [(word, start_index, end_index)]
            else:
                new_dict[entity].append((word, start_index, end_index))
        return new_dict
        
      
    def merge_two_dict(self, res_dict1, res_dict2):
        """
        :param res_dict1: dict
        :param res_dict2: dict
        :return merged_dict: dict
        """
        merged_dict = {}
        for key in res_dict1.keys():
            if key in res_dict2.keys():
                merged_dict[key] = list(set(res_dict1[key]).union(set(res_dict2[key])))
            else:
                merged_dict[key] = list(set(res_dict1[key]))

        for key in res_dict2.keys():
            if key not in res_dict1.keys():
                merged_dict[key] = list(set(res_dict2[key]))
        
        return merged_dict

    def get_matching_index(self, text_to_search, word_to_match):
        """
        gets all regex match of word in text
        :param word_to_match: str
        :param text_to_search: str
        :return: list[(start_index:int,end_index:int)]
        """
        print('get_matching_index ....')
        print('text_to_search ', text_to_search, ' word_to_match ', word_to_match)
        word_to_match = word_to_match.lower()
        text_to_search = text_to_search.lower()
        indexes = [(m.start(0), m.end(0)) for m in re.finditer(re.escape(word_to_match), text_to_search)]
        if len(indexes) == 0:
            pattern = r"".join([wrd + "\s*\n*" for wrd in word_to_match.split()])
            pattern = pattern[0:-6]
            pattern = re.escape(pattern)
            indexes = [(m.start(0), m.end(0)) for m in re.finditer(pattern, text_to_search)]
        if len(indexes) >= 1:
            return indexes
        else:
            return None

    def get_closest_match(self, estimated_start_index, actual_index):
        """gets the closest index among all matched indexes
        :param estimated_start_index: int
        :param actual_index: list[tuple(int, int)]
        :return: tuple(start_index, end_index)
        """
        dist = [abs(index[0] - estimated_start_index) for index in actual_index]
        return actual_index[dist.index(min(dist))]

    def get_corrected_index(self, res, text):
        """
        :param res: dict
        :param text: str
        :return: dict with corrected index values
        """
        corrected_res = {}
        for key in res:
            corrected_res[key] = []
            for entity, estimated_start_index, estimated_end_index in res[key]:
                matched_indexes = self.get_matching_index(text.lower(), entity)
                if matched_indexes is None:
                    corrected_res[key].append((entity, estimated_start_index, estimated_end_index))
                    continue
                corrected_ind = self.get_closest_match(estimated_start_index, matched_indexes)
                if corrected_ind is not None:
                    corrected_res[key].append((entity, corrected_ind[0], corrected_ind[1]))
                else:
                    corrected_res[key].append((entity, estimated_start_index, estimated_end_index))
        return corrected_res
    
    def clean_null_values(self,res):
        '''
        input:
            result: {"pii_type":[(pii,start_ind,end_ind)]}
        '''
        null_list = ["null","nan","nan,","null,",""]
        for pii_type in res:
            res[pii_type] = [pii for pii in res[pii_type] if pii[0] not in null_list]
        return res