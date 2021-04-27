import xmltodict, json
import os
import ast
import pandas as pd
data_path = '/Users/jingweizhang/Documents/Projects/BoE_news_data/'
data_hist = 'text_en_BoE_199801_201710'
data_recent = 'text_en_BoE_201711_201908'

def get_altId(altid_item):
    for value in altid_item:
        if value['@type'] == "idType:USN":
            return value['#text']
        
# Transform xml format date to json files
def xml_to_json(file_name, data_dir, save_output = False):
    xml_file = open(data_path + data_dir + '/' + file_name).read()
    obj = xmltodict.parse(xml_file)
    dict_file = json.loads(json.dumps(obj))
    if save_output == True:
        with open(data_path + 'json_data/'+ file_name[:-3] + '.json', 'w') as outfile:
             json.dump(dict_file, outfile)
    return dict_file

# Extract meta table
def json_to_dict_2017(file_name,data_dir):
    dict_file = xml_to_json(file_name,data_dir)
    news_item = dict_file['newsMessage']['itemSet']['newsItem']         
    news_dict = dict()
    
    news_dict = {'Id': file_name.split('_')[1][:-4],
                 'time': dict_file['newsMessage']['header']['sent'],
                 'language':news_item['contentMeta']['language']['@tag'],
                 'headline':news_item['contentMeta']['headline'],
                 'wordcount': int(news_item['contentSet']['inlineXML']['@wordcount']),
                 'genre': None,
                 'slugline': news_item['contentMeta']['slugline']['#text'],
                 'subject':','.join([value['@qcode'] for value in news_item['contentMeta']['subject'] if '@qcode' in value.keys()]),
                 'urgency': news_item['contentMeta']['urgency'],
                 'altId': get_altId(news_item['contentMeta']['altId'])
                 }
    if 'genre' in news_item['contentMeta'].keys():
        genres = news_item['contentMeta']['genre']
        if type(genres) == list:
            news_dict['genre'] = ','.join([value['name'] for value in genres])

        else:
            news_dict['genre'] = genres['name'] 
    
    return news_dict  

def json_to_dict_1998(file_name,data_dir):
#     print(file_name)
    dict_file = xml_to_json(file_name,data_dir)
    news_item = dict_file['n-docbody']['document']
    news_dict = dict()
    
    ner_item = news_item['indexing']
    
    if type(ner_item) == list:
        ner_item = ner_item[1]
        
    try:
        ric_codes_list = news_item['indexing'][0]['business-terms']['ric-code-wrap']['ric-code']
        ric_codes = [value['#text'] for value in ric_codes_list]
    except:
        ric_codes = None
        
    try:
        time = news_item['pub-info']['pub-date']['@iso-time']
    except: 
        time = None
        
        
    def get_feature_list(classification_feature):
        '''
        feature: location, subject, industry
        
        '''
        if 'classification-terms' in ner_item.keys():
            block = 'pres-%s-block'%(classification_feature)
            wrap =  'pres-%s-wrap'%(classification_feature)
            feature_list = None
            if type(ner_item['classification-terms']) == list:
                if block in ner_item['classification-terms'][0].keys():
                    feature_list = ner_item['classification-terms'][0][block][wrap]
                elif block in ner_item['classification-terms'][1].keys():
                    feature_list = ner_item['classification-terms'][1][block][wrap]

            elif block in ner_item['classification-terms'].keys():
                 feature_list = ner_item['classification-terms'][block][wrap]

            if feature_list != None:
                if type(feature_list) == list:
                    feature_items = [value['pres-%s'%(classification_feature)] for value in feature_list]
                    return ','.join(feature_items)
                else:
                    feature_items = feature_list['pres-%s'%(classification_feature)]
                    return  feature_items
            
    
              

    news_dict = {'Id':file_name.split('.')[0], 
                'date': news_item['pub-info']['pub-date']['sort-pub-date'],
                 'time': time,
                 'language': news_item['@load-language'],
                 'headline': news_item['title-info']['title']['sort-title'],
                 'wordcount': int(news_item['content']['derived-word-count']['#text'].split()[-1]),
                 'location':get_feature_list('location'),
                 'subject':get_feature_list('subject'),
                 'industry':get_feature_list('industry'),
                 'ric_codes': ric_codes
                 }
    
    if type(news_dict['headline']) == dict:
        news_dict['headline'] = news_dict['headline']['#text']
    
    return news_dict


# Extract files 

def get_text_body_1998(test_id):
    data_hist = 'text_en_BoE_199801_201710'
    doc_file = xml_to_json(test_id, data_hist, save_output = False)
    text_dict = doc_file['n-docbody']['document']['content']['text']
    para_list = []
    if type(text_dict) == list:
        for item in text_dict:
            if type(item['p']) == list:
                for para in item['p']:
                    try:
                        para_list.append(para['#text'])
                    except:
                        pass
            else:
                para_list.append(item['p']['#text'])
    else:
        if type(text_dict['p']) == list:
            for para in text_dict['p']:
                try:
                    para_list.append(para['#text'])
                except:
                    pass
        else:
            para_list = text_dict['p']['#text']
    
    if type(para_list) == list:
        return [' '.join(para.split('\n')) for para in para_list if para != None]
    else:
        return ' '.join(para_list.split('\n'))

def get_text_body_2017(test_id):
    data_recent = 'text_en_BoE_201711_201908'
    dict_file = xml_to_json(test_id, data_recent)
    para_list = dict_file['newsMessage']['itemSet']['newsItem']['contentSet']['inlineXML']['html']['body']['p'] 
    if type(para_list) == list:
        return [' '.join(para.split('\n')) for para in para_list if para != None]
    else:
        return ' '.join(para_list.split('\n'))