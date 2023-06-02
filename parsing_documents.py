import os, nltk, json, re, string, lxml
from bs4 import BeautifulSoup
from nltk.tokenize import word_tokenize
from nltk.stem.porter import PorterStemmer
from nltk.util import ngrams



def get_json_content(file: json) -> str:
    '''loads the json file and returns the string contents of the json'''
    with open(file, 'r') as json_file:
        data = json.load(json_file)
    
    return str(data['content'])

def get_json_url(file: json) -> str:
    '''gets the json file's string url'''
    with open(file, 'r') as json_file:
        data = json.load(json_file)

    return str(data['url'])


def json_parse_to_tokens(json: str) -> list:
    '''handles the parsing of the 'contents' section and returns a list of tokens in the document'''

    words = []
    res = []

    try: 
        soup = BeautifulSoup(json, "lxml")

        #print(soup)

        for script in soup(["script", "style"]):
            script.extract()    # rip it out
                # get text
        text = soup.get_text()

        #removes non english, punc, dsmdmsm
        text = re.sub(r'[^\x00-\x7F]+', ' ', text)

        #prepaparing the html content into a string
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = '\n'.join(chunk for chunk in chunks if chunk)

        #cleaning the string
        tokens = word_tokenize(text)
        table = str.maketrans('', '', string.punctuation)
        stripped = [w.translate(table) for w in tokens]

        words = [word.lower() for word in stripped if word.isalpha()]

        #stemming the word
        porter = PorterStemmer()
        words = [porter.stem(word) for word in words]
        
        #adding bigrams to the token list
        bigrams = ngrams(words, 2)
        trigrams = ngrams(words, 3)

        #combination of unigram, bigram, trigram :D
        res = words + [" ".join(bigram) for bigram in bigrams] + [" ".join(x) for x in trigrams]

    except:
        pass

    return res


if __name__ == "__main__":
    url = get_json_content("/Users/andrewchang/Desktop/DEV/aiclub_ics_uci_edu/8ef6d99d9f9264fc84514cdd2e680d35843785310331e1db4bbd06dd2b8eda9b.json")
    #print(url)

    print(json_parse_to_tokens(url))

    #print(calculate_tf_in_doc(url_parse_to_tokens(url)))