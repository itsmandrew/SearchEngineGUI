import os, sys, heapq, math, time, itertools, re
from Postings import Posting
from collections import defaultdict
from parsing_documents import get_json_content, get_json_url, json_parse_to_tokens
from nltk.stem.porter import PorterStemmer
from urllib.parse import urlparse

class InvertedIndex:

    def __init__(self, directory: str):
        
        #one of many hashmaps sigh
        self.document_id_to_url = defaultdict(str)

        #second hashmap
        self.token_to_postings = defaultdict(list)

        #indexing the indexing
        self.index_of_the_index = defaultdict(int)

        self.pages = 0
        self.directory = directory

        self.MAX_DICT_SIZE = 10000000 # mb


    def init_docid_to_url(self) -> None:
        '''  initializes the hashmap from  key : id_number -> int , value: file -> str '''

        url_set = set()

        for filename in os.listdir(self.directory):
            #ANALYST/...
            path = self.directory + "/" + filename

            #need this to avoid DS.store?
            if os.path.isdir(path):
                for f in os.listdir(path):

                    new_path = path + "/" + f #actual file
                    #ANALYST/folder/...

                    #handling duplicate pages...
                    url = get_json_url(new_path)
                    parsed_url = urlparse(url)
                    cleaned_url = parsed_url.scheme + '://' + parsed_url.netloc + parsed_url.path
                    
                    #if cleaned_url is already in the dict.values()
                    if cleaned_url in url_set:
                        continue
                    
                    url_set.add(cleaned_url)

                    self.document_id_to_url[self.pages] = cleaned_url
                    self.pages += 1

            else:
                continue
        
        #print(self.pages)
        url_set.clear()
        

    def init_indexes_simple(self) -> None:
        ''' initializes the hashmap from :   key: token -> str , value: Posting() uhh   '''

        curr_page = 0


        for filename in os.listdir(self.directory):
            path = self.directory + "/" + filename   #directory

            if os.path.isdir(path):
                for f in os.listdir(path): 
                    
                    new_path = path + "/" + f #actual file
                    
                    '''part that actually matters'''

                    #get the json[content] 
                    json_content = get_json_content(new_path)

                    #tokens to use for the count
                    tokens_in_page = json_parse_to_tokens(json_content)

                    #unique tokens to use for the indexer
                    unique_tokens = list(set(json_parse_to_tokens(json_content)))

                    #making the tokens for each file...
                    for token in unique_tokens:
                        self.token_to_postings[token].append( Posting(curr_page, tokens_in_page.count(token)) )  # token - > [doc_id, word_count]

                    print(new_path)
                    curr_page += 1

                #break #one directory test
                
            else:
                continue
    
    def init_partial_indexes(self) -> list():
        ''' initializes the hashmap from :   key: token -> str , value: Posting() uhh   
            returns a list of the file names for combining.
        '''

        curr_page = 0
        file_number = 0
        res  = [] 

        # starting the directory search man
        for filename in os.listdir(self.directory):
            #ANALYST/...
            path = self.directory + "/" + filename

            #need this to avoid DS.store?
            if os.path.isdir(path):
                for f in os.listdir(path):
            
                    new_path = path + "/" + f #actual file
                    #ANALYST/folder/...

                    #handling duplicate pages...
                    url = get_json_url(new_path)
                    parsed_url = urlparse(url)
                    cleaned_url = parsed_url.scheme + '://' + parsed_url.netloc + parsed_url.path
                    
                    if self.document_id_to_url[curr_page] != cleaned_url:
                        continue
                
                    '''part that actually matters'''

                    #get the json[content] 
                    json_content = get_json_content(new_path)

                    #tokens to use for the count
                    tokens_in_page = json_parse_to_tokens(json_content)

                    #unique tokens to use for the indexer
                    unique_tokens = list(set(json_parse_to_tokens(json_content)))

                    #making the tokens for each file...
                    for token in unique_tokens:
                        self.token_to_postings[token].append( Posting(curr_page, tokens_in_page.count(token)) )  # token - > [doc_id, word_count]
                    

                    #emptying memory and writing to disk when it hits 10 MB
                    if sys.getsizeof(self.token_to_postings) >= self.MAX_DICT_SIZE:

                        sorted_index = sorted(self.token_to_postings.items())
                        file_name = "index" + str(file_number) + ".txt"

                        with open(file_name, "w") as f:

                            for term, postings in sorted_index:
                                posting_str = " ".join(str( str(p.doc_id) + '-' + str(p.freq ) )  for p in postings)
                                f.write(f"{term}: {posting_str}\n")
                        
                            #adding to the result list for merging later
                            res.append(file_name)

                            #clearing memory in index
                            self.token_to_postings.clear()
                            file_number += 1
                        

                    print(new_path + " " + str(curr_page))
                    #updating the pages for the postings / indexer
                    curr_page += 1


        # dumping the rest 
        sorted_index = sorted(self.token_to_postings.items())
        file_name = "index" + str(file_number) + ".txt"

        with open(file_name, "w") as f:

            for term, postings in sorted_index:
                posting_str = " ".join(str( str(p.doc_id) + '-' + str(p.freq ) )  for p in postings)
                f.write(f"{term}: {posting_str}\n")
        
            #adding to the result list for merging later
            res.append(file_name)

            #clearing memory in index
            self.token_to_postings.clear()
            file_number += 1

        return res

    def multi_merge(self, files: list) -> None:
        '''combines multiple files and merges them into one file with all the words 0_0 '''
        
        # opens the files for reading
        inputs = [open(file, "r") for file in files]
        posting_str = ""

        # initializing a heap to hold the first line of all files...
        heap = []

        for i, file in enumerate(inputs):
            line = file.readline().rstrip()

            if line:

                word, page = line.split(":")
                page = page.lstrip().split(" ")
                heap.append((word, [x for x in page], i))

        heapq.heapify(heap)

        with open("merged_output.txt", "w") as output:

            prev_word = None
            prev_postings = []

            while heap: 
                
                token, postings, file_index = heapq.heappop(heap)

                # if the word is not the first and also the previous isnt the curr -> WRITE
                if prev_word is not None and prev_word != token:
                    posting_str = " ".join(prev_postings)
                    output.write(f"{prev_word.lstrip()}: {posting_str.rstrip()}\n")
                    prev_postings = []
                
                # yeah this works
                prev_word = token
                prev_postings.extend(postings)

                # getting the new line 0_0
                new_posting = inputs[file_index].readline()
                if new_posting:
                    # if the file is not empty, insert the next posting into the heap
                    word, page = new_posting.split(":")
                    page = page.lstrip().split(" ")
                    heapq.heappush(heap, (word.lstrip(), [x.rstrip() for x in page], file_index))

            
            if prev_word is not None:
                posting_str = " ".join(prev_postings)
                output.write(f"{prev_word.lstrip()}: {posting_str.rstrip()}\n")

        # deleting the files that were combined
        for file in inputs:
            file.close()
        
        for file in files:
            os.remove(file)
        
        
    def indexing_the_index(self, file: str) -> None:
        ''' this function assumes that the partial indexing and output.txt has already been created
            ex. ) "ape" is at position 300 in the output.txt file
            this will prob fit in memory 
        '''
        with open(file, "r") as f:
            line_beginning = 0

            for line in f:

                # split the line into words and their postings
                token, postings = line.strip().split(":")

                # maps the token to the start of the line that it's on
                # token  -> seek
                self.index_of_the_index[token] = line_beginning

                # move the file pointer to the beginning of the line
                #f.seek(line_beginning)
                #print(f.readline().rstrip())

                line_length = len(bytearray(line, encoding='utf-8'))

                line_beginning += line_length


    def process_query(self, query: str) -> str():
        '''WIP'''
        porter = PorterStemmer()
        processed_query = " ".join([porter.stem(word) for word in query.split(" ")])

        return processed_query


    def run_query(self, query: str, file: str) -> list():
    
        res = []
        #starting the timer
        start_time = time.time()
        unique_urls = set()
        #processing the query... (stemming)
        processed_query = self.process_query(query)
        position = None
        tf_idf_scores = []

        with open(file, "r") as f:
            
            # if not in dict...
            if processed_query in self.index_of_the_index:
                tf_idf_scores = []
                position = self.index_of_the_index.get(processed_query)
                f.seek(position)
                line = f.readline().rstrip()
                
                #since this is only one term query pretty much all we have to do is run this once
                tf_idf = self.get_tf_idf_scores(line)

                # removing duplicates
                tf_idf_scores = list(set(tf_idf))

                #sorting the top 10 pages and printing
                sorted_tf_idf = sorted(tf_idf_scores, key= lambda x : x[1], reverse=True)[0:10]

                for x, y in sorted_tf_idf:
                    if self.document_id_to_url[x] in unique_urls:
                        continue
                    res.append(self.document_id_to_url[x])
                    unique_urls.add(self.document_id_to_url[x])



            else: # not a term in the dictionary lol
                processed_query = self.get_word_combinations(processed_query.split(" "))

                cleaned = []

                # just trying to get any valuble information from the query... and making sure they are in our index
                for word in processed_query:
                    if (word not in self.index_of_the_index):
                        continue
                    cleaned.append(word)
                
                # for every word in cleaned... add to our tf_idf scores on that word
                for word in cleaned:
                    position = self.index_of_the_index.get(word)
                    f.seek(position)
                    line = f.readline().rstrip()

                    tf_idf_scores.extend(self.get_tf_idf_scores(line))

                # removing duplicates
                tf_idf_scores = list(set(tf_idf_scores))

                #sorting the top 10 pages and printing
                sorted_tf_idf = sorted(tf_idf_scores, key= lambda x : x[1], reverse=True)[0:10]
                for x, y in sorted_tf_idf:
                    if self.document_id_to_url[x] in unique_urls:
                        continue
                    res.append(self.document_id_to_url[x])
                    unique_urls.add(self.document_id_to_url[x])
                
                
        #ending and printing out the query time       
        end_time = time.time()
        elapsed_time = (end_time - start_time) * 1000
        print("Elapsed time: {:.2f} ms".format(elapsed_time))
        return res

    def get_tf_idf_scores(self, line: str) -> None:
        ''' given the query info, we process the value and calculate the tf-idf scores 
            keep in mind... thing only gives the tf-idfs of ONE word
        '''

        tf_idf = []
        line = line.split(":")

        line = line[1].lstrip().split(" ")
        df = len(line)

        #print(line)

        for posting in line:
            doc_tf = [int(x) for x in posting.split('-')]

            tf = 1 + math.log10(doc_tf[1]) # calculate term frequency using logarithm
            idf = math.log10(self.pages / df) # calculate inverse document frequency
            tf_idf.append((doc_tf[0], tf * idf))

        return tf_idf


    def get_word_combinations(self, word_list: list) -> set:
        """
        Given a list of words, returns all possible combinations of those words.
        """
        words = set(word_list)
        combinations = set(words)
        for i in range(2, len(words)+1):
            for c in itertools.combinations(words, i):
                combinations.add(' '.join(c))
        return list(combinations)


    def run(self) -> None:
        ''' all the code to initialize our search engine 
            we don't have to run this constantly, however, 
            we must run it once, to write all our tokens onto disc
        '''
        self.init_docid_to_url()
        files = self.init_partial_indexes()
        self.multi_merge(files)


if __name__ == "__main__":

    #must initialize directories / ids before indexing
    i_d = InvertedIndex("/Users/andrewchang/Desktop/DEV")
    #i_d.run()

    #indexing the index needs to be run separately since it's in memory
    i_d.indexing_the_index("merged_output.txt")
    i_d.run_query("merged_output.txt")