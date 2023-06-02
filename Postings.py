
class Posting:
    
    def __init__(self, doc_id, freq_count):
        self.doc_id = doc_id
        self.freq = freq_count

    def get_doc_id(self):
        return self.doc_id
    
    def get_freq(self):
        return self.freq