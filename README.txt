Details on how this software runs:

1. We first start off by creating an object of the type InvertedIndex()

2. Once we have created this... we do the following to create our InvertedIndex
    
    - We run the class function .run() and this initializes and creates our InvertedIndex into disk.

3. When we have our inverted index, all our tokens should be in a file called merged_output.txt . 
    We can access this whenever, and now longer need to call .run()

4. When we have our merged_output.txt, we can call app.py.

5. Running app.py, we go on any browser and go to the url "localhost:8000".
    Now our search engine is ready to be used.