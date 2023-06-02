from flask import Flask, render_template, request
from InvertedIndex import InvertedIndex
from collections import defaultdict
from urllib.parse import urlsplit


app = Flask(__name__, static_folder='static')

i_d = InvertedIndex("/Users/andrewchang/Desktop/DEV")
i_d.init_docid_to_url()
i_d.indexing_the_index("merged_output.txt")


@app.route('/', methods=['GET', 'POST'])


def search():
    global i_d
    if request.method == 'POST':
        query = request.form['query']
        results = i_d.run_query(query, "merged_output.txt")
        return render_template('results.html', results=results, query=query)
    else:
        return render_template('home.html')


if __name__ == '__main__':
    app.run(port=8000, debug=True)