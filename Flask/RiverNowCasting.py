from flask import Flask, render_template

app = Flask(__name__)


@app.route('/')
def home():
    return 'Home page. Test text'

if __name__ == '__main__':
    app.run(debug=True)
