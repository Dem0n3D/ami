from flask import Flask, request, jsonify
from tasks import calculate


app = Flask(__name__)

@app.route("/test")
def test():
    x = int(request.args.get("x", 0))
    calculate.delay(x, 'abc')
    return jsonify({"status": "OK"})

if __name__ == '__main__':
    app.run()
