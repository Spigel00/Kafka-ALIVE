from flask import Flask, request, render_template_string
from kafka import KafkaProducer
import json
import datetime

app = Flask(__name__)

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

HTML_PAGE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <title>Producer (Waiter)</title>
    <style>
        body { font-family: sans-serif; text-align: center; padding: 50px; background: #f0f2f5; }
        button { padding: 15px 30px; font-size: 20px; background: #007bff; color: white; border: none; cursor: pointer; border-radius: 5px; }
        button:hover { background: #0056b3; }
        .status { margin-top: 20px; color: green; }
    </style>
</head>
<body>
    <h1>📝 Order Input</h1>
    <form action="/send" method="post">
        <button type="submit">🔥 Send "Urgent Order"</button>
    </form>
    {% if message %}
        <div class="status">✅ {{ message }}</div>
    {% endif %}
</body>
</html>
"""

@app.route('/', methods=['GET'])
def index():
    return render_template_string(HTML_PAGE)

@app.route('/send', methods=['POST'])
def send_message():
    timestamp = datetime.datetime.now().strftime("%H:%M:%S")
    msg = {"time": timestamp, "content": "Critical Update", "status": "NEW"}
    
    producer.send('demo-topic', value=msg)
    producer.flush()
    
    return render_template_string(HTML_PAGE, message=f"Sent at {timestamp}")

if __name__ == '__main__':
    print("👍 Producer WebApp running on http://localhost:2005")
    app.run(port=2005, debug=True)