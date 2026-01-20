from flask import Flask, Response, render_template_string
from kafka import KafkaConsumer
import json

app = Flask(__name__)

HTML_PAGE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <title>Consumer (Kitchen)</title>
    <style>
        body { font-family: sans-serif; padding: 50px; background: #282c34; color: white; }
        #messages { margin-top: 20px; }
        .card { background: #444; padding: 15px; margin-bottom: 10px; border-left: 5px solid #28a745; border-radius: 4px; animation: fadeIn 0.5s; }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(-10px); } to { opacity: 1; transform: translateY(0); } }
    </style>
</head>
<body>
    <h1>👨‍🍳 Live Kitchen Monitor</h1>
    <div id="messages">Waiting for orders...</div>

    <script>
        // Connect to the SSE stream
        const eventSource = new EventSource("/stream");
        const container = document.getElementById("messages");

        eventSource.onmessage = function(event) {
            if (container.innerHTML === "Waiting for orders...") container.innerHTML = "";
            
            const data = JSON.parse(event.data);
            const div = document.createElement("div");
            div.className = "card";
            div.innerHTML = `<strong>${data.time}</strong>: ${data.content} <span style="float:right">Status: ${data.status}</span>`;
            container.prepend(div); // Add new message to top
        };
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_PAGE)

@app.route('/stream')
def stream():
    def event_stream():
        # Unique Consumer Group ensures every browser tab gets the message
        consumer = KafkaConsumer(
            'demo-topic',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        for message in consumer:
            # Format as Server-Sent Event (data: ...)
            yield f"data: {json.dumps(message.value)}\n\n"

    return Response(event_stream(), mimetype="text/event-stream")

if __name__ == '__main__':
    print("👍 Consumer WebApp running on http://localhost:5001")
    app.run(port=5001, threaded=True)