
## Code of Flask Server for Upload and Display Image + Kafka Producer  ##


import os
import random
import sqlite3
import sys
import uuid

from flask import (Flask, redirect, render_template_string, request,
                   send_from_directory)

## Creating DB and Images Folder
IMAGES_DIR = "images"
MAIN_DB = "main.db"

app = Flask(__name__)

#***************** Creating PRODUCER ***************
from confluent_kafka import Producer
import socket
topic = 'Kafka_ImageClass_Hagar21'
client_id = 'admin_hagar100'
conf = {'bootstrap.servers': "34.70.120.136:9094,35.202.98.23:9094,34.133.105.230:9094",
        'client.id': client_id}
producer = Producer(conf)
#*****************************************

def get_db_connection():
    conn = sqlite3.connect(MAIN_DB)
    conn.row_factory = sqlite3.Row
    return conn


con = get_db_connection()
con.execute("CREATE TABLE IF NOT EXISTS image(id, filename, object)")
if not os.path.exists(IMAGES_DIR):
    os.mkdir(IMAGES_DIR)


@app.route('/', methods=['GET'])
def index():
    con = get_db_connection()
    cur = con.cursor()
    res = cur.execute("SELECT * FROM image")
    images = res.fetchall()
    con.close()
    return render_template_string("""
<!DOCTYPE html>
<html>
<head>
<style>

.container {
          display: grid;
          grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
          grid-auto-rows: minmax(100px, auto);
          gap: 20px;
              }
img {
           display: block;
           max-width:100%;
           max-height:100%;
           margin-left: auto;
           margin-right: auto;
        }
.img {
       height: 270px;
    }
    
 .label {
           height: 30px;
          text-align: center;
        }
    
}

</style>

<script>
    function functionToRefresh()
    {
        window.location = window.location.href;
    }
    setInterval('autoRefresh()', 20);
</script>
</head>

<body style="background-color:#9FE2BF;">
<form method="post" enctype="multipart/form-data">

<div>
    <h1 
        style="font-weight: normal;
               font-size: 40px;
               font-weight: bold;
               text-align: center;
               color:#DE3163;
               text-transform: uppercase;">
                                  Custom File Upload <span style = " color:White;text-align: center; font-size: 20px;  display: block; padding-left: 4px;">With Kafka</span>
    </h1>    
  </div>
  
  <div style = "">
        <label  style="color:teal;font-weight:500; font-size: 20px; " for="file">Choose Image to upload</label>
        <input  style= "	cursor: pointer;" type="file" id="file" name="file" accept="image/x-png,image/gif,image/jpeg" />
  </div>
  
  
  <div>
    <button
       style= "
                  background-color: #EA4C89;
                  border-radius: 10px;
                  border-style: none;
                  color: #FFFFFF;
                  cursor: pointer;
                  font-weight: 500;
                  height: 30px;
                  width:80px;
                  line-height: 30px;
                  text-align: center;" onclick="functionToRefresh()">Submit</button> </br> </br>
  </div>
</form>

<div class="container">  {% for image in images %}
<div>
        <div class="img"><img src="/images/{{ image.filename }}"></img></div>
        <div class="label">{{ image.object | default('undefined', true) }}</div>
</div>
{% endfor %}
</div>
</body>
</html> """, images=images)


@app.route('/images/<path:path>', methods=['GET'])
def image(path):
    return send_from_directory(IMAGES_DIR, path)


@app.route('/object/<id>', methods=['PUT'])
def set_object(id):
    con = get_db_connection()
    cur = con.cursor()
    json = request.json
    object = json['object']
    cur.execute("UPDATE image SET object = ? WHERE id = ?", (object, id))
    con.commit()
    con.close()
    return '{"status": "OK"}'


@app.route('/', methods=['POST'])
def upload_file():
    f = request.files['file']
    ext = f.filename.split('.')[-1]
    id = uuid.uuid4().hex
    filename = "{}.{}".format(id, ext)
    f.save("{}/{}".format(IMAGES_DIR, filename))
    con = get_db_connection()
    cur = con.cursor()
    cur.execute("INSERT INTO image (id, filename, object) VALUES (?, ?, ?)", (id, filename, ""))
    con.commit()
    con.close()

#*****************PRODUCER***************
    ##Send Img_Id to the producer
    producer.produce(topic,  key=id, value=id)
    producer.flush()

#*****************PRODUCER***************

    return redirect('/')


if __name__ == '__main__':
    app.run(debug=True, port=(int(sys.argv[1]) if len(sys.argv) > 1 else 5000))