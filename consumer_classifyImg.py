## Code of Kafka Consumer - using Classifiers API for Image Classification  ##

import random
import sys
import requests
from confluent_kafka import Consumer, KafkaError, KafkaException
# for keras
from classification_models.keras import Classifiers
import numpy as np
from skimage.io import imread
from skimage.transform import resize
from keras.applications.imagenet_utils import decode_predictions
from classification_models.keras import Classifiers
from tensorflow import keras
# import tensorflow.keras
from classification_models.tfkeras import Classifiers


topic = "Kafka_ImageClass_Hagar21"
group_id = "GID_HagarIbrahim_ClassifyImgUisngAPI5100"


conf = {'bootstrap.servers': "34.70.120.136:9094,35.202.98.23:9094,34.133.105.230:9094",
        'group.id': group_id,
        'enable.auto.commit': False,
        'auto.offset.reset': 'earliest'}


def detect_object(id):
    # *****************************************************************************
    ResNet18, preprocess_input = Classifiers.get('resnet18')
    model = ResNet18((224, 224, 3), weights='imagenet')
    ResNet18, preprocess_input = Classifiers.get('resnet18')
    # read and prepare image
    x = imread("./images/"+id+".jpg")
    x = resize(x, (224, 224)) * 255  # cast back to 0-255 range
    x = preprocess_input(x)
    x = np.expand_dims(x, 0)
    # load model
    model = ResNet18(input_shape=(224, 224, 3), weights='imagenet', classes=1000)
    # processing image
    y = model.predict(x)
    # result
    Img_class= (decode_predictions(y)[0][0][1] + ' (' + str(round(decode_predictions(y)[0][0][2], 2)) + ' %)')
    return Img_class


consumer = Consumer(conf)
try:
    consumer.subscribe([topic])
    print('ImgClassifier Consumer ..')

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None: continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                    (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            print("Image Classified :", msg.value())
            requests.put('http://127.0.0.1:5000/object/' + msg.value().decode(), json={"object": detect_object(msg.value().decode())})
            consumer.commit(asynchronous=False)


finally:
    consumer.close()