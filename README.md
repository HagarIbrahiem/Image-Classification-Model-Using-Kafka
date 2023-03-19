# Image-Classification-Model-Using-Kafka
An Image Classification NN Model Using Flask, Kafka, Resne50, Image Classifiers and Pillow APIs



An implemnation of a simple Image Classification model using Apache Kafka . 

The code files include : 
1-Admin.py , with Kafka administrative client's implementation, responsible for managing topics and brokers. 
2-FlaskServer.py : includes implementation of Flask Server for image uploading and displaying, in addition to code implementation of  Kafka Producer.
3-Consumer_classifyImg.py: Kafka consumer's code for image classification 
4- Consumer_grayScaleImg.py : Kafka consumer's code for image processing (gray scale, Image Enhancement). 

**Tools and Technologies used:**
- Confluent-Kafka for managing a high-level producer, consumer and adminClient  
-ImageClassifiers API (using resnet50)
-Pillow API for image processing
-Web technologies (HTML , Javascript)
