# producer.py
import time
import cv2
from kafka import KafkaProducer
#  connect to Kafka
from kafka.errors import KafkaError
import logging

producer = KafkaProducer(bootstrap_servers=['192.168.2.102:9092'], retries=5)
# Assign a topic
topic = 'my-topic'


def video_emitter(video):
    # Open the video
    video = cv2.VideoCapture(video)
    print(' emitting.....')

    # read the file
    while video.isOpened:
        # read the image in each frame
        success, image = video.read()

        # check if the file has read the end
        if not success:
            break

        # convert the image png
        ret, jpeg = cv2.imencode('.png', image)
        # Convert the image to bytes and send to kafka
        # Asynchronous by default
        future = producer.send(topic, jpeg.tobytes())

        # Block for 'synchronous' sends
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
            # Decide what to do if produce request failed...
            logging.exception(KafkaError)
            pass

        # Successful result returns assigned partition and offset
        print(record_metadata.topic)
        print(record_metadata.partition)
        print(record_metadata.offset)
        # To reduce CPU usage create sleep time of 0.2sec
        time.sleep(0.2)
    # producer.flush()
    # clear the capture
    video.release()
    print('done emitting')


if __name__ == '__main__':
    video_emitter('video.mp4')
