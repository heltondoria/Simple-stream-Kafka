import time
import cv2
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

producer = KafkaProducer(bootstrap_servers=['192.168.2.102:9092'], retries=5)
topic = 'my-topic'


def video_emitter(video):
    video = cv2.VideoCapture(video)
    print(' emitting.....')

    while video.isOpened:
        success, image = video.read()

        if not success:
            break

        ret, jpeg = cv2.imencode('.png', image)
        future = producer.send(topic, jpeg.tobytes())

        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
            logging.exception(KafkaError)
            pass

        print(record_metadata.topic)
        print(record_metadata.partition)
        print(record_metadata.offset)
        # To reduce CPU usage create sleep time of 0.2sec
        time.sleep(0.2)

    # clear the capture
    video.release()
    print('done emitting')


if __name__ == '__main__':
    video_emitter('video.mp4')
