import time
import cv2
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

topic = 'video-stream'
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], retries=5)

def video_emitter(video):
    video = cv2.VideoCapture(video)
    print(' emitting.....')

    while video.isOpened:
        success, image = video.read()

        if not success:
            # or key ==27:
            break

        ret, jpeg = cv2.imencode('.png', image)
        future = producer.send(topic, jpeg.tobytes())

        try:
            record_metadata = future.get(timeout=10)
            print("topic: {}, partition: {}, offset: {}".format(record_metadata.topic, record_metadata.partition, record_metadata.offset))
        except KafkaError:
            logging.exception(KafkaError)
            pass


        # To reduce CPU usage create sleep time of 0.2sec
        time.sleep(0.2)

    # clear the capture
    video.release()
    print('done emitting')


if __name__ == '__main__':
    video_emitter('video.mp4')
