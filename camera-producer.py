import time
import cv2
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], retries=5)
topic = 'video-stream'
face_cascade = cv2.CascadeClassifier('haarcascade_frontalface_default.xml')

def video_emitter():
    video = cv2.VideoCapture(0)
    print(' emitting.....')

    while video.isOpened:
        success, image = video.read()
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        faces = face_cascade.detectMultiScale(gray, 1.3, 5)

        for (x, y, w, h) in faces:
            cv2.rectangle(image, (x,y), (x+w, y+h), (255, 0, 0), 2)
            # roi_gray = gray[y:y+h, x:x+w]
            # roi_color = image[y:y+h, x:x+w]

        if not success:
            break

        ret, jpeg = cv2.imencode('.png', image)
        future = producer.send(topic, jpeg.tobytes())

        try:
            record_metadata = future.get(timeout=10)
            print("topic: {}, partition: {}, offset: {}".format(record_metadata.topic, record_metadata.partition, record_metadata.offset))
        except KafkaError:
            logging.exception(KafkaError)
            pass

        time.sleep(0.2)

    # clear the capture
    video.release()
    print('done emitting')

if __name__ == '__main__':
    video_emitter()
