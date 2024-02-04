import json
import pika

def produce_message(data):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='data_queue')

    channel.basic_publish(exchange='',
                          routing_key='data_queue',
                          body=json.dumps(data))
    print(f" [x] Sent {data}")

    connection.close()

# Assuming you have a function to read data from the file
def read_data_from_file():
    with open('C:\\Users\\dasak\\assignment_updated.json', 'r') as file:
        data = json.load(file)
    return data

if __name__ == '__main__':
    data = read_data_from_file()
    produce_message(data)
