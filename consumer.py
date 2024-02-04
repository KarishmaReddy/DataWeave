import json
import pika
import cx_Oracle

class DatabaseHandler:
    def __init__(self):
        # Connect to Oracle database
        self.connection = cx_Oracle.connect("username", "password", "localhost:1521/ORCLPDB1")
        self.cursor = self.connection.cursor()

        # Initialize tables and schema as needed

    def persist_data(self, data):
        total_products_tracked = self.get_total_products_tracked(data)
        total_instock_products = self.get_total_instock_products(data)
        availability_score = self.calculate_availability_score(total_instock_products, total_products_tracked)

        # Logic to persist data to the Oracle database, including availability_score
        self.save_to_oracle(data, availability_score)

    def save_to_oracle(self, data, availability_score):
        # Example: Insert data into Oracle table
        query = "INSERT INTO your_table_name (column1, column2, ..., availability_score) VALUES (:1, :2, ..., :3)"
        self.cursor.execute(query, (data['index'], data['meta_info'], availability_score))
        self.connection.commit()

    def get_total_products_tracked(self, data):
        meta_info = json.loads(data['meta_info'].replace("'", "\""))
        return meta_info.get('crawl_page_counter', 0)

    def get_total_instock_products(self, data):
        if data['stock'] == 'In Stock':
            return 1
        else:
            return 0

    def calculate_availability_score(self, total_instock_products, total_products_tracked):
        # Logic to calculate availability_score
        if total_products_tracked > 0:
            return total_instock_products / total_products_tracked
        else:
            return 0

def consume_messages():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='data_queue')

    db_handler = DatabaseHandler()

    def callback(ch, method, properties, body):
        data = json.loads(body.decode())
        db_handler.persist_data(data)

    channel.basic_consume(queue='data_queue', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    consume_messages()
