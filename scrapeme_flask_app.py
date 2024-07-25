import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer, KafkaConsumer
import json
import time
from flask import Flask, jsonify
import threading

def get_product():
    url = "https://scrapeme.live/shop/"
    response = requests.get(url)
    # use BeautifulSoup to read html content
    # it helps find and get data we need from webpage
    soup = BeautifulSoup(response.content, "html.parser")

    #create product list
    products = []
    for product in soup.select(".product"):
        title = product.select_one(".woocommerce-loop-product__title").text.strip()
        price = product.select_one(".price").text.strip()

        #get product link because description and stock information are in the product page
        product_link = product.select_one("a").get("href")

        #get product details from the product link 
        product_response = requests.get(product_link)
        product_soup = BeautifulSoup(product_response.content, "html.parser")

        description = product_soup.select_one(".woocommerce-product-details__short-description")
        description = description.text.strip() if description else "No description available"
        
        stock_info = product_soup.select_one(".stock")
        stock = stock_info.text.strip() if stock_info else "Stock information not available"

        #add informations to product list
        products.append({
            "title": title,
            "price": price,
            "description": description,
            "stock": stock
        })

    return products

#call function and print what we have
data = get_product()
print(data)

def create_producer():
    try:
        # create kafka producer to send messages to the kafka server
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092', # connect to the server
            value_serializer=lambda v: json.dumps(v).encode('utf-8') # convert messages to JSON format
        )
        return producer
    except Exception as e:
        print(f"Producer creating error: {e}")
        return None

def produce_messages(producer, data):
    #send data lsit to my-topic
    try:
        for item in data:
            producer.send('my-topic', value=item)
            print(f"Sent message: {item}")  # print the message to see being sent correctly
            time.sleep(1)  # waiting for 1 second to send next message
    except Exception as e:
        print(f"Sending error messages: {e}")

#this part was to check our producer was created correctly
# data = get_product()
# producer = create_producer()
# if producer:
#     produce_messages(producer, data)
# else:
#     print("Producer could not be created.")

def write_to_file():
    # create a consumer to read messages from my-topic
    consumer = KafkaConsumer(
        'my-topic',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='my-group',
        enable_auto_commit=True,
        auto_commit_interval_ms=5000
    )

    #create list for messages
    messages = []
    
    try:
        for message in consumer:
            print(f"Received message: {message.value}") #print messages so we can see what we received
            messages.append(message.value.decode('utf-8')) #decode message and add to list

            #write in json file
            with open('kafka_data.json', 'w') as file:
                json.dump(messages, file)
    except Exception as e:
        print(f"Consumer loop error: {e}")
    finally:
        consumer.close()
app = Flask(__name__)
# flask route
@app.route('/data', methods=['GET'])

def get_data():
    #read data from 'kafka_data.json' and return it as json
    try:
        with open('kafka_data.json', 'r') as f:
            #load data
            data = json.load(f)
        return jsonify(data)
    except Exception as e:
        print(f"Error reading kafka_data.json file: {e}")
        #return error http 500 status
        return jsonify({"error": "Failed to read data"}), 500

#flask runner
def run_flask():
    #web server
    app.run(host='0.0.0.0', port=5000)

if __name__ == '__main__':
    # Start Kafka consumer in a separate thread
    consumer_thread = threading.Thread(target=write_to_file)
    consumer_thread.daemon = True #exit tread when program exit
    consumer_thread.start()

    #get data and send produce messages
    try:
        producer = create_producer()
        if producer:
            data = get_product()
            produce_messages(producer, data)
        else:
            print("Producer could not be created.")
    except Exception as e:
        print(f"Error in producing messages: {e}")

    # Start Flask app
    run_flask()