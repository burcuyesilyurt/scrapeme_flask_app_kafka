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
        producer = KafkaProducer(
            bootstrap_servers='localhost:9093',  # Match the advertised listener
            value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize messages to JSON
        )
        return producer
    except Exception as e:
        print(f"Producer creating error: {e}")
        return None

# Function to produce messages to Kafka
def produce_messages(producer, data):
    try:
        for item in data:
            producer.send('my-topic', value=item)
            print(f"Sent message: {item}")
            time.sleep(1)  # Delay between sending messages
        producer.flush()  # Ensure all messages are sent
    except Exception as e:
        print(f"Sending error messages: {e}")

def write_to_file():
    consumer = KafkaConsumer(
        'my-topic',
        bootstrap_servers='localhost:9093',  # Match the advertised listener
        auto_offset_reset='earliest',
        group_id='my-group',
        enable_auto_commit=True,
        auto_commit_interval_ms=5000
    )

    messages = []
    try:
        for message in consumer:
            print(f"Received message: {message.value.decode('utf-8')}")
            messages.append(message.value.decode('utf-8'))

            # Write messages to file
            with open('kafka_data.json', 'w') as file:
                json.dump(messages, file)
    except Exception as e:
        print(f"Consumer loop error: {e}")
    finally:
        consumer.close()

app = Flask(__name__)

# Flask route to get data
@app.route('/data', methods=['GET'])
def get_data():
    try:
        with open('kafka_data.json', 'r') as f:
            data = json.load(f)
        return jsonify(data)
    except Exception as e:
        print(f"Error reading kafka_data.json file: {e}")
        return jsonify({"error": "Failed to read data"}), 500

# Function to run Flask app
def run_flask():
    app.run(host='0.0.0.0', port=5001)

# Main execution
if __name__ == '__main__':
    # Start Kafka consumer in a separate thread
    consumer_thread = threading.Thread(target=write_to_file)
    consumer_thread.daemon = True  # Ensure thread exits when the main program exits
    consumer_thread.start()

    # Produce messages
    try:
        producer = create_producer()
        if producer:
            data = get_product()  # Assuming get_product() is defined elsewhere
            produce_messages(producer, data)
        else:
            print("Producer could not be created.")
    except Exception as e:
        print(f"Error in producing messages: {e}")

    # Start Flask app
    run_flask()