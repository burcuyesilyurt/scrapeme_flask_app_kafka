# Description:

## Project Overview

1. **Web Scraping**: Scrapes data from the website [scrapeme.live](https://scrapeme.live/shop/) and extracts product details.
2. **Kafka Messaging**: Sends scraped data to a Kafka topic.
3. **Data Storage**: Saves Kafka messages to a JSON file.
4. **REST API**: Provides a Flask-based REST API to access the stored data.

## Files

- **Dockerfile**: Contains the instructions to build the Docker image for the Flask application.
- **markdown.dockerignore**: Specifies files and directories to be ignored by Docker when building the image.
- **requirements.txt**: Lists the Python dependencies required for the application.
- **scrape_flask_app.py**: Contains the main application code, including web scraping, Kafka integration, and Flask API.
- **kafka_data.json**: Stores data retrieved from Kafka. This file is updated by the Flask application.
- **scrape_flask_app.ipynb**: Jupyter Notebook with additional code and notes related to the project.
- **docker-compose.yml**: Defines and manages multi-container Docker applications, including Kafka, Zookeeper, and the Flask app.

## Prerequisites

- Docker
- Docker Compose

## Setup and Running

### 1. Clone the Repository

### 2. Build and Start the Containers

```bash
docker-compose up --build
```

This command will build the Docker images and start the services defined in `docker-compose.yml`, including Kafka, Zookeeper, and the Flask application.

### 3. Testing the Application

#### Web Scraping and Kafka

1. The `scrapeme_flask_app.py` script will scrape data from the website and send it to the Kafka topic.
2. Data will be stored in the `kafka_data.json` file.

#### Access the Flask API

- Navigate to `http://localhost:5001/data` in your web browser or use a tool like `curl` or `Postman` to view the data stored in `kafka_data.json`.

```bash
curl http://localhost:5001/data
```

## Troubleshooting

If you encounter an error related to port usage, make sure that port 5001 is not being used by another program. You can change the port in scrape_flask_app.py, Dockerfile, and docker-compose.yml if needed. After making changes, rebuild and restart the Docker containers.
