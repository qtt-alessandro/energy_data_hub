import logging
from datetime import datetime

# Configure logging with append mode
logging.basicConfig(
    filename='app.log',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Your original code with logging
try:
    logging.info("Starting script execution")
    print("say something...")
    logging.info("Script completed successfully")
except Exception as e:
    logging.error(f"An error occurred: {str(e)}")