import logging

# Logging setup for api_pull package programs

logging.basicConfig(
    level=logging.DEBUG,
    format='%(levelname)s - %(asctime)s - %(filename)s - Function:%(funcName)s - %(message)s',
    filename='news_and_dogs_api.log',
    filemode='a'
)

file_handler = logging.FileHandler('api_pull_logs.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter())

root_logger = logging.getLogger()
root_logger.addHandler(file_handler)
