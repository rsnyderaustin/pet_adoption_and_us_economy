import logging

# Logging setup for api_pull package programs

log_format = '%(levelname)s - %(asctime)s - %(filename)s - Function:%(funcName)s -\n    %(message)s'

formatter = logging.Formatter(log_format)

logging.basicConfig(
    level=logging.DEBUG,
    format=log_format,
    filename='news_and_dogs_api.log',
    filemode='a'
)

file_handler = logging.FileHandler('api_pull_logs.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

root_logger = logging.getLogger()
root_logger.addHandler(file_handler)
