import lo

def get_logging_message(message_name, **kwargs):
    try:
        logging_message = error_messages.get(message_name)
    except KeyError:
        logging_message = format_message(message_name='get_error_message',
                                         params=kwargs)
