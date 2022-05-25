import abc


class ProducerMiddleware(abc.ABC):
    def process_before_produce(self, message: dict) -> dict:
        return message

    def process_after_produce(self, message: dict):
        pass
