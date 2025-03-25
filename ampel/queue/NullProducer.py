from collections.abc import Callable

from ampel.queue.AbsProducer import AbsProducer


class NullProducer(AbsProducer):
    """
    A producer that does nothing.
    """

    def produce(
        self, item: AbsProducer.Item, delivery_callback: None | Callable[[], None]
    ) -> None:
        if delivery_callback:
            delivery_callback()

    def __exit__(self, exc_type, exc_val, exc_tb):
        return None