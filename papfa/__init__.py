"""Top-level package for papfa."""

from .consumers import consumer
from .producers import get_message_producer
from .settings import Papfa

__all__ = ["consumer", "get_message_producer", "Papfa"]

__author__ = """Kamyab Zareh"""
__email__ = "kamyab.zareh@gmail.com"
__version__ = '0.1.5'
