import logging
import random
import sys

from chessnet.server import app


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logging.getLogger("chess.engine").setLevel(logging.DEBUG)
    app.run()
