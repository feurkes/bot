import logging # NOTE NEXT UPDATE NOTE 

logger = logging.getLogger("steam_rental")
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = True
logger.setLevel(logging.DEBUG)
