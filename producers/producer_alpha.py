from common import publish_loop

if __name__ == "__main__":
    publish_loop("producer-alpha", "event.alpha", "alpha", interval_sec=2.0)
