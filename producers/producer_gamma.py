from common import publish_loop

if __name__ == "__main__":
    publish_loop("producer-gamma", "event.gamma", "gamma", interval_sec=4.0)
