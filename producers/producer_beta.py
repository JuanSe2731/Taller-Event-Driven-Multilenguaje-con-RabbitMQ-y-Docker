from common import publish_loop

if __name__ == "__main__":
    publish_loop("producer-beta", "event.beta", "beta", interval_sec=3.0)
