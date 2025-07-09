# ibkrAPI

This Python script fetches daily Open, High, Low, and Close (OHLC) data for every symbol listed on the NASDAQ and other exchanges using the Interactive Brokers API (ibkrAPI). The script is optimized to efficiently manage multiple concurrent requests by leveraging threading, semaphores, and a custom await mechanism, ensuring that each request is accurately tracked and matched with its corresponding sent reqID.

## Features

- Fetches daily OHLC data for all symbols listed on the NASDAQ and other exchanges.
- Utilizes the ibkrAPI to interact with the Interactive Brokers platform.
- Employs threading to handle multiple requests concurrently.
- Uses a semaphore to limit the number of simultaneous API requests.
- Implements a custom await mechanism to ensure all requested data is received before proceeding.
