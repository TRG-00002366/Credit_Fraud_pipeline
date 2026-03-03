import random
import time
from datetime import datetime
from faker import Faker

fake = Faker()


MERCHANTS = [
    "Amazon",
    "Walmart",
    "Target",
    "BestBuy",
    "Apple",
    "Uber",
    "Netflix",
    "Airbnb"
]

CATEGORIES = [
    "Shopping",
    "Travel",
    "Food",
    "Electronics",
    "Entertainment"
]

TRANSACTION_TYPES = ["ONLINE", "POS", "ATM"]

def generate_transaction():

    transaction = {
        "transaction_id": "TXN-" + str(random.randint(10000, 99999)),
        "card_id": "CARD-" + str(random.randint(1000, 9999)),
        "merchant": random.choice(MERCHANTS),
        "category": random.choice(CATEGORIES),
        "amount": round(random.uniform(10, 5000), 2),
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "location": fake.city(),
        "transaction_time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "is_international": random.choice([True, False])
    }

    return transaction


if __name__ == "__main__":

    print("Starting Credit Card Transaction Generator...\n")

    while True:
        txn = generate_transaction()
        print(txn)

        time.sleep(1)