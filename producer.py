from faker import Faker
import argparse
import json
import random
from dataclasses import dataclass
from typing import Dict, Optional, Tuple



TRANSACTION_TYPES = ("PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN")
TYPE_WEIGHTS = {
    "PAYMENT": 50,
    "CASH_IN": 15,
    "CASH_OUT": 15,
    "TRANSFER": 15,
    "DEBIT": 5,
}


@dataclass(frozen=True)
class Account:
    account_id: str
    balance: float


def _weighted_choice(rng: random.Random, weights: Dict[str, int]) -> str:
    items = list(weights.items())
    population = [k for k, _ in items]
    w = [v for _, v in items]
    return rng.choices(population, weights=w, k=1)[0]


def _round_money(value: float) -> float:
    return float(f"{max(0.0, value):.2f}")


def _make_account_id(prefix: str, rng: random.Random) -> str:
    # use only "C" (customer) to keep it simple
    number = rng.randint(1000000000, 9999999999)
    return f"{prefix}{number}"


def _init_accounts(fake: Faker, rng: random.Random, num_accounts: int) -> Dict[str, float]:
    """
    in-memory account balance map.
    """
    balances: Dict[str, float] = {}
    for _ in range(num_accounts):
        acct_id = _make_account_id("C", rng)
        # Skew balances: many small/medium, some large
        starting = rng.triangular(0, 250_000, 5_000) # Triangular gives decent skew.
        balances[acct_id] = _round_money(starting)
    return balances


def _pick_two_distinct_accounts(rng: random.Random, balances: Dict[str, float]) -> Tuple[str, str]:
    keys = list(balances.keys())
    orig = rng.choice(keys)
    dest = rng.choice(keys)
    while dest == orig:
        dest = rng.choice(keys)
    return orig, dest


def _generate_amount(rng: random.Random, txn_type: str, orig_balance: float) -> float:
    """
    Generate amount, constrained by origin balance for debit-like transactions.
    """
    # Baselines by type
    if txn_type == "PAYMENT":
        amount = rng.triangular(1, 2_000, 50)
    elif txn_type == "DEBIT":
        amount = rng.triangular(1, 10_000, 100)
    elif txn_type == "CASH_IN":
        amount = rng.triangular(1, 50_000, 500)
    elif txn_type in ("TRANSFER", "CASH_OUT"):
        amount = rng.triangular(1, 500_000, 1_000)
    else:
        amount = rng.triangular(1, 10_000, 100)

    #  operations that reduce origin balance, clamp to available funds sometimes
    reduces_origin = txn_type in ("PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT")
    if reduces_origin and orig_balance > 0:
        # 90% of time, keep it <= balance; 10% allow weird cases (data quality / overdraft)
        if rng.random() < 0.90:
            amount = min(amount, orig_balance)
    return _round_money(amount)


def _fraud_probability(txn_type: str, amount: float) -> float:
    """
    Base fraud likelihood. 
    """
    base = 0.10

    # Fraud tends to concentrate in TRANSFER/CASH_OUT
    if txn_type == "TRANSFER":
        base *= 2.0
    elif txn_type == "CASH_OUT":
        base *= 1.7
    elif txn_type == "PAYMENT":
        base *= 0.7

    # Large transactions are riskier
    if amount >= 200_000:
        base *= 6
    elif amount >= 50_000:
        base *= 2.5
    elif amount >= 10_000:
        base *= 1.5

    # Cap to avoid ridiculous values
    return min(base, 0.5)

def generate_event(
    rng: random.Random,
    balances: Dict[str, float],
    step: int,
) -> Dict[str, object]:
    txn_type = _weighted_choice(rng, TYPE_WEIGHTS)

    name_orig, name_dest = _pick_two_distinct_accounts(rng, balances)
    old_org = balances[name_orig]
    old_dest = balances[name_dest]

    amount = _generate_amount(rng, txn_type, old_org)

    # determine fraud + flagged fraud
    p_fraud = _fraud_probability(txn_type, amount)
    is_fraud = 1 if rng.random() < p_fraud else 0

    # transactions >200,000 are flagged for suspicious"
    is_flagged = 1 if (txn_type == "TRANSFER" and amount > 200_000) else 0

    # Apply balance updates.
    reduces_origin = txn_type in ("PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT")
    increases_origin = txn_type == "CASH_IN"

    # Origin updates
    new_org = old_org
    if reduces_origin:
        new_org = _round_money(old_org - amount)
    elif increases_origin:
        new_org = _round_money(old_org + amount)

    # Destination updates
    new_dest = old_dest
    affects_dest = txn_type in ("TRANSFER", "CASH_IN")
    if affects_dest:
        if is_fraud == 1 and txn_type == "TRANSFER":
            # fraud: origin loses money but dest doesn't reliably get it
            # 70% dest unchanged, 30% dest gets it anyway
            if rng.random() < 0.70:
                new_dest = old_dest
            else:
                new_dest = _round_money(old_dest + amount)
        else:
            new_dest = _round_money(old_dest + amount)

    # Persist balances for next events (stateful generator)
    balances[name_orig] = new_org
    balances[name_dest] = new_dest

    return {
        "step": step,
        "type": txn_type,
        "amount": amount,
        "nameOrig": name_orig,
        "oldbalanceOrg": _round_money(old_org),
        "newbalanceOrig": _round_money(new_org),
        "nameDest": name_dest,
        "oldbalanceDest": _round_money(old_dest),
        "newbalanceDest": _round_money(new_dest),
        "isFraud": is_fraud,
        "isFlaggedFraud": is_flagged,
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate fraud transaction events")
    p.add_argument("--count", type=int, default=500, help="number of events to generate")
    p.add_argument("--max-step", type=int, default=743, help="how many hours of data")
    p.add_argument("--accounts", type=int, default=5000, help="how many accounts to maintain balances for")
    p.add_argument("--seed", type=int, default=None, help="Random seed")
    p.add_argument("--out", type=str, default="-", help="output file path. '-' for stdout")
    p.add_argument("--format", choices=("jsonl", "csv"), default="jsonl", help="Output format.")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    rng = random.Random(args.seed)
    fake = Faker()
    if args.seed is not None:
        Faker.seed(args.seed)

    balances = _init_accounts(fake=fake, rng=rng, num_accounts=args.accounts)

    # Steps cycle through 1 to max-step
    steps = [(i % args.max_step) + 1 for i in range(args.count)]

    if args.out == "-":
        fh = None
    else:
        fh = open(args.out, "w", encoding="utf-8", newline="")

    try:
        if args.format == "jsonl":
            for step in steps:
                event = generate_event(rng=rng, balances=balances, step=step)
                line = json.dumps(event, separators=(",", ":"), ensure_ascii=False)
                if fh:
                    fh.write(line + "\n")
                else:
                    print(line)
        else:  # CSV but idk if we are gonna use that
            header = [
                "step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
                "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud"
            ]
            if fh:
                fh.write(",".join(header) + "\n")
            else:
                print(",".join(header))

            for step in steps:
                e = generate_event(rng=rng, balances=balances, step=step)
                row = [
                    str(e["step"]),
                    str(e["type"]),
                    f'{e["amount"]:.2f}',
                    str(e["nameOrig"]),
                    f'{e["oldbalanceOrg"]:.2f}',
                    f'{e["newbalanceOrig"]:.2f}',
                    str(e["nameDest"]),
                    f'{e["oldbalanceDest"]:.2f}',
                    f'{e["newbalanceDest"]:.2f}',
                    str(e["isFraud"]),
                    str(e["isFlaggedFraud"]),
                ]
                line = ",".join(row)
                if fh:
                    fh.write(line + "\n")
                else:
                    print(line)
    finally:
        if fh:
            fh.close()


if __name__ == "__main__":
    main()
