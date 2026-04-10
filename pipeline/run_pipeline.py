import argparse

from config.retailers import RETAILER_CONFIG
from pipeline.run_a101_pipeline import run_pipeline as run_a101_pipeline
from pipeline.run_migros_pipeline import run_pipeline as run_migros_pipeline


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--retailer", required=True)
    parser.add_argument("--category", required=True)
    args = parser.parse_args()

    if args.retailer not in RETAILER_CONFIG:
        raise ValueError(f"Unknown retailer: {args.retailer}")

    if args.retailer == "migros":
        run_migros_pipeline(args.category)
    elif args.retailer == "a101":
        run_a101_pipeline(args.category)
    else:
        raise ValueError(f"Retailer not implemented yet: {args.retailer}")


if __name__ == "__main__":
    main()
