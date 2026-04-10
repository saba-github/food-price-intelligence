import argparse

from pipeline.run_migros_pipeline import run_pipeline as run_migros_pipeline


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--retailer", required=True)
    parser.add_argument("--category", required=True)
    args = parser.parse_args()

    if args.retailer == "migros":
        run_migros_pipeline(args.category)
    else:
        raise ValueError(f"Unsupported retailer: {args.retailer}")


if __name__ == "__main__":
    main()
