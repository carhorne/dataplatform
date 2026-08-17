# utils/env_loader.py
import os
import argparse
from dotenv import load_dotenv

def load_environment():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=None, help="Path to .env file")
    args, _ = parser.parse_known_args()

    if args.env:
        if not os.path.exists(args.env):
            raise FileNotFoundError(f".env file not found at {args.env}")
        load_dotenv(dotenv_path=args.env)
    else:
        load_dotenv()  # loads .env from cwd if present, no-op if absent

