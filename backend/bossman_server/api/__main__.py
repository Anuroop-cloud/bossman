"""Run the BOSSman API:  python -m bossman_server.api   (same as `bossmand serve`)."""

import sys

from bossman_server.cli import main

if __name__ == "__main__":
    sys.exit(main(["serve", *sys.argv[1:]]))
