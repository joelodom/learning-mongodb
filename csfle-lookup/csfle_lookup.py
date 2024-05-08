"""
This is a toy program to experiment with $lookup and client-side-field-level
encryption in MongoDB.

Author: Joel Odom
"""

import argparse

def create_parser():
    parser = argparse.ArgumentParser(
        description=
        "A toy program to experiment wtih CSFLE and encryption in MongoDB.")
    
    group = parser.add_mutually_exclusive_group()

    group.add_argument("--setup-database",
                        action="store_true",
                        help="initializes the database before a first run")

    group.add_argument("--destroy-database",
                        action="store_true",
                        help="destroys the database")

    group.add_argument("--demonstrate-without-encryption",
                        action="store_true",
                        help="run through some lookups without encryption")

    group.add_argument("--demonstrate-with-encryption",
                        action="store_true",
                        help="shows how encryption should work (but doesn't)")

    return parser

def main():
    parser = create_parser()
    args = parser.parse_args()
    
    if args.setup_database:
        print("Setting up database before first use...")
    elif args.setup_database:
        print("Destroying the database...")
    elif args.demonstrate_without_encryption:
        print("Showing demonstration without encryption...")
    elif args.demonstrate_with_encryption:
        print("Showing demonstration with encryption (which will fail)...")
    else:
        parser.print_help()
    
    print()

if __name__ == "__main__":
    main()
