#!/usr/bin/env python3
"""
Authentication token generation script for PyAleph.

This script provides functionality to:
1. Generate new ECDSA key pairs
2. Create authentication tokens using private keys
"""

import argparse
import getpass
import sys
from pathlib import Path

# Add src directory to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

from aleph.toolkit.ecdsa import generate_key_pair, create_auth_token, generate_key_pair_from_private_key


def generate_new_keypair():
    """Generate and display a new ECDSA key pair."""
    print("Generating new ECDSA key pair...")
    private_key, public_key = generate_key_pair()
    
    print(f"\nPrivate Key (keep secure!): {private_key}")
    print(f"Public Key: {public_key}")
    
    return private_key, public_key


def read_keypair():
    """Create an authentication token from a private key."""
    print("Enter your private key in hex format:")
    private_key_hex = getpass.getpass("Private Key: ").strip()
    
    if not private_key_hex:
        print("Error: Private key cannot be empty")
        return None

    private_key, public_key = generate_key_pair_from_private_key(private_key_hex)

    # print(f"\nPrivate Key (keep secure!): {private_key}")
    # print(f"Public Key: {public_key}")

    return private_key, public_key


def main():
    parser = argparse.ArgumentParser(
        description="PyAleph Authentication Token Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate a new key pair
  python generate_auth_token.py --generate-keys
  
  # Create token from existing private key
  python generate_auth_token.py --create-token
  
  # Do both operations
  python generate_auth_token.py --generate-keys --create-token
        """
    )
    
    parser.add_argument(
        '--generate-keys', '-g',
        action='store_true',
        help='Generate a new ECDSA key pair'
    )
    
    parser.add_argument(
        '--create-token', '-t',
        action='store_true',
        help='Create an authentication token from a private key'
    )
    
    args = parser.parse_args()
    
    if not args.generate_keys and not args.create_token:
        parser.print_help()
        return
    
    private_key = None
    
    if args.generate_keys:
        private_key, _ = generate_new_keypair()
    
    if args.create_token:
        if private_key is None:
            private_key, _ = read_keypair()

        try:
            token = create_auth_token(private_key)
            print(f"\nGenerated Token: {token}")
        except Exception as e:
            print(f"Error creating token: {e}")


if __name__ == "__main__":
    main()