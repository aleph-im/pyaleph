import asyncio
import aioipfs
import asyncpg
import argparse
import logging
import os

# Configure logging to provide clear output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DATABASE_DSN="postgres://USERNAME:PASSWORD@localhost:8080/aleph"
IPFS_API="'/ip4/127.0.0.1/tcp/5001'"

async def get_ipfs_pins(api_addr: str) -> set:
    """
    Connects to an IPFS instance and retrieves a set of recursively pinned CIDs.

    Args:
        api_addr: The multiaddress of the IPFS API endpoint (e.g., '/ip4/127.0.0.1/tcp/5001').

    Returns:
        A set of recursively pinned CIDs (as strings).
    """
    logging.info(f"Connecting to IPFS API at {api_addr}...")
    client = None
    try:
        client = aioipfs.AsyncIPFS(maddr=api_addr)
        pins = set()
        # The 'type' argument filters for recursive pins directly in the API call.
        # The result is an async generator, so we iterate through it.
        pin_list = await client.pin.ls(pintype='recursive', quiet=True)
        pinned = list(pin_list['Keys'].keys())
        for pin in pinned:
            pins.add(pin)
        logging.info(f"Found {len(pins)} recursively pinned files in IPFS.")
        return pins
    except Exception as e:
        logging.error(f"Failed to connect or retrieve pins from IPFS: {e}")
        return set()
    finally:
        if client:
            await client.close()
            logging.info("IPFS client connection closed.")


async def get_database_hashes(dsn: str) -> set:
    """
    Connects to a PostgreSQL database and retrieves a set of file hashes that should be pinned.

    Args:
        dsn: The PostgreSQL connection string.

    Returns:
        A set of file hashes (as strings) that should be pinned.
    """
    logging.info("Connecting to PostgreSQL database...")
    conn = None
    try:
        conn = await asyncpg.connect(dsn)
        # The query provided by the user
        # query = """
        #         SELECT f.hash FROM file_pins fp
        #                                INNER JOIN files f ON f.hash = fp.file_hash
        #                                INNER JOIN messages m ON m.item_hash = fp.item_hash
        #         WHERE m."type" = 'STORE' and m."content"->>'item_type' = 'ipfs' \
        #         """
        query = """
                SELECT f.hash FROM files f
                WHERE f.hash like 'Qm%' or f.hash like 'bafkrei%' \
                """
        rows = await conn.fetch(query)
        hashes = {row['hash'] for row in rows}
        logging.info(f"Found {len(hashes)} files that should be pinned in the database.")
        return hashes
    except Exception as e:
        logging.error(f"Failed to connect or query the database: {e}")
        return set()
    finally:
        if conn:
            await conn.close()
            logging.info("Database connection closed.")


async def unpin_files(api_addr: str, cids_to_unpin: list):
    """
    Removes pins for a given list of CIDs from the IPFS node.

    Args:
        api_addr: The multiaddress of the IPFS API endpoint.
        cids_to_unpin: A list of CID strings to unpin.
    """
    if not cids_to_unpin:
        logging.info("No files to unpin.")
        return

    logging.info(f"Connecting to IPFS API at {api_addr} to unpin files...")
    client = None
    try:
        client = aioipfs.AsyncIPFS(maddr=api_addr)
        for cid in cids_to_unpin:
            try:
                logging.warning(f"Unpinning {cid}...")
                await client.pin.rm(cid)
                logging.info(f"Successfully unpinned {cid}.")
            except Exception as e:
                logging.error(f"Failed to unpin {cid}: {e}")
    except Exception as e:
        logging.error(f"Failed to connect to IPFS for unpinning: {e}")
    finally:
        if client:
            await client.close()
            logging.info("IPFS client connection closed after unpinning.")

async def pin_files(api_addr: str, cids_to_pin: list):
    """
    Pins a given list of CIDs to the IPFS node.

    Args:
        api_addr: The multiaddress of the IPFS API endpoint.
        cids_to_pin: A list of CID strings to pin.
    """
    if not cids_to_pin:
        logging.info("No files to pin.")
        return

    logging.info(f"Connecting to IPFS API at {api_addr} to pin files...")
    client = None
    try:
        client = aioipfs.AsyncIPFS(maddr=api_addr)
        for cid in cids_to_pin:
            try:
                logging.info(f"Pinning {cid}...")
                # The 'add' method pins recursively by default
                async for cid_pin in client.pin.add(cid):
                    print('Pin progress', cid_pin['Progress'])
                logging.info(f"Successfully pinned {cid}.")
            except Exception as e:
                logging.error(f"Failed to pin {cid}: {e}")
    except Exception as e:
        logging.error(f"Failed to connect to IPFS for pinning: {e}")
    finally:
        if client:
            await client.close()
            logging.info("IPFS client connection closed after pinning.")


async def main():
    """
    Main function to orchestrate the IPFS pin synchronization process.
    """
    parser = argparse.ArgumentParser(
        description="Compares IPFS pins with a database record and optionally syncs the state."
    )
    # IPFS arguments
    parser.add_argument(
        '--ipfs-api',
        default=os.getenv('IPFS_API', IPFS_API),
        help="IPFS API multiaddress (default: /ip4/127.0.0.1/tcp/5001)"
    )
    # PostgreSQL arguments from environment variables for security
    parser.add_argument(
        '--db-dsn',
        default=os.getenv('DATABASE_DSN', DATABASE_DSN),
        help="PostgreSQL DSN (e.g., 'postgres://user:pass@host:port/dbname'). "
             "Can also be set via DATABASE_DSN environment variable."
    )
    # Action arguments
    parser.add_argument(
        '--unpin',
        action='store_true',
        help="Actually perform the unpinning of files. Default is a dry run."
    )
    parser.add_argument(
        '--pin',
        action='store_true',
        help="Actually perform the pinning of missing files. Default is a dry run."
    )
    args = parser.parse_args()

    if not args.db_dsn:
        logging.error("Database DSN must be provided via --db-dsn argument or DATABASE_DSN environment variable.")
        return

    # Get the two sets of hashes/CIDs
    ipfs_pins = await get_ipfs_pins(args.ipfs_api)
    db_hashes = await get_database_hashes(args.db_dsn)

    if not ipfs_pins and not db_hashes:
        logging.warning("Both IPFS and database checks returned empty sets. Exiting.")
        return

    # --- 1. Check for files in IPFS that should be UNPINNED ---
    pins_to_remove = ipfs_pins - db_hashes

    if not pins_to_remove:
        logging.info("All pinned files are correctly referenced in the database.")
    else:
        logging.warning(f"Found {len(pins_to_remove)} files to UNPIN (in IPFS, not in DB):")
        for cid in pins_to_remove:
            print(f"  - {cid}")

        if args.unpin:
            logging.info("--- UNPINNING ENABLED ---")
            await unpin_files(args.ipfs_api, list(pins_to_remove))
            logging.info("--- UNPINNING PROCESS COMPLETE ---")
        else:
            logging.info("-> This was a dry run. Use --unpin to remove them.")

    print("-" * 50)

    # --- 2. Check for files in DB that should be PINNED ---
    hashes_to_add = db_hashes - ipfs_pins

    if not hashes_to_add:
        logging.info("All necessary files from the database are already pinned in IPFS.")
    else:
        for cid in hashes_to_add:
            print(f"  + {cid}")

        if args.pin:
            logging.info("--- PINNING ENABLED ---")
            await pin_files(args.ipfs_api, list(hashes_to_add))
            logging.info("--- PINNING PROCESS COMPLETE ---")
        else:
            logging.info("-> This was a dry run. Use --pin to add them.")

    logging.warning(f"Found {len(pins_to_remove)} files to UNPIN (in IPFS, not in DB):")
    logging.warning(f"Found {len(hashes_to_add)} files to PIN (in DB, not in IPFS):")


if __name__ == "__main__":
    asyncio.run(main())
