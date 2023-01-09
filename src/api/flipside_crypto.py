import time
from pathlib import Path
from shroomdk import ShroomDK
from shroomdk.errors import QueryRunRateLimitError
from shroomdk.errors import QueryRunTimeoutError

from datetime import datetime
from src.misc.data import remove_last_entries_from_dict, load_json

ADDRESSES_FILE = Path.cwd().joinpath("src", "configs", "addresses.json")

class FlipsideCrypto:
    def __init__(self, api_key: str):
        self.sdk = ShroomDK(api_key)
        self.addresses = load_json(ADDRESSES_FILE)
        self._queries_in_last_5_min = []
        self._max_queries_per_5_min = 250

    def save_send_query(self, query, timeout_minutes=1):

        if len(self._queries_in_last_5_min) > self._max_queries_per_5_min*0.9:
            time_of_10th_query = self._queries_in_last_5_min[0]
            wait_seconds = time.time() - time_of_10th_query
            print(f"Almost reached max queries for 5 minutes, wait {wait_seconds}.")
            time.sleep(wait_seconds)
            self._queries_in_last_5_min = [ts for ts in self._queries_in_last_5_min if ts > time.time() - 5*60]

        self._queries_in_last_5_min.append(time.time())

        try:
            data = self.sdk.query(query, timeout_minutes=timeout_minutes)
        except QueryRunRateLimitError as e:
            print(f"You have been rate limited: {e.message}")
            raise Exception("Flipside Rate Limit reached.")
        except QueryRunTimeoutError as e:
            print(f"Your query has taken longer than {timeout_minutes} minutes to run: {e.message}")
            raise TimeoutError
        except Exception as e:
            raise Exception(f"Flipside request failed. {e}")

        return data

    def get_syncs_and_swaps(self, dex: str, pair: str, ts_gte: int, amount: int = -1, request_amount: int = 100_000):
        # sync-swap events
        # want to know the reserves before the swap
        # need to check that sync-swap have same tx_hash and event_index(sync) = event_index(swap) - 1
        # request_amount default to 100_000: All query runs can return a maximum of 1,000,000 rows 
        #                                    and a maximum of 100k records can be returned in a single page.
        
        blocknr_gte = self.get_blocknr_at_timestamp(ts_gte)
        all_logs = {}
        
        do_continue = True  # set to false if reached amount or got empty request
        while do_continue:
            logs = self._get_syncs_and_swaps_at_block_nr(dex, pair, blocknr_gte, request_amount)
            curr_logs = {log["_log_id"]: log for log in logs}
            all_logs.update(curr_logs)
            blocknr_gte = logs[-1]["block_number"]

            if len(curr_logs) < request_amount:
                # reached end of data
                do_continue = False
            if (amount > 0) and (len(all_logs) >= amount): 
                do_continue = False
                if len(all_logs) > amount:
                    entries_to_remove = len(all_logs) - amount
                    all_logs = remove_last_entries_from_dict(all_logs, entries_to_remove)
        
        return [log for log in all_logs.values()]

    def _get_syncs_and_swaps_at_block_nr(self, dex: str, pair: str,  blocknr_gte: int, amount: int = 1000):

        pair_address = self.lookup_pair_address(dex, pair)

        query = f"""
        select
            _log_id,
            block_number,
            block_timestamp,
            tx_hash,
            event_index,
            event_name,
            event_inputs
        from
            ethereum.core.fact_event_logs
        where contract_address = lower('{pair_address}')
          and block_number > {blocknr_gte}
          and (
              event_name = 'Sync'
              or event_name = 'Swap'
          )
        order by block_number ASC, event_index ASC
        limit {amount}
        """

        data = self.save_send_query(query)
        return data.records

    def lookup_pair_address(self, dex: str, pair: str) -> str:
        if (dex in self.addresses) and (pair in self.addresses[dex]):
            return self.addresses[dex][pair]
        else:
            print(f"Address unknown for {dex} {pair}. Check file {ADDRESSES_FILE}.")
            raise ValueError

    def get_blocknr_at_timestamp(self, ts_ge):
        
        time_str = self._timestamp_to_str(ts_ge)

        query = f"""
        select
            block_number, block_timestamp
        from
            ethereum.core.fact_blocks
        where
            block_timestamp > '{time_str}'
        order by block_number ASC
        limit 1
        """

        data = self.save_send_query(query)
        return data.records[0]["block_number"]

    @staticmethod
    def _timestamp_to_str(ts):
        return datetime.fromtimestamp(ts).isoformat().replace("T", " ")


if __name__ == "__main__":
    
    api_key = "249f0e62-a575-4ddb-8ccd-b7618eb9d766"
    dex = "uniswap-v2"
    pair = "WBTC-ETH"
    amount = 1500

    flip = FlipsideCrypto(api_key=api_key)
    ts = 1672834013

    blocknr = flip.get_blocknr_at_timestamp(ts)
    print(f"\nBlockNr at timestamp {ts}: {blocknr}")

    syncs_and_swaps = flip.get_syncs_and_swaps(dex=dex, pair=pair, ts_gte=ts, amount=amount)
    print(f"\nGot {len(syncs_and_swaps)} swaps. \nExample:")
    print(syncs_and_swaps[0])