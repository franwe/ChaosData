from abc import ABC, abstractmethod
import requests
from src.misc.data import remove_last_entries_from_dict


class TheGraphAPI(ABC):
    def __init__(self):
        self.url = "https://api.thegraph.com/subgraphs/name"

    @property
    def endpoint(self):
        raise NotImplementedError

    def _send_request(self, query):
        url = f"{self.url}/{self.endpoint}"
        r = requests.post(url, json={'query': query})
        data = r.json()        
        return data

    @abstractmethod
    def send_request(self, query):
        # use self._send_request and check for errors
        raise NotImplementedError


class GraphUniswapV3(TheGraphAPI):
    endpoint = "uniswap/uniswap-v2"
    column_dtypes = {
        "amount0In": float, 
        "amount0Out": float, 
        "amount1In": float, 
        "amount1Out": float, 
        "id": str, 
        "sender": str, 
        "to": str, 
        "timestamp": int, 
        "reserve0": float,
        "reserve1": float,
        "blockNumber":int,
        "pair": dict, 
        "transaction": dict,
    }

    def send_request(self, query):
        data = self._send_request(query)
        if "data" in data:
            return data["data"]

        if "errors" in data:
            for error in data["errors"]:
                print(f"error_message: {error['message']}")
            raise ValueError
        raise NotImplementedError

    def get_swaps(self, pair: str, ts_gte: int, amount: int = -1):
        # pair: "WBTC-ETH"
        # ts_gte: timestamp in sec, where to start from 
        # amount: how many to request, if -1: get until today

        blocknr_gte = self.get_blocknr_at_timestamp(ts_gte)
        request_amount = 100
        all_swaps = {}
        
        do_continue = True  # set to false if reached amount or got empty request
        while do_continue:
            swaps = self._get_swaps_gte_blocknr(pair, blocknr_gte, amount=request_amount)
            curr_swaps = {s["id"]: s for s in swaps}
            all_swaps.update(curr_swaps)  # if got swap-id twice because overlap of blocknr, only keep one
            blocknr_gte = int(swaps[-1]["transaction"]["blockNumber"])

            if len(swaps) < request_amount:
                do_continue = False
            if (amount > 0) and (len(all_swaps) >= amount): 
                do_continue = False
                if len(all_swaps) > amount:
                    entries_to_remove = len(all_swaps) - amount
                    all_swaps = remove_last_entries_from_dict(all_swaps, entries_to_remove)

        return [s for s in all_swaps.values()]

    def _get_swaps_gte_blocknr(self, pair: str, blocknr_gte: int, amount: int=100):
        # pair: "WBTC-ETH"
        # blocknr_gte: timestamp in sec, where to start from 
        # amount: how many to request
        # ordered by timestamp (asc), starting at blocknr_gte where pair_address

        pair_address = self.lookup_pair_address("WBTC-ETH")

        query = f"""
                {{
                swaps(
                    first: {amount}
                    orderBy: timestamp
                    orderDirection: asc
                    where: {{pair_: {{id: "{pair_address}"}} _change_block: {{number_gte: {blocknr_gte} }} }}
                ) {{
                    amount0In
                    amount0Out
                    amount1In
                    amount1Out
                    id
                    sender
                    to
                    timestamp
                    pair {{
                    token0Price
                    token1Price
                    token0 {{
                        symbol
                        decimals
                    }}
                    token1 {{
                        symbol
                        decimals
                    }}
                    reserve1
                    reserve0
                    }}
                    transaction {{
                        blockNumber
                    }}
                }}
            }}
            """

        d = self.send_request(query)
        return d["swaps"]

    def get_blocknr_at_timestamp(self, ts: int) -> int:
        query = f"""
        {{
                transactions(first: 1, where: {{timestamp_gte: \"{ts}\"}}, orderBy: timestamp) 
                {{
                    id
                    blockNumber
                    timestamp
                }}
        }}"""
        d = self.send_request(query)
        return int(d["transactions"][0]["blockNumber"])

    
    def lookup_pair_address(self, pair: str) -> str:
        # TODO: implement using addresses.json
        # example for WBTC-ETH
        return "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"


if __name__ == "__main__":

    uniswap_graph = GraphUniswapV3()
    ts = 1672834013

    blocknr = uniswap_graph.get_blocknr_at_timestamp(ts)
    print(f"\nBlockNr at timestamp {ts}: {blocknr}")

    swaps = uniswap_graph.get_swaps(pair="WBTC-ETH", ts_gte=ts, amount=110)
    print(f"\nGot {len(swaps)} swaps. \nExample:")
    print(swaps[0])
