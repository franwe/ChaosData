from shroomdk import ShroomDK


class FlipsideCrypto:
    def __init__(self, api_key: str):
        self.sdk = ShroomDK(api_key)

    def get_swaps(self, dex: str, pair: str, amount: int = -1):
        pair_address = self.lookup_pair_address(dex, pair)

        query = f"""
        select
        *
        from
        ethereum.core.ez_dex_swaps
        where contract_address = lower('{pair_address}')
        order by block_timestamp desc
        limit {amount}
        """
        data = self.sdk.query(query)
        return data

    def get_syncs(self, dex: str, pair: str, amount: int = -1):
        pair_address = self.lookup_pair_address(dex, pair)

        query = f"""
        select
        BLOCK_NUMBER, BLOCK_TIMESTAMP, TX_HASH, EVENT_INPUTS
        from
        ethereum.core.fact_event_logs
        where contract_address = lower('{pair_address}')
          and event_name = 'Sync'
        order by block_timestamp desc
        limit {amount}
        """

        data = self.sdk.query(query)
        return data




    def lookup_pair_address(self, dex: str, pair: str) -> str:
        # TODO: implement using addresses.json
        # example for uniswap-v2 WBTC-ETH
        return "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"




if __name__ == "__main__":
    
    api_key = "249f0e62-a575-4ddb-8ccd-b7618eb9d766"
    dex = "uniswap-v2"
    pair = "WBTC-ETH"
    amount = 5

    flip = FlipsideCrypto(api_key=api_key)
    swaps = flip.get_swaps(dex=dex, pair=pair, amount=amount)

    syncs = flip.get_syncs(dex=dex, pair=pair, amount=amount)

    print(swaps.records)
    print(syncs.records)