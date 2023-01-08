def calc_price_from_reserves(reserve0, token0_decimals, reserve1, token1_decimals):
    return float((reserve0 / token0_decimals) / (reserve1 / token1_decimals))
    
def calc_slippage(price_before, price_after):
    return abs((price_before - price_after) / price_before)
