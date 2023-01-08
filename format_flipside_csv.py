from pathlib import Path
import pandas as pd

def load_events_from_flipside_csv(filename):
    import json
    def parse_string(s):
        d = {}
        try:
            d = json.loads(s)
        except:
            print(s)
        return d


    dfs = pd.read_csv(filename, sep=';')
    dfs.columns = [col.lower() for col in dfs.columns]
    dfs["event_inputs"] = dfs["event_inputs"].apply(lambda s: parse_string(s))
    return dfs


if __name__ == "__main__":
    pair = "USDC-ETH"
    block_number = 16313916
    block_number = 16333882

    filename = Path.cwd().joinpath("data", f"{pair}_{block_number}.csv")

    with open(filename) as file:
        data = file.read()

    new_str = ""
    for line in data.split("\n"):
        parts = line.split(",", 6)
        new_line = ";".join(parts)
        new_str += new_line + "\n"

    filename_new = Path.cwd().joinpath("data", f"{pair}_{block_number}_new.csv")
    with open(filename_new, "wt") as file:
        file.write(new_str)
