import pandas as pd


def remove_last_entries_from_dict(d: dict, entries_to_remove: int) -> dict:
    keys_to_remove = list(d.keys())[-entries_to_remove:]
    for key in keys_to_remove:
        d.pop(key)
    return d


def format_dtypes(df: pd.DataFrame, dtypes: dict) -> pd.DataFrame:
    for col in df.columns:
        if dtypes[col] in [float, int]:
            df[col] = pd.to_numeric(df[col])

        if dtypes[col] == dict:
            flat_dicts = df[col].apply(lambda d: pd.Series(unnest_dict(d, {})))  # why are they al the same 
            new_columns = list(flat_dicts.iloc[0].keys())
            df[new_columns] = flat_dicts
            a=1
            
    return df


def unnest_dict(my_dict: dict, existing_dict, level_name="") -> dict:
    # {"pair" : {"token1": "a", "token2": "b"}}
    # {"pair_token1": "a", "pair_token2": "b"}
    #
    # {"pair": "ab"}
    # {"pair": "ab"}
    # 
    # {"pair" : {"id": "0x123..", "token1": {"id": "0x456..", "name": "name xyz"}}}
    # {"pair_id": "0x123..", "pair_token1_id": "0x456..", "pair_token1_name": "name xyz"}
    for k, v in my_dict.items():
        if not isinstance(v, dict):
            existing_dict[f"{level_name}{k}"] = v
        else:
            unnest_dict(v, existing_dict, level_name=f"{level_name}{k}_")
    return existing_dict


if __name__ == "__main__":
    d = {
        "pair": {
            "id": "0x123..", 
            "token1": {
                "id": "0x456..", 
                "name": "name xyz"
                }
                }
                }

    print(unnest_dict(d, {}))
