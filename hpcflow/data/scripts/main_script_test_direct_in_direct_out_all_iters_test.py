def main_script_test_direct_in_direct_out_all_iters_test(p1):
    """Used for a simple loop test, where we pass all iterations to the script."""

    # sum over all iterations:
    all_p1_vals = []
    for key, val in p1.items():
        print(f"\nkey: {key}")
        print(f"loop_idx: {val['loop_idx']}")
        print(f"value: {val['value']}")
        all_p1_vals.append(val["value"])

    p1 = sum(all_p1_vals) + 1

    # return outputs
    return {"p1": p1}
