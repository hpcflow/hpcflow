def main_script_test_direct_in_direct_out_labels(p1):
    # process
    p1_1 = p1["one"]
    p1_2 = p1["two"]
    p2 = p1_1 + p1_2

    # return outputs
    return {"p2": p2}
