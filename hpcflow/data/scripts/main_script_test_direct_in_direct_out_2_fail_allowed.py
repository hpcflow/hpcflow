def main_script_test_direct_in_direct_out_2_fail_allowed(p2):
    # process, accounting for possible unset data:
    p3 = (p2 if p2 is not None else 0) + 100

    # return outputs
    return {"p3": p3}
