def main_script_test_direct_in_direct_out_2_fail_allowed_group(p2):
    # process, accounting for possible unset data:
    p2_sum = sum(i for i in p2 if i is not None)
    p3 = p2_sum + 100

    # return outputs
    return {"p3": p3}
