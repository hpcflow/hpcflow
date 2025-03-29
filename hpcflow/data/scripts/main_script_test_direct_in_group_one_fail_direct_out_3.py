def main_script_test_direct_in_group_one_fail_direct_out_3(p3):
    # process, ignore some un-set inputs:
    p4 = sum([i for i in p3 if i is not None]) + 100

    # return outputs
    return {"p4": p4}
