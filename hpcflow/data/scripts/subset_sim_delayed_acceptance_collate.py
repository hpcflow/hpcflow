def subset_sim_delayed_acceptance_collate(p0):
    print(f"subset_sim_delayed_acceptance_collate: {p0=!r}")
    if isinstance(p0, int):
        return {"p1": p0, "p0": p0}
    else:
        return {"p1": sum(p0), "p0": sum(p0)}
