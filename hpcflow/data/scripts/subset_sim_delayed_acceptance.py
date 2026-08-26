def subset_sim_delayed_acceptance(g, p0):
    g_coarse = g["coarse"]
    g_fine = g["fine"]

    x_0 = g_coarse + (g_fine or 10)
    print(f"{g_coarse=!r}; {g_fine=!r} --> {x_0=!r}")
    return {"x_0": x_0, "p0": p0}
