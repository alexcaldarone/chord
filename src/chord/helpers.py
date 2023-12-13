def is_between(id: int,
               lower_bound: int,
               upper_bound: int,
               k: int,
               include_upper: bool = False,
               include_lower: bool = False):
    
    if lower_bound <= upper_bound:
        return is_between_normal(id, lower_bound, upper_bound,
                                 include_lower, include_upper)
    else:
        return is_between_reverse(id, lower_bound, upper_bound, k)

def is_between_normal(id: int,
                      lower_bound: int,
                      upper_bound: int,
                      include_lower: bool = False,
                      include_upper: bool = False):
    if include_upper and include_lower:
            return lower_bound <= id and id <= upper_bound
    elif include_upper and not include_lower:
        return lower_bound < id and id <= upper_bound
    elif not include_upper and include_lower:
        return lower_bound <= id and id < upper_bound
    else:
        return lower_bound < id and id < upper_bound

def is_between_reverse(id: int,
                       lower_bound: int,
                       upper_bound: int,
                       k: int):
    assert lower_bound >= upper_bound

    res = is_between_normal(id, 0, upper_bound, include_lower=True) or \
        is_between_normal(id, lower_bound, k, include_lower=True)
    
    return res