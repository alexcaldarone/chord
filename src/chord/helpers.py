def is_between(id: int,
               upper_bound: int,
               lower_bound: int,
               include_upper: bool = False,
               include_lower: bool = False):
    
    if include_upper and include_lower:
        return lower_bound <= id and id <= upper_bound
    elif include_upper and not include_lower:
        return lower_bound < id and id <= upper_bound
    elif not include_upper and include_lower:
        return lower_bound <= id and id < upper_bound
    else:
        return lower_bound < id and id < upper_bound