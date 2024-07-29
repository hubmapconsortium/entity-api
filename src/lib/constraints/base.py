def build_constraint(ancestor: dict, descendants: list[dict]) -> dict:
    return {
        'ancestors': ancestor,
        'descendants': descendants
    }


def build_constraint_unit(entity, sub_type: list = None, sub_type_val: list = None) -> dict:
    if type(sub_type) is list:
        sub_type.sort()

    if type(sub_type_val) is list:
        sub_type_val.sort()

    constraint: dict = {
        'entity_type': entity,
        'sub_type': sub_type,
        'sub_type_val': sub_type_val
    }
    return constraint


def build_search_constraint_unit(keyword, value) -> dict:
    constraint: dict = {
        'keyword': keyword,
        'value': value
    }
    return constraint

