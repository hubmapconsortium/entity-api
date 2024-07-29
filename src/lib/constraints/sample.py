from lib.constraints.base import build_constraint, build_constraint_unit
# can be the descendant of / --->

def build_sample_organ_constraints(entity, constraints=None):
    if constraints is None:
        constraints = []

    # Donor ---> Sample organ
    ancestor = build_constraint_unit('donor')
    descendant = build_constraint_unit(entity, ['Organ'])
    constraints.append(build_constraint(ancestor, [descendant]))

    return constraints


def build_all_sample_constraints(entity):
    constraints = build_sample_organ_constraints(entity)
    return constraints
