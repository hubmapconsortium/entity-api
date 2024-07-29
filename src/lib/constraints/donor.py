from lib.constraints.base import build_constraint, build_constraint_unit

# can be the descendant of / --->
def build_all_donor_constraints(entity):

    # Sample organ ---> Donor
    ancestor = build_constraint_unit(entity)
    descendant = build_constraint_unit('sample', 'Organ')

    return [
        build_constraint(ancestor, [descendant])
    ]