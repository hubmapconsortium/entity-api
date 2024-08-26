from deepdiff import DeepDiff

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

# can be the descendant of / --->
def build_all_donor_constraints(entity):

    # Sample organ ---> Donor
    ancestor = build_constraint_unit(entity)
    descendant = build_constraint_unit('sample', ['Organ'])

    return [
        build_constraint(ancestor, [descendant])
    ]

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

def build_donor_constraints(entity) -> list:
    return build_all_donor_constraints(entity)


def build_sample_constraints(entity) -> list:
    return build_all_sample_constraints(entity)


def enum_val_lower(member):
        return member.value.lower()

def determine_constraint_from_entity(constraint_unit) -> dict:
    entity_type = constraint_unit.get('entity_type', '')
    entity_type = entity_type.lower()
    sub_type = constraint_unit.get('sub_type')
    error = None
    constraints = []
    entities = ['dataset', 'sample', 'donor', 'publication', 'upload']

    if entity_type not in entities:
        error = f"No `entity_type` found with value `{entity_type}`"
    else:
        try:
            _sub_type = f"{sub_type[0].replace(' ', '_')}_" if sub_type is not None else ''
            func = f"build_{entity_type}_{_sub_type}constraints"
            constraints = globals()[func.lower()](entity_type)
        except Exception as e:
            if sub_type:
                sub_type_desc = sub_type[0]
                error = f"Constraints could not be found with `sub_type`: `{sub_type_desc}`."
            else:
                error = "Constraints could not be found."
            func = f"build_{entity_type}_constraints"
    return {
        'constraints': constraints,
        'error': error
    }


def validate_constraint_units_to_entry_units(entry_units, const_units) -> bool:
    all_match = []
    const_units = get_constraint_unit_as_list(const_units)
    entry_units = get_constraint_unit_as_list(entry_units)
    for entry_unit in entry_units:

        sub_type = entry_unit.get('sub_type')
        if sub_type is not None:
            sub_type.sort()

        sub_type_val = entry_unit.get('sub_type_val')
        if sub_type_val is not None:
            sub_type_val.sort()

        match = False
        for const_unit in const_units:
            if DeepDiff(entry_unit, const_unit, ignore_string_case=True, exclude_types=[type(None)]) == {}:
                match = True
                break

        all_match.append(match)

    return False not in all_match

def get_constraint_unit(entry):
    if type(entry) is list and len(entry) > 0:
        return entry[0]
    elif type(entry) is dict:
        return entry
    else:
        return None


def get_constraint_unit_as_list(entry):
    if type(entry) is list:
        return entry
    elif type(entry) is dict:
        return [entry]
    else:
        return []


# Validates based on exclusions. Example constraint:
# build_constraint_unit(entity, [SpecimenCategory.BLOCK], ['!', Organs.BLOOD])
def validate_exclusions(entry, constraint, key) -> bool:
    entry_key = get_constraint_unit_as_list(entry.get(key))
    const_key = get_constraint_unit_as_list(constraint.get(key))

    if len(const_key) > 0 and const_key[0] == "!":
        const_key.pop(0)
        if any(x in entry_key for x in const_key):
            return False
        else:
            return True
    else:
        return False


def get_constraints(entry, key1, key2, is_match=False) -> dict:
    entry_key1 = get_constraint_unit(entry.get(key1))
    msg = f"Missing `{key1}` in request. Use orders=ancestors|descendants request param to specify. Default: ancestors"
    result = {'code': 400, 'name': "Bad Request"} if is_match else {'code': 200, 'name': 'OK', 'description': 'Nothing to validate.'}

    if entry_key1 is not None:
        report = determine_constraint_from_entity(entry_key1)
        constraints = report.get('constraints')
        if report.get('error') is not None and not constraints:
            result = {'code': 400, 'name': "Bad Request", 'description': report.get('error')}

        for constraint in constraints:
            const_key1 = get_constraint_unit(constraint.get(key1))

            if DeepDiff(entry_key1, const_key1, ignore_string_case=True, exclude_types=[type(None)]) == {}: # or validate_exclusions(entry_key1, const_key1, 'sub_type_val'):
                const_key2 = constraint.get(key2)

                if is_match:
                    entry_key2 = entry.get(key2)
                    v = validate_constraint_units_to_entry_units(entry_key2, const_key2) 
                    if entry_key2 is not None and v:
                        result = {'code': 200, 'name': 'OK', 'description': const_key2}
                    else:
                        entity_type = entry_key1.get('entity_type')
                        entity_type = entity_type.title() if entity_type is not None else entity_type
                        sub_type = entry_key1.get('sub_type')
                        sub_type = ', '.join(sub_type) if sub_type is not None else ''
                        msg = (
                            f"This `{entity_type}` `{sub_type}` cannot be associated with the provided `{key1}` due to entity constraints. "
                            f"Click the link to view valid entity types that can be `{key2}`"
                        )
                        result = {'code' :404, 'name': msg, 'description': const_key2}
                else:
                    result = {'code': 200, 'name': 'OK', 'description': const_key2}
                break

            else:
                result = {'code' :404, 'name': f"No matching constraints on given '{key1}"}

    return result
