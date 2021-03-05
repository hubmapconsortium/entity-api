# HuBMAP Provenance Schema

A Python package that handles the schema yaml loading, parsing, validation, and generating entity properties via trigger methods.

## Entities schema yaml

The yaml file `provenance_schema.yaml` contains all the attributes of each entity type and generated metadata information of attributes via trigger methods. This file is being used to validate the user input and also as a way of standarding all the details of entities.

## Rules
- By default, the schema treats all entity properties as optional. Use `user_input_required: true` to mark a property as user_input_required
- By default, the schema treats all entity properties as mutable. Use `immutable: true` to mark a property as immutable
- By default, the schema treats all entity properties as persistent. Use `transient: true` to mark a property as transient
- By default, the schema treats all entity properties as they have no triggers. Specify the trigger methods if needed
- By default, the schema treats all entity properties as `exposed: true`. 

- If a property is marked as `user_input_required: true`, it means this property is required to be provided in the client request JSON
- If a property is marked as `user_input_required: true`, it can't have `trigger` at the same time
- If a property is marked as `exposed: flase`, it'll be filtered from the response
- There are 3 types of triggers: `before_create_trigger`, `before_update_trigger`, and `on_read_trigger`
- If a property has one of the triggers, it can't be used in client request JSON
- If a property has `on_read_trigger`, it must be transient, meaning it's not stored in neo4j and only available during response
