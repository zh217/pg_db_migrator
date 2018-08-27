import os
from distutils.version import StrictVersion

import tarjan


def resolve_schema_files(base_dir):
    init_file = os.path.join(base_dir, 'init.sql')
    with open(init_file, 'r') as f:
        init_data = f.read()

    schema_data = {}
    for root, subdirs, files in os.walk(os.path.join(base_dir, 'schema')):
        for file in files:
            filename, ext = os.path.splitext(file)
            if ext.lower() == '.sql':
                with open(os.path.join(root, file), 'r') as f:
                    content = f.read()
                schema_data[filename.lower()] = content

    migration_data = {}
    for root, subdirs, files in os.walk(os.path.join(base_dir, 'migration')):
        for file in files:
            filename, ext = os.path.splitext(file)
            if ext.lower() == '.sql':
                with open(os.path.join(root, file), 'r') as f:
                    content = f.read()
                migration_data[filename.lower()] = content

    return init_data, schema_data, migration_data


def resolve_dependency(schema_data):
    for l in schema_data.splitlines():
        if l.startswith('--! depends:'):
            return l.replace('--! depends:', '').split()
    return []


def resolve_dependencies(schema_map):
    return {k: resolve_dependency(v) for k, v in schema_map.items()}


def ordered_dependencies(schema_map):
    dependencies = resolve_dependencies(schema_map)
    orders = tarjan.tarjan(dependencies)
    schemas = []
    for clique in orders:
        assert len(clique) == 1, f'cyclic dependency detected: {clique}'
        schemas.append(schema_map[clique[0]])
    return schemas


def ordered_migrations(migration_map, cur_version):
    migration_versions = sorted((v for v in migration_map.keys() if StrictVersion(v) > StrictVersion(cur_version)),
                                key=StrictVersion)
    return [migration_map[v] for v in migration_versions]


def get_schema_and_migrations(base_dir, cur_version):
    init, schema, migrations = resolve_schema_files(base_dir)
    return init, ordered_dependencies(schema), ordered_migrations(migrations, cur_version)
