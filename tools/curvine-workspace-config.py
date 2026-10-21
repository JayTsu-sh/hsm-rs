#!/usr/bin/env python3
"""Emit Cargo patches for coordinated, unpushed Curvine/hsm-rs development."""
import argparse
import json
from pathlib import Path
import tomllib

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--curvine', required=True, type=Path)
parser.add_argument('--output', required=True, type=Path)
args = parser.parse_args()
curvine = args.curvine.resolve()
hsm = Path(__file__).resolve().parents[1]
workspace = tomllib.loads((curvine / 'Cargo.toml').read_text())['workspace']
packages = {}
for member in workspace['members']:
    for directory in curvine.glob(member):
        manifest = directory / 'Cargo.toml'
        if manifest.exists():
            package = tomllib.loads(manifest.read_text()).get('package')
            if package:
                packages[package['name']] = directory.resolve()
assert 'curvine-model' in packages and 'curvine-client-core' in packages
lines = ['[patch."https://github.com/JayTsu-sh/curvine.git"]']
for name, path in sorted(packages.items()):
    lines.append(f'{name} = {{ path = {json.dumps(str(path))} }}')
lines += ['', '[patch."https://github.com/JayTsu-sh/hsm-rs.git"]',
          f'hsm-core = {{ path = {json.dumps(str(hsm / "crates/hsm-core"))} }}', '']
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text('\n'.join(lines))
print(args.output)
