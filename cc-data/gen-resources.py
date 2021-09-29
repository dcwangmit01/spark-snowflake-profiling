#!/usr/bin/env python

import sys
import yaml

resources = []
for i in sys.stdin:
    resources.append(i.strip())

config = {
    "policies": [],
}

for resource in resources:
    config["policies"].append({"name": resource.replace('.', '-') + "-all", "resource": resource})

print(yaml.dump(config, indent=2))
