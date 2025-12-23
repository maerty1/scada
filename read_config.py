#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Скрипт для чтения конфигурации службы из config.json
"""
import json
import sys

try:
    with open('config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    service_config = config.get('service', {})
    run_as_user = service_config.get('run_as_user', '')
    run_as_password = service_config.get('run_as_password', '')
    
    if len(sys.argv) > 1:
        if sys.argv[1] == 'user':
            print(run_as_user)
        elif sys.argv[1] == 'password':
            print(run_as_password)
        else:
            print(f"{run_as_user}|{run_as_password}")
    else:
        print(f"{run_as_user}|{run_as_password}")
        
except Exception as e:
    print(f"ERROR: {e}", file=sys.stderr)
    sys.exit(1)

