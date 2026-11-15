#!/usr/bin/env python3
"""Prepare the EC-preserving watchdog LaunchAgent without changing a running job."""
import argparse
import plistlib
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--source', type=Path, default=Path.home() / 'Library/LaunchAgents/org.prats.mediation.watchdog.plist')
parser.add_argument('--output', type=Path, default=Path('build/launchd/org.prats.mediation.watchdog.plist'))
args = parser.parse_args()
settings = plistlib.loads(args.source.read_bytes())
if settings.get('Label') != 'org.prats.mediation.watchdog':
    raise SystemExit('ERROR: source is not the Mediation watchdog LaunchAgent')
settings['AbandonProcessGroup'] = True
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_bytes(plistlib.dumps(settings))
print(f'Prepared {args.output} with AbandonProcessGroup=true.')
print('The running job is unchanged. Plan the one-time migration before reloading it.')
