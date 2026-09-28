#!/usr/bin/env python3
"""Re-finalize completed legacy makespan artifacts after validator fixes."""
from __future__ import annotations
import argparse, csv, importlib.util, json, sys
from pathlib import Path

ROOT=Path(__file__).parent
spec=importlib.util.spec_from_file_location('live_runner_refinalize', ROOT/'live_runner_20260926.py')
runner=importlib.util.module_from_spec(spec); sys.modules[spec.name]=runner; spec.loader.exec_module(runner)

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('run_dir',type=Path); a=ap.parse_args(); root=a.run_dir
    env=json.loads((root/'environment.json').read_text()); rows=list(csv.DictReader((root/'round_summary.csv').open()))
    ctx=runner.RunContext(str(env['run_id']),root,'or-sim-exp','http://115.145.179.144:10680',0,0,0,makespan_mode=True,post_target_dwell_seconds=5)
    result={'run_id':ctx.run_id,'completed_rounds':len(rows),'expected_rounds':12,'ok':len(rows)==12,'failure':None,'range_run':False}
    result=runner._validate_and_record_outputs(ctx,result)
    print(json.dumps({'ok':result['ok'],'artifact_validation_ok':result['artifact_validation_ok'],'failure':result.get('failure')},sort_keys=True))
    return 0 if result['ok'] else 1
if __name__=='__main__': raise SystemExit(main())
