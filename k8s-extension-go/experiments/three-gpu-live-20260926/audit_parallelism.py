#!/usr/bin/env python3
"""Conservative DAG/resource/timestamp parallelism audit (never overclaims readiness)."""
from __future__ import annotations
import argparse, json
from collections import Counter
from pathlib import Path

def load(p):
    with open(p) as f:return json.load(f)
def ancestors(ident,nodes,memo):
    if ident in memo:return memo[ident]
    out=set()
    for dep in nodes[ident].get('dependsOn',[]): out.add(dep); out.update(ancestors(dep,nodes,memo))
    memo[ident]=out; return out
def main():
    ap=argparse.ArgumentParser(); ap.add_argument('run_dir',type=Path); a=ap.parse_args(); root=a.run_dir
    classification=Counter(); overlaps=[]; unknown=[]
    for r in range(1,13):
        p=load(root/'plans'/f'r{r:02d}_terminal_plan.json'); nodes={n['id']:n for n in p['spec']['actionDag']['nodes']}; memo={}; sts={x['id']:x for x in p['status']['actionStatuses']}
        done=[]
        for ident,n in nodes.items():
            s=sts.get(ident,{}); st,en=s.get('relativeStartSeconds'),s.get('relativeEndSeconds')
            if s.get('status')=='completed' and isinstance(st,(int,float)) and isinstance(en,(int,float)): done.append((ident,n,float(st),float(en)))
        for i,(ia,na,sa,ea) in enumerate(done):
            for ib,nb,sb,eb in done[i+1:]:
                shared=bool(set(na.get('resources',[]))&set(nb.get('resources',[]))); dependent=ia in ancestors(ib,nodes,memo) or ib in ancestors(ia,nodes,memo); same_phase=na.get('phase')==nb.get('phase'); amount=min(ea,eb)-max(sa,sb)
                if amount>0:
                    classification['observed_overlap']+=1
                    if na['action'].get('physical_gpu_id')==nb['action'].get('physical_gpu_id') and na['action'].get('slot')!=nb['action'].get('slot'): overlaps.append({'round':r,'a':ia,'b':ib,'seconds':amount})
                elif dependent: classification['serialized_by_transitive_dag']+=1
                elif shared: classification['serialized_by_declared_resource']+=1
                elif not same_phase: classification['serialized_by_phase_gate']+=1
                else:
                    classification['unknown_simultaneously_ready_candidate']+=1
                    unknown.append({'round':r,'a':ia,'b':ib,'aType':na['action'].get('type'),'bType':nb['action'].get('type'),'gapSeconds':max(sa,sb)-min(ea,eb),'reason':'timestamps do not prove both were ready; may be executor scheduling, implicit controller lock, capacity/readiness gate'})
    report={'method':'uses transitive actionDag reachability, declared resource claims, phases and completed intervals; it does not infer readiness from timestamps','classification':dict(classification),'sameGpuDistinctSlotObservedOverlaps':overlaps,'unknownCandidates':unknown,'verdict':'unknown candidates are not labelled defects because readiness/implicit locks are not recorded per action'}
    (root/'parallelism_audit.json').write_text(json.dumps(report,indent=2,sort_keys=True)+'\n')
    print(json.dumps({'classification':report['classification'],'sameGpuDistinctSlotOverlaps':len(overlaps),'unknownCandidates':len(unknown)},sort_keys=True))
if __name__=='__main__':main()
