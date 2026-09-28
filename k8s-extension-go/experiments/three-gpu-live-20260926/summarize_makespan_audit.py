#!/usr/bin/env python3
"""Add action, batch-chain and concurrency details to a strict run audit."""
from __future__ import annotations
import argparse, csv, json
from pathlib import Path

def load(p):
    with open(p) as f: return json.load(f)

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('run_dir', type=Path); a=ap.parse_args(); root=a.run_dir
    rows=list(csv.DictReader(open(root/'round_summary.csv'))); all_actions=[]; batches=[]; overlaps=[]; candidates=[]
    for row in rows:
        r=int(row['live_round']); p=load(root/'plans'/f'r{r:02d}_terminal_plan.json'); nodes={n['id']:n for n in p['spec']['actionDag']['nodes']}; status={s['id']:s for s in p['status']['actionStatuses']}
        for ident,n in nodes.items():
            s=status.get(ident,{}); act=n.get('action',{}); all_actions.append(act.get('type'))
            if act.get('type')=='patch_batch_config':
                batches.append({'round':r,'workload':act.get('workload'),'oldBatch':act.get('old_batch'),'newBatch':act.get('new_batch'),'slot':act.get('slot'),'completed':s.get('status')=='completed'})
        completed=[]
        for ident,n in nodes.items():
            s=status.get(ident,{}); st,en=s.get('relativeStartSeconds'),s.get('relativeEndSeconds')
            if s.get('status')=='completed' and isinstance(st,(int,float)) and isinstance(en,(int,float)):
                completed.append((ident,n,float(st),float(en)))
        for i,(ida,na,sa,ea) in enumerate(completed):
            aa=na.get('action',{}); ra=set(na.get('resources',[]))
            for idb,nb,sb,eb in completed[i+1:]:
                ab=nb.get('action',{}); rb=set(nb.get('resources',[])); amount=min(ea,eb)-max(sa,sb)
                if amount>0 and aa.get('physical_gpu_id')==ab.get('physical_gpu_id') and aa.get('slot')!=ab.get('slot'):
                    overlaps.append({'round':r,'a':ida,'b':idb,'aType':aa.get('type'),'bType':ab.get('type'),'gpu':aa.get('physical_gpu_id'),'seconds':amount})
                # Conservative candidate: no direct dependency either way, no shared declared resource,
                # yet their intervals did not overlap.  This is a review list, not a correctness failure.
                if not ra.intersection(rb) and idb not in na.get('dependsOn',[]) and ida not in nb.get('dependsOn',[]) and ea<=sb:
                    candidates.append({'round':r,'first':ida,'second':idb,'firstType':aa.get('type'),'secondType':ab.get('type'),'gapSeconds':sb-ea})
    from collections import Counter
    ms=[float(x['makespan_seconds']) for x in rows]
    same_phase=[x for x in candidates if next(n for n in load(root/'plans'/f"r{x['round']:02d}_terminal_plan.json")['spec']['actionDag']['nodes'] if n['id']==x['first'])['phase'] == next(n for n in load(root/'plans'/f"r{x['round']:02d}_terminal_plan.json")['spec']['actionDag']['nodes'] if n['id']==x['second'])['phase']]
    d={'roundMakespansSeconds':[{ 'round':int(x['live_round']),'seconds':float(x['makespan_seconds']),'actions':int(x['action_count'])} for x in rows],
       'makespanSummary':{'sum':sum(ms),'mean':sum(ms)/len(ms),'max':max(ms),'maxRound':int(rows[ms.index(max(ms))]['live_round'])},
       'totalActions':len(all_actions),'actionCounts':dict(Counter(all_actions)),'batchPatches':batches,
       'sameGpuDistinctSlotOverlaps':overlaps,'sameGpuDistinctSlotOverlapCount':len(overlaps),
       'directDependencyFreeDisjointResourceSequentialCandidates':candidates,'candidateCount':len(candidates),'samePhaseCandidateCount':len(same_phase),'samePhaseCandidateExamples':same_phase[:20],
       'notes':['Candidates use only direct DAG dependencies and declared resources; transitive dependencies, controller phases, readiness and capacity gates can legitimately serialize them.', 'No candidate is treated as an execution defect without a dependency/capacity-gate review.']}
    (root/'strict_runtime_audit_summary.json').write_text(json.dumps(d,indent=2,sort_keys=True)+'\n')
    print(json.dumps({'sum':d['makespanSummary']['sum'],'actions':d['totalActions'],'batchPatches':len(batches),'overlaps':len(overlaps),'candidates':len(candidates)},sort_keys=True))
if __name__=='__main__': main()
