#!/usr/bin/env python3
"""Write-Ahead Log (WAL) for crash recovery."""
import sys,json,os,tempfile

class WAL:
    def __init__(self,path=None):
        self.path=path or tempfile.mktemp(suffix='.wal')
        self.entries=[];self.lsn=0
    def append(self,op,key,value=None,old_value=None):
        self.lsn+=1
        entry={"lsn":self.lsn,"op":op,"key":key,"value":value,"old":old_value}
        self.entries.append(entry)
        with open(self.path,'a') as f:f.write(json.dumps(entry)+'\n')
        return self.lsn
    def flush(self):pass  # already writing each entry
    def replay(self):
        entries=[]
        if os.path.exists(self.path):
            with open(self.path) as f:
                for line in f:
                    if line.strip():entries.append(json.loads(line))
        return entries
    def checkpoint(self,state):
        cp_path=self.path+'.checkpoint'
        with open(cp_path,'w') as f:json.dump({"state":state,"lsn":self.lsn},f)
    def recover(self):
        cp_path=self.path+'.checkpoint';state={};start_lsn=0
        if os.path.exists(cp_path):
            with open(cp_path) as f:cp=json.load(f)
            state=cp["state"];start_lsn=cp["lsn"]
        for entry in self.replay():
            if entry["lsn"]<=start_lsn:continue
            if entry["op"]=="PUT":state[entry["key"]]=entry["value"]
            elif entry["op"]=="DELETE":state.pop(entry["key"],None)
        return state
    def cleanup(self):
        for p in [self.path,self.path+'.checkpoint']:
            if os.path.exists(p):os.unlink(p)

def main():
    if len(sys.argv)>1 and sys.argv[1]=="--test":
        wal=WAL()
        try:
            wal.append("PUT","x","1");wal.append("PUT","y","2");wal.append("DELETE","x")
            state=wal.recover()
            assert state=={"y":"2"},f"Got {state}"
            # Checkpoint + more writes
            wal.checkpoint(state)
            wal.append("PUT","z","3")
            state2=wal.recover()
            assert state2=={"y":"2","z":"3"}
            # Replay from fresh WAL object
            wal2=WAL(wal.path)
            entries=wal2.replay()
            assert len(entries)==4
            print("All tests passed!")
        finally:
            wal.cleanup()
    else:
        wal=WAL();wal.append("PUT","key","value")
        print(f"WAL entries: {wal.replay()}")
        wal.cleanup()
if __name__=="__main__":main()
