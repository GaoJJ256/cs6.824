package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Client struct {
	req int64
	res string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data   map[string]string
	client map[int64]Client
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	client, _ := kv.client[args.Id]
	if args.Req >= client.req {
		client.req = args.Req + 1 // 更新
		delete(kv.data, args.Key)
		kv.data[args.Key] = args.Value
		kv.client[args.Id] = client
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	client, _ := kv.client[args.Id]
	if args.Req >= client.req {
		client.req = args.Req + 1 // 更新
		client.res = kv.data[args.Key]
		kv.data[args.Key] += args.Value
		kv.client[args.Id] = client
	}
	reply.Value = client.res
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.client = make(map[int64]Client)
	// kv.mu = sync.Mutex{}
	return kv
}
