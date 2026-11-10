"""Isolated XBroker TCP + synthetic HQMarket integration. No production database access.
Usage: python3 service_integration.py --binary /tmp/build/XBroker --socket TEST_SOCKET --mysql-port 13307 [--xengine-probe /tmp/xengine/broker_client_probe]
"""
import argparse, contextlib, datetime, json, os, pathlib, socket, struct, subprocess, tempfile, threading, time

def varint(n):
    result = bytearray()
    while n > 127:
        result.append((n & 127) | 128); n >>= 7
    result.append(n); return bytes(result)

def field(n, value):
    if isinstance(value, str): value = value.encode()
    return varint(n << 3) + varint(value) if isinstance(value, int) else varint((n << 3) | 2) + varint(len(value)) + value

def parse(data):
    def integer(pos):
        value = shift = 0
        while True:
            b = data[pos]; pos += 1; value |= (b & 127) << shift
            if b < 128: return value, pos
            shift += 7
    result = {}; pos = 0
    while pos < len(data):
        tag, pos = integer(pos)
        if (tag & 7) == 0: value, pos = integer(pos)
        elif (tag & 7) == 2:
            size, pos = integer(pos); value = data[pos:pos+size]; pos += size
        else: raise AssertionError('unexpected wire type')
        result.setdefault(tag >> 3, []).append(value)
    return result

def one(message, key, default=0): return message.get(key, [default])[0]
def packet(kind, request_id=0, extra=None, payload=b''):
    data = field(1, kind) + field(5, request_id) + payload
    for key, value in (extra or {}).items(): data += field(3, field(1, key) + field(2, str(value)))
    return struct.pack('!I', len(data)) + data

def recv(sock):
    def exact(size):
        data = b''
        while len(data) < size:
            part = sock.recv(size-len(data))
            if not part: raise EOFError()
            data += part
        return data
    return parse(exact(struct.unpack('!I', exact(4))[0]))

class Client:
    def __init__(self, port):
        self.socket = socket.create_connection(('127.0.0.1', port), timeout=5); self.next_id = 100; self.events = []
    def call(self, kind, **extra):
        self.next_id += 1; self.socket.sendall(packet(kind, self.next_id, extra))
        while True:
            response = recv(self.socket)
            if one(response, 5) == self.next_id: return response
            self.events.append(response)
    def ok(self, kind, **extra):
        result = self.call(kind, **extra)
        assert one(result, 6) == 0, (kind, result)
        return result

def free_port():
    with socket.socket() as sock: sock.bind(('127.0.0.1', 0)); return sock.getsockname()[1]

class Market:
    def __init__(self):
        self.listener = socket.socket(); self.listener.bind(('127.0.0.1', 0)); self.port = self.listener.getsockname()[1]; self.listener.listen(); self.listener.settimeout(.2)
        self.stop = threading.Event(); self.subscribed = threading.Event(); self.sequence = 0
        self.thread = threading.Thread(target=self.run, daemon=True); self.thread.start()
    def run(self):
        while not self.stop.is_set():
            try: client, _ = self.listener.accept()
            except socket.timeout: continue
            threading.Thread(target=self.handle, args=(client,), daemon=True).start()
    def handle(self, client):
        client.settimeout(.05); subscribed = False
        try:
            while not self.stop.is_set():
                try:
                    request = recv(client); kind = one(request, 1)
                    if kind in (1, 6): client.sendall(packet(kind, one(request, 5), {'token':'hq-test'}))
                    if kind == 40: subscribed = True; self.subscribed.set()
                except socket.timeout: pass
                if subscribed:
                    self.sequence += 1; now = int(time.time()*1000)
                    level = field(1, 1000) + field(2, 10000) + field(3, 2)
                    depth = field(1, field(1, '600000') + field(2, 1)) + field(2, now) + field(3, now) + field(4, level) + field(5, level)
                    client.sendall(packet(48, extra={'sequence':self.sequence}, payload=field(15, depth)))
        except (OSError, EOFError): pass
        finally: client.close()
    def close(self): self.stop.set(); self.thread.join(2); self.listener.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', required=True)
    parser.add_argument('--socket', required=True)
    parser.add_argument('--mysql-port', type=int, required=True)
    parser.add_argument('--xengine-binary')
    parser.add_argument('--ledger-fixture', required=True, help='SQL schema and preseeded simulation ledger for the four test identities; balances 10000000/20000000/30000000/40000000 cents')
    args = parser.parse_args()
    source = pathlib.Path(__file__).resolve().parents[1]/'XBroker'
    root = source.parents[1]
    database = 'account_identity_test_' + str(os.getpid())
    engine_database = database + '_engine'
    engine_user = 'identity_' + str(os.getpid())
    def sql(query):
        return subprocess.check_output(['mysql','--socket='+args.socket,'-uroot','-N','-B','-e',query], text=True)
    sql('CREATE DATABASE '+database)
    sql('USE '+database+'; '+(source/'exec/debug/sql/table_create.sql').read_text())
    sql("INSERT INTO "+database+".table_user(user,password_hash,password_salt) VALUES('broker-service',UNHEX(SHA2(CONCAT(UNHEX(REPEAT('01',16)),'broker-password'),256)),UNHEX(REPEAT('01',16)))")
    subprocess.run(['mysql','--socket='+args.socket,'-uroot',database], input=pathlib.Path(args.ledger_fixture).read_text(), text=True, check=True)
    sql('CREATE DATABASE '+engine_database)
    sql("CREATE USER '"+engine_user+"'@'127.0.0.1' IDENTIFIED BY 'identity-test-password'")
    sql("GRANT ALL ON "+engine_database+".* TO '"+engine_user+"'@'127.0.0.1'")
    market = Market()
    processes = []
    clients = []
    try:
        with tempfile.TemporaryDirectory(prefix='account-identity-') as directory:
            runtime = pathlib.Path(directory)
            (runtime/'ini').mkdir(); (runtime/'sql').mkdir()
            (runtime/'sql/table_create.sql').write_text((source/'exec/debug/sql/table_create.sql').read_text())
            (runtime/'ini/brokers.json').write_text(json.dumps([
                dict(id=1,name='Sim A',adapter='simulated',sites=['default','other']),
                dict(id=2,name='Sim B',adapter='simulated',sites=['default'])]))
            rules=json.loads((source/'counter/rules.example.json').read_text())
            now=int(time.time()*1000); day=int(datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d'))
            rules['calendar']=[dict(day=day,open_ms=now-3600000,break_ms=now+3600000,resume_ms=now+7200000,close_ms=now+10800000)]
            rules['rules'][0]['day']=day
            (runtime/'rules.json').write_text(json.dumps(rules))
            port=free_port()
            (runtime/'ini/system.ini').write_text(f'[System]\ntcp_port={port}\n[MySQL]\nhost=127.0.0.1\nport={args.mysql_port}\nuser=root\npassword=\ndatabase={database}\npool_size=4\n[HQMarket]\nhost=127.0.0.1\nport={market.port}\nuser=hq-test\npassword=hq-test\n[Trade]\nbrokers_path={runtime}/ini/brokers.json\nrules_path={runtime}/rules.json\n')
            env=dict(os.environ,LD_LIBRARY_PATH=str(source/'libs/debug')+':'+os.environ.get('LD_LIBRARY_PATH',''))
            log=open(runtime/'broker.log','w')
            def start():
                process=subprocess.Popen([str(pathlib.Path(args.binary).resolve())],cwd=runtime,env=env,stdout=log,stderr=log)
                processes.append(process)
                for _ in range(100):
                    if process.poll() is not None: raise AssertionError((runtime/'broker.log').read_text())
                    try:
                        client=Client(port); clients.append(client); client.ok(1,user='broker-service',password='broker-password'); return process,client
                    except OSError: time.sleep(.1)
                raise AssertionError('startup timeout')
            process,client=start()
            issued=client.ok(1,user='broker-service',password='broker-password')
            token=next(one(parse(item),2).decode() for item in issued.get(3,[]) if one(parse(item),1)==b'token')
            assert len(token)==32
            resumed=Client(port); clients.append(resumed)
            resumed.ok(1,token=token)
            assert one(resumed.call(1,token='invalid-token'),6)!=0
            assert not parse(one(client.ok(9),31)).get(1,[]), 'startup published accounts'
            assert len(parse(one(client.ok(61),42)).get(1,[])) == 2, 'broker directory'
            identity=dict(broker_id=1,site='default',account='000123')
            assert one(client.call(8,**dict(identity,account='unknown'),password='secret'),6) != 0, 'unknown account login accepted'
            for changed in [dict(broker_id=999),dict(site='missing')]:
                assert one(client.call(8,**dict(identity,**changed),password='secret'),6) != 0
            for target in [identity,dict(identity,site='other'),dict(identity,broker_id=2),dict(identity,account='123')]:
                response=client.ok(8,**target,password='secret')
                record=parse(one(response,41)); assert one(record,15) == target['account'].encode()
            assert len(parse(one(client.ok(9),31)).get(1,[])) == 4
            def funds(peer,target):
                reply=parse(one(peer.ok(14,**target),41)); result=parse(one(reply,9))
                assert one(result,11) == target['account'].encode()
                return one(result,2)
            assert [funds(client,target) for target in [identity,dict(identity,site='other'),dict(identity,broker_id=2),dict(identity,account='123')]] == [10000000,20000000,30000000,40000000]
            client.ok(8,**identity,password='secret')
            assert market.subscribed.wait(5)
            time.sleep(.2)
            order=dict(identity,client_order_id=201,security='600000.SSE',side='buy',order_type='limit',price=1000,price_scale=2,quantity=100,strategy_id=42,signal_id=43)
            reply=parse(one(client.ok(49,**order),41)); order_id=one(parse(one(reply,8)),1)
            client.ok(49,**order)
            trades=parse(one(parse(one(client.ok(53,**identity),41)),12)); assert len(trades.get(1,[])) == 1
            for kind,tag in [(51,10),(52,11),(53,12)]: assert tag in parse(one(client.ok(kind,**identity),41))
            for target in [dict(identity,site='other'),dict(identity,broker_id=2)]:
                assert not parse(one(parse(one(client.ok(53,**target),41)),12)).get(1,[]), 'cross-account trades'
            client.ok(13,**identity)
            client.ok(8,**identity,password='secret')
            balance=funds(client,identity)
            assert balance < 10000000
            assert one(client.call(59,**identity),6) != 0
            if args.xengine_binary:
                engine=runtime/'engine'; (engine/'ini').mkdir(parents=True); (engine/'sql').mkdir()
                engine_source=root/'XEngine/XEngine'
                (engine/'sql/table_create.sql').write_text((engine_source/'exec/debug/sql/table_create.sql').read_text())
                engine_port=free_port()
                (engine/'ini/system.ini').write_text(f'[Server]\nhost0=synthetic_127.0.0.1_{market.port}_1\n[System]\ntcp_port={engine_port}\nhttp_port={free_port()}\n[Broker]\nhost=127.0.0.1\nport={port}\nuser=broker-service\npassword=broker-password\n[MySQL]\nhost=127.0.0.1\nport={args.mysql_port}\nuser={engine_user}\npassword=identity-test-password\ndatabase={engine_database}\npool_size=4\n[HQMarket]\nhost=127.0.0.1\nport={market.port}\nuser=hq-test\npassword=hq-test\n')
                engine_env=dict(env,LD_LIBRARY_PATH=str(engine_source/'libs/debug')+':'+env['LD_LIBRARY_PATH'])
                engine_log=open(engine/'engine.log','w')
                engine_process=subprocess.Popen([args.xengine_binary],cwd=engine,env=engine_env,stdout=engine_log,stderr=engine_log); processes.append(engine_process)
                for _ in range(100):
                    if engine_process.poll() is not None: raise AssertionError((engine/'engine.log').read_text())
                    try: terminal=Client(engine_port); clients.append(terminal); break
                    except OSError: time.sleep(.1)
                terminal.ok(3,user='test-user',password='test-password'); terminal.ok(1,user='test-user',password='test-password')
                assert len(parse(one(terminal.ok(61),42)).get(1,[])) == 2
                terminal.ok(8,**identity,password='secret')
                time.sleep(2)
                assert funds(terminal,identity) == balance
                pending=dict(order,client_order_id=202,signal_id=44,price=900)
                pending_id=one(parse(one(parse(one(terminal.ok(49,**pending),41)),8)),1)
                terminal.ok(50,**identity,order_id=pending_id)
                strategy=field(2,'moving_average')+field(3,'identity-strategy')+field(5,field(1,'600000')+field(2,'SSE')+field(3,'quote'))+field(6,4096)+field(8,1)+field(9,1)+field(10,'default')+field(11,'000123')
                terminal.next_id+=1; terminal.socket.sendall(packet(20,terminal.next_id,payload=field(22,strategy)))
                while True:
                    result=recv(terminal.socket)
                    if one(result,5)==terminal.next_id: break
                assert one(result,6)==0, ('strategy binding',result)
                strategies=parse(one(terminal.ok(22),23)); bound=parse(one(strategies,1))
                assert one(bound,9)==1 and one(bound,10)==b'default' and one(bound,11)==b'000123'
                engine_process.terminate(); engine_process.wait(10)
                print('XTrader protocol -> XEngine -> XBroker directory, dynamic login, query, order, cancel and strategy binding passed',flush=True)
            client.socket.close(); process.terminate(); process.wait(10)
            deadline=time.monotonic()+45
            while time.monotonic()<deadline:
                if sql('SELECT IF(expires_at>CURRENT_TIMESTAMP(3),1,0) FROM '+database+'.sim_counter_lease WHERE id=1').strip()=='0':break
                time.sleep(.2)
            process,client=start()
            assert not parse(one(client.ok(9),31)).get(1,[]), 'restart automatically logged in'
            client.ok(8,**identity,password='secret')
            assert funds(client,identity)==balance, 'restart changed balance'
            assert len(parse(one(parse(one(client.ok(53,**identity),41)),12)).get(1,[]))==1, 'restart lost trades'
            print('Identity isolation, leading zeros, independent opening/login, duplicate open/login, reconnect and restart ledger recovery passed',flush=True)
    finally:
        for client in clients:
            with contextlib.suppress(OSError):client.socket.close()
        for process in processes:
            if process.poll() is None:process.terminate();process.wait(10)
        market.close()
        sql('DROP DATABASE '+database);sql('DROP DATABASE '+engine_database)
        sql("DROP USER '"+engine_user+"'@'127.0.0.1'")

if __name__ == '__main__':main()
