"""Exercise the public gateway and retain sanitized responses for the report."""
import base64
import json
import time
import urllib.error
import urllib.request
from pathlib import Path
from uuid import uuid4

base = 'http://127.0.0.1:18080'
evidence = {}
out = Path('reports/labs79')
out.mkdir(parents=True, exist_ok=True)


def call(path, body=None, token=None):
    headers = {'Content-Type':'application/json'}
    if token:
        headers['Authorization']='Bearer '+token
    request = urllib.request.Request(base+path, json.dumps(body).encode() if body is not None else None, headers)
    try:
        response = urllib.request.urlopen(request,timeout=40)
    except urllib.error.HTTPError as exc:
        response = exc
    return response.status,json.load(response)


email = 'student-'+uuid4().hex[:6]+'@example.test'
account = dict(email=email,password='Demo-lab-password')
status, user = call('/auth/register',account)
assert status == 201,(status,user)
evidence['register']=[status,user]
status,login=call('/auth/login',account)
assert status == 200
token=login['access_token']
claims=json.loads(base64.urlsafe_b64decode(token.split('.')[1]+'==='))
evidence['login']=[status,dict(login,access_token=token[:25]+'…[сокращено]'),claims]
evidence['negative_login']=call('/auth/login',dict(account,password='wrong'))
assert evidence['negative_login'][0]==401
cfg=dict(line.split('=',1) for line in Path('.env').read_text().splitlines() if '=' in line)
admin=call('/auth/login',dict(email='admin',password=cfg['ADMIN_PASSWORD']))[1]['access_token']
evidence['before']=call('/analytics/summary',token=admin)
evidence['catalog']=call('/catalog/products',token=token)
item=evidence['catalog'][1]['products'][0]
payload=dict(userId=user['id'],items=[dict(productId=str(item['id']),quantity=1)],totalAmount=item['price'])
status,order=call('/orders',payload,token)
assert status==201,(status,order)
evidence['create']=[status,order]
oid=order['orderId']
for _ in range(90):
    result=call('/orders/'+oid,token=token)
    after=call('/analytics/summary',token=admin)
    notes=call('/notifications/',token=admin)
    matched=[n for n in notes[1].get('notifications',[]) if n['orderId']==oid]
    if result[1].get('status')=='CONFIRMED' and matched and after[1]['paid_amount']>=evidence['before'][1]['paid_amount']+item['price']:
        break
    time.sleep(1)
else:
    raise RuntimeError((result,after,notes))
evidence.update(confirmed=result,after=after,notification=matched)
evidence['missing']=call('/orders/'+str(uuid4()),token=token)
call('/orders/'+oid+'/cancel',dict(reason='Observability demo'),token)
(out/'e2e.json').write_text(json.dumps(evidence,indent=2,ensure_ascii=False),encoding='utf8')
print('E2E OK',oid)

