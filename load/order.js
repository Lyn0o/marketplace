import http from 'k6/http';
import {check,sleep} from 'k6';
export const options={stages:[{duration:'30s',target:20},{duration:'1m',target:50},{duration:'30s',target:0}],summaryTrendStats:['med','p(95)','p(99)']};
const url='http://nginx';
export function setup(){
  const account={email:'load'+Date.now()+'@example.test',password:'Lab9-demo-password'};
  const r=http.post(url+'/auth/register',JSON.stringify(account),{headers:{'Content-Type':'application/json'}});
  if(r.status!==201) throw Error('Registration '+r.status);
  return {account,id:r.json('id')};
}
export default function(data){
  const headers={'Content-Type':'application/json'};
  const r=http.post(url+'/auth/login',JSON.stringify(data.account),{headers});
  if(check(r,{'login 200':x=>x.status===200})){
    headers.Authorization='Bearer '+r.json('access_token');
    const order=http.post(url+'/orders',JSON.stringify({userId:data.id,items:[{productId:'1',quantity:1}],totalAmount:1500}),{headers});
    check(order,{'order 201':x=>x.status===201});
  }
  sleep(1);
}
