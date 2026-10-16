"""Tabbed workload controls and non-secret browser settings for the observer."""

COLUMN_JS = r'''
function primaryColumns(){return [...document.querySelectorAll('#primaryColumns .column-definition')].map(row=>({name:row.querySelector('.column-name').value,length:Number(row.querySelector('.column-width').value)}))}
function renderPrimaryColumns(columns){
 const count=Number($('#primaryColumnCount').value);
 if(!Number.isInteger(count)||count<1||count>64)return;
 const previous=columns||primaryColumns();
 $('#primaryColumns').innerHTML=Array.from({length:count},(_,i)=>`<div class="column-definition field"><label for="primaryColumnName${i}">Column ${i+1} name / VARCHAR width *</label><input id="primaryColumnName${i}" class="column-name" maxlength="64" aria-label="Column ${i+1} name"><input id="primaryColumnWidth${i}" class="column-width" type="number" min="1" max="10000" aria-label="Column ${i+1} VARCHAR width"></div>`).join('');
 [...document.querySelectorAll('#primaryColumns .column-definition')].forEach((row,i)=>{row.querySelector('.column-name').value=previous[i]?.name??`column_${i+1}`;row.querySelector('.column-width').value=previous[i]?.length??100});
 updatePrimarySql();
}
function updatePrimarySql(){
 const columns=primaryColumns(),quote=value=>'`'+String(value).replace(/`/g,'``')+'`';
 const table=quote($('#primaryDatabase').value||'test_database')+'.`elastic_insert_<run>`';
 $('#primarySqlPreview').textContent=`CREATE TABLE ${table} (\n  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,\n  createdby VARCHAR(255) NOT NULL,\n  ${columns.map(c=>quote(c.name)+' VARCHAR('+c.length+') NOT NULL').join(',\n  ')}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n\nINSERT INTO ${table} (createdby, ${columns.map(c=>quote(c.name)).join(', ')})\nVALUES (@@hostname, ${columns.map(c=>"REPEAT('x', "+c.length+")").join(', ')});\n-- Batch size: ${$('#primaryBatchSize').value} rows; COMMIT after each batch.`;
}
renderPrimaryColumns();
$('#primaryColumnCount').addEventListener('input',()=>renderPrimaryColumns());
document.addEventListener('input',event=>{if(event.target.closest('#primaryColumns')||['primaryDatabase','primaryBatchSize'].includes(event.target.id))updatePrimarySql()});
'''

RENDER_JS = r'''
function renderTest(kind,run){const running=!!run?.running;$('#'+kind+'Start').disabled=running;$('#'+kind+'Stop').disabled=!running||run.stopping;$('#'+kind+'State').textContent=run?(run.stopping?'Stopping…':running?'Running':'Stopped')+' · '+run.host+':'+run.port:'Idle';if(!run)return;
const columns=[['Session',(s,k)=>k],['Status',s=>s.status],['Backend / connection',s=>`${s.hostname||'—'} / ${s.connection_id??'—'}`],[kind==='ddl'?'Completed cycles / probes':kind==='primary'?'Completed batches / probes':'Completed queries / probes',s=>s.successes],['Committed rows',s=>s.committed_rows??0],['Committed batches',s=>s.committed_batches??0],...(kind==='ddl'?[['DDL creates / drops',s=>`${s.ddl_creates??0} / ${s.ddl_drops??0}`],['Reader checks / timeouts',s=>`${s.consistency_passes??0} / ${s.consistency_timeouts??0}`],['Table',s=>s.table||'—']]:[]),['Failures',s=>s.failures],['Connections / reconnects',(s,k)=>`${s.connections} / ${k==='fresh-login'?'—':Math.max(0,s.connections-1)}`],['Last / max operation (s)',s=>`${s.latency_seconds??'—'} / ${s.max_latency_seconds}`],['Interruption / max (s)',s=>`${s.outage_seconds??'—'}${s.outage_active?' (ongoing)':''} / ${s.max_outage_seconds}`],['Last success (UTC)',s=>s.last_success_at||'—'],['Last failure (UTC)',s=>s.last_failure_at||'—'],['Uncertain commits',s=>s.uncertain_commits],['Last error',s=>s.last_error||'—']];
$('#'+kind+'Results').innerHTML=`${kind!=='readonly'?`<p>Table: <code>${esc(run.table||'Per-worker DDL tables below')}</code> · Committed rows: ${esc(run.committed_rows)} · Committed batches: ${esc(run.committed_batches)} · Average rows/s: ${esc(run.rows_per_second)}</p>`:''}<p>Run started ${esc(run.started_at)}${run.stopped_at?' · Stopped '+esc(run.stopped_at):''}</p>${run.cancel_error?'<p class="error">Stop interruption error: '+esc(run.cancel_error)+'; waiting for the operation timeout.</p>':''}<table class="eventtable"><thead><tr>${columns.map(([label])=>'<th>'+label+'</th>').join('')}</tr></thead><tbody>${Object.entries(run.sessions).sort(([a],[b])=>a.localeCompare(b,undefined,{numeric:true})).map(([key,s])=>'<tr>'+columns.map(([,value])=>'<td>'+esc(value(s,key))+'</td>').join('')+'</tr>').join('')}</tbody></table>`;
$('#'+kind+'Events').innerHTML=[...run.events].reverse().map(e=>`<tr><td>${esc(e.timestamp_utc)}</td><td>${esc(e.session)}</td><td>${esc(e.event)}</td><td>${esc(e.detail)}</td></tr>`).join('')}
'''

SETTINGS_JS = r'''
const settingsKey='elasticdb-observer-settings-v1';
const settingsFields=['user','interval','primaryThreads','primaryDatabase','primaryBatchSize','primaryColumnCount','primaryPause','readonlyThreads','readonlySleep','readonlyPause','ddlDatabase','ddlThreads','ddlBatchSize','ddlBatches','ddlWidth','ddlTimeout','ddlPause'];
let settingsTimer;
function settingsMessage(text,bad=false){$('#settingsMessage').textContent=text;$('#settingsMessage').className=bad?'error':'muted'}
function saveSettings(){
 clearTimeout(settingsTimer);
 try{
  const fields=Object.fromEntries(settingsFields.map(id=>[id,$('#'+id).value]));
  const tables=[...document.querySelectorAll('#tablepick input[type="checkbox"]')].map(input=>({name:input.value,checked:input.checked}));
  const servers=[...document.querySelectorAll('.serverinput')].map(row=>({name:row.querySelector('.name').value,host:row.querySelector('.host').value,port:row.querySelector('.port').value}));
  const settings={version:1,servers,fields,tables,columns:primaryColumns(),activeTab:toolTabs.findIndex(tab=>tab.getAttribute('aria-selected')==='true')};
  localStorage.setItem(settingsKey,JSON.stringify(settings));
  settingsMessage('Settings saved in this browser. Passwords are not saved.');
 }catch(error){settingsMessage('Settings could not be saved. Check that browser storage is enabled.',true)}
}
function restoreSettings(){
 try{
  const raw=localStorage.getItem(settingsKey);if(raw===null)return;
  const settings=JSON.parse(raw);
  if(!settings||settings.version!==1||!Array.isArray(settings.servers)||settings.servers.length>16||!settings.fields||!Array.isArray(settings.tables))throw Error('Invalid saved settings');
  if(settings.servers.some(s=>!s||typeof s.name!=='string'||typeof s.host!=='string'||typeof s.port!=='string'))throw Error('Invalid saved servers');
  if(settings.tables.length>2000||settings.tables.some(t=>!t||typeof t.name!=='string'||!/^[A-Za-z0-9_]+$/.test(t.name)||typeof t.checked!=='boolean'))throw Error('Invalid saved tables');
  $('#serverInputs').replaceChildren();
  for(const server of settings.servers)addServer(server.name,server.host,server.port);
  for(const id of settingsFields)if(typeof settings.fields[id]==='string')$('#'+id).value=settings.fields[id];
  renderPrimaryColumns(Array.isArray(settings.columns)?settings.columns:undefined);
  if(settings.tables.length)$('#tablepick').innerHTML=settings.tables.map(t=>`<label><input type="checkbox" value="${esc(t.name)}"${t.checked?' checked':''}> ${esc(t.name)}</label>`).join('');
  selectToolTab(Number.isInteger(settings.activeTab)&&settings.activeTab>=0&&settings.activeTab<toolTabs.length?settings.activeTab:0);
  settingsMessage('Saved settings restored. Enter your password before starting.');
 }catch(error){settingsMessage('Saved settings could not be restored. You can enter and save new settings.',true)}
}
restoreSettings();
$('#saveSettings').onclick=saveSettings;
function settingsInput(target){return target.matches('#serverInputs input, #tablepick input, #primaryColumns input')||settingsFields.includes(target.id)}
document.addEventListener('input',event=>{if(settingsInput(event.target)){clearTimeout(settingsTimer);settingsTimer=setTimeout(saveSettings,300)}});
document.addEventListener('change',event=>{if(settingsInput(event.target))saveSettings()});
document.addEventListener('click',event=>{if(event.target.closest('#addServer, .serverinput button, [role="tab"]'))saveSettings()});
window.addEventListener('pagehide',()=>{if(settingsTimer)saveSettings()});
// Retain the selected tables when discovery refreshes the available choices.
const discoverWithSettings=$('#discover').onclick;
$('#discover').onclick=async()=>{
 const selected=new Set([...document.querySelectorAll('#tablepick input:checked')].map(input=>input.value));
 await discoverWithSettings();
 for(const input of document.querySelectorAll('#tablepick input'))input.checked=selected.has(input.value);
 saveSettings();
};
'''


def field(id, label, value='', type='number', limits=''):
    return f'<div class="field"><label for="{id}">{label} *</label><input id="{id}" type="{type}" value="{value}" {limits}></div>'


def case_panel(kind, title):
    controls = field(kind+'Threads', 'Threads', '1', limits='min="1" max="64"')
    if kind != 'readonly':
        controls += field(kind+'Database', 'Existing test database', type='text')
    if kind == 'readonly':
        controls += field('readonlySleep', 'SLEEP timer (seconds)', '10', limits='min="0.1" max="3600" step="0.1"')
    controls += field(kind+'Pause', 'Loop pause (seconds)', '0.1', limits='min="0.01" max="60" step="0.01"')
    extra = ''
    if kind == 'primary':
        extra = '<div class="formgrid">'+field('primaryBatchSize', 'Batch size (rows per commit)', '100', limits='min="1" max="10000"')
        extra += field('primaryColumnCount', 'VARCHAR columns per INSERT', '3', limits='min="1" max="64"')+'</div>'
        extra += '<p class="muted">Each row includes id, createdby = @@hostname, and your VARCHAR columns filled using REPEAT(\'x\', width). Data remains after Stop.</p><div id="primaryColumns" class="formgrid"></div><details><summary>Table structure and INSERT preview</summary><pre id="primarySqlPreview"></pre></details>'
    if kind == 'ddl':
        extra = '<div class="formgrid">'+field('ddlBatchSize','Rows per batch','100',limits='min="1" max="10000"')
        extra += field('ddlBatches','Batches per cycle','2',limits='min="1" max="100"')
        extra += field('ddlWidth','Payload VARCHAR width','100',limits='min="1" max="10000"')
        extra += field('ddlTimeout','Reader convergence timeout (seconds)','30',limits='min="0.1" max="300" step="0.1"')+'</div>'
        extra += '<p>Cycle: clean up the previous run-owned table → CREATE → INSERT and COMMIT batches → verify primary and read-only schema/data → DROP → verify absence on all detected read-only nodes.</p><p class="muted">Only elastic_ddl_ tables owned by this run are dropped. An interrupted cycle can leave its table for inspection. CREATE, DROP, INSERT and SELECT privileges are required.</p>'
    checks = '<div class="testresults"><h3>Read-node consistency</h3><p class="muted">Expected / observed data: [row count, minimum ID, maximum ID, rows matching payload, cycle token and createdby]. Schema must also match the primary.</p><table class="eventtable"><thead><tr><th>Worker / table</th><th>Read-only server</th><th>Phase</th><th>Result</th><th>Expected / observed</th><th>Elapsed (s)</th><th>Last check (UTC)</th><th>Error</th></tr></thead><tbody id="ddlChecks"></tbody></table></div>' if kind == 'ddl' else ''
    return f'''<section id="{kind}View" role="tabpanel" aria-labelledby="{kind}Tab" hidden>
<div class="panel"><h2>{title}</h2><p>Uses the server list and credentials applied with Start observe on the Observer tab. Roles are detected automatically; no separate IP settings are needed.</p><p class="roleSummary muted">Start observe, then detect server roles or start this test.</p><p id="{kind}Message" role="status"></p><div class="formgrid">{controls}</div>{extra}<div class="actions"><button id="{kind}Start" class="primary">Start test</button><button id="{kind}Stop" disabled>Stop</button><span id="{kind}State">Idle</span></div><div class="testresults" id="{kind}Results"></div>{checks}<div class="testresults"><h3>Workload events (UTC)</h3><table class="eventtable"><thead><tr><th>UTC time</th><th>Session</th><th>Event</th><th>Detail</th></tr></thead><tbody id="{kind}Events"></tbody></table></div></div></section>'''


JS = r'''
const kinds=['primary','readonly','ddl'];
const toolTabs=['observer',...kinds].map(kind=>$('#'+kind+'Tab'));
function selectToolTab(index){toolTabs.forEach((tab,i)=>{tab.classList.toggle('active',i===index);tab.setAttribute('aria-selected',String(i===index));tab.tabIndex=i===index?0:-1;$('#'+tab.getAttribute('aria-controls')).hidden=i!==index})}
toolTabs.forEach((tab,i)=>{tab.onclick=()=>selectToolTab(i);tab.onkeydown=e=>{let index;if(e.key==='ArrowRight')index=(i+1)%toolTabs.length;else if(e.key==='ArrowLeft')index=(i+toolTabs.length-1)%toolTabs.length;else if(e.key==='Home')index=0;else if(e.key==='End')index=toolTabs.length-1;else return;e.preventDefault();selectToolTab(index);toolTabs[index].focus()}});
function testMessage(kind,text,bad=false){$('#'+kind+'Message').textContent=text;$('#'+kind+'Message').className=bad?'error':'ok'}
function renderRoles(roles){if(!roles?.length)return;const text=roles.map(s=>`${s.name}: ${s.host}:${s.port} · ${s.connected?(s.read_only?'READ ONLY':'WRITABLE')+' · '+s.hostname:'UNAVAILABLE · '+s.error}`).join(' | ');document.querySelectorAll('.roleSummary').forEach(el=>el.textContent=text)}
$('#detectRoles').onclick=async()=>{try{const result=await api('/api/tests/roles',{method:'POST'});renderRoles(result.roles);$('#roleMessage').textContent='Roles detected from Observer settings.'}catch(error){$('#roleMessage').textContent=error.message}};
for(const kind of kinds){
 $('#'+kind+'Start').onclick=async()=>{const button=$('#'+kind+'Start');button.disabled=true;try{
  const data={threads:Number($('#'+kind+'Threads').value),pause:Number($('#'+kind+'Pause').value)};
  if(kind==='primary')Object.assign(data,{database:$('#primaryDatabase').value,batch_size:Number($('#primaryBatchSize').value),columns:primaryColumns()});
  if(kind==='readonly')data.sleep=Number($('#readonlySleep').value);
  if(kind==='ddl')Object.assign(data,{database:$('#ddlDatabase').value,batch_size:Number($('#ddlBatchSize').value),batches:Number($('#ddlBatches').value),width:Number($('#ddlWidth').value),consistency_timeout:Number($('#ddlTimeout').value)});
  await api('/api/tests/'+kind+'/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
  testMessage(kind,'Test started. Observer sampling continues.');await refreshTests();
 }catch(error){testMessage(kind,error.message,true);button.disabled=false}};
 $('#'+kind+'Stop').onclick=async()=>{try{await api('/api/tests/'+kind+'/stop',{method:'POST'});await refreshTests()}catch(error){testMessage(kind,error.message,true)}};
}
let testsRefreshing=false;
async function refreshTests(){if(testsRefreshing)return;testsRefreshing=true;try{const runs=await api('/api/tests/status');for(const kind of kinds){renderTest(kind,runs[kind]);if(runs[kind])renderRoles(runs[kind].roles)}const checks=runs.ddl?.checks||[];$('#ddlChecks').innerHTML=checks.map(c=>`<tr><td>${esc(c.worker)}<br>${esc(c.table)}</td><td>${esc(c.name)}<br>${esc(c.host)}:${esc(c.port)}</td><td>${esc(c.phase)}</td><td>${esc(c.status)}</td><td>${esc(JSON.stringify(c.expected))} / ${esc(JSON.stringify(c.observed))}</td><td>${esc(c.elapsed_seconds)}</td><td>${esc(c.observed_at)}</td><td>${esc(c.error||'—')}</td></tr>`).join('')}catch(error){$('#roleMessage').textContent=error.message}finally{testsRefreshing=false}}
setInterval(refreshTests,1000);refreshTests();
'''


def add_test_ui(page):
    tabs = '<div class="tabs" role="tablist" aria-label="Observer and test cases">'
    for i, (kind, label) in enumerate([('observer','Observer'),('primary','Test 1 · INSERT'),('readonly','Test 2 · SLEEP'),('ddl','Test 3 · DDL + consistency')]):
        tabs += f'<button id="{kind}Tab" role="tab" class="{"active" if i == 0 else ""}" aria-selected="{"true" if i == 0 else "false"}" aria-controls="{kind}View" tabindex="{0 if i == 0 else -1}">{label}</button>'
    tabs += '</div>'
    banner = '<section class="config"><div class="actions"><button id="saveSettings">Save settings</button><span id="settingsMessage" role="status">Settings save automatically. Passwords are not saved.</span></div><div class="actions"><button id="detectRoles">Detect server roles</button><span id="roleMessage" role="status">Configure Primary, Node 1 and Node 2 on Observer, then Start observe to apply settings.</span></div><p class="muted">Tests run until Stop. Switching tabs keeps tests and observation running. Fresh-login probes run every second. Interruption includes operation and retry time; uncertain COMMIT replies are counted separately.</p></section>'
    page = page.replace('<main>', '<main>'+banner+tabs+'<div id="observerView" role="tabpanel" aria-labelledby="observerTab">')
    panels = ''.join(case_panel(kind,title) for kind,title in [('primary','Test 1 · Primary INSERT load'),('readonly','Test 2 · Read-only long queries'),('ddl','Test 3 · Ordered DDL / INSERT load and read-node consistency')])
    page = page.replace('</main>', '</div>'+panels+'</main>')
    page = page.replace("addServer('Server 1');addServer('Server 2');", "addServer('Primary');addServer('Node 1');addServer('Node 2');")
    page = page.replace('</style>', '''[hidden]{display:none!important}.tabs{display:flex;flex-wrap:wrap;gap:.3rem;border-bottom:1px solid #bac8d8;margin-bottom:1rem}.tabs button{border-radius:6px 6px 0 0}.tabs button.active{background:white;border-bottom-color:white;font-weight:700;margin-bottom:-1px}.primary{background:linear-gradient(#c74634,#a52e20)!important;border-color:#a52e20!important}.primary:disabled{background:#aeb5bc!important;border-color:#aeb5bc!important}.testresults{max-height:400px;overflow:auto;margin-top:1rem}.testresults table td{white-space:normal;min-width:100px}.testresults table td:last-child{min-width:200px}.formgrid{margin-top:.7rem}pre{white-space:pre-wrap;overflow-wrap:anywhere}details{margin-top:.7rem}</style>''')
    return page.replace('</script>', COLUMN_JS+RENDER_JS+JS+SETTINGS_JS+'</script>')
