/**
 * dsh-aboard — host half.
 *
 * Serves the A-share dashboard (A股市场宏观与情绪看板) under the `/aboard` web prefix:
 *
 *   /aboard                  the dashboard document (rewritten to talk to /aboard/api/*)
 *   /aboard/echarts.min.js   the vendored ECharts build
 *   /aboard/api/*            reverse-proxied to the bundled Python backend
 *   /aboard/__status         readiness probe used by the waiting page
 *
 * The dashboard itself is a 5000-line ECharts application that cannot be faithfully
 * re-implemented as a React panel, so it is shipped as-is and rendered inside a
 * same-origin frame by the client half. Its data backend is `backend/server.py`
 * (Flask + akshare + pandas), started as a child process on a private loopback port.
 */

import { spawn, spawnSync } from 'node:child_process'
import { request as httpRequest } from 'node:http'
import { createServer as createNetServer } from 'node:net'
import { createReadStream, existsSync } from 'node:fs'
import { copyFile, mkdir, open, readFile, readdir, stat, writeFile } from 'node:fs/promises'
import { homedir } from 'node:os'
import { dirname, extname, isAbsolute, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const PREFIX = '/aboard'
const PKG_ROOT = dirname(dirname(fileURLToPath(import.meta.url)))
const ASSETS_DIR = join(PKG_ROOT, 'assets')
const BACKEND_DIR = join(PKG_ROOT, 'backend')

const HTML_FILE = join(ASSETS_DIR, 'dashboard.html')
const ECHARTS_FILE = join(ASSETS_DIR, 'echarts.min.js')
const REQUIREMENTS_FILE = join(BACKEND_DIR, 'requirements.txt')

const ECHARTS_CDN = 'https://fastly.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js'

/**
 * The dashboard lives in a frame that the host recreates whenever the user leaves and
 * returns to the panel, which would otherwise drop them back on 行情数据 every time.
 * This restores the last tab and records changes; it only touches the page's own
 * `.tab` elements and runs after the dashboard's own listeners are attached.
 */
const TAB_MEMORY = `<script>
(function () {
  var KEY = 'aboard_active_tab';
  var tabs = document.querySelectorAll('.tab');
  if (!tabs.length) return;
  var known = {};
  tabs.forEach(function (tab) { known[tab.dataset.page] = tab; });
  var saved = null;
  try { saved = window.localStorage.getItem(KEY); } catch (e) { saved = null; }
  tabs.forEach(function (tab) {
    tab.addEventListener('click', function () {
      try { window.localStorage.setItem(KEY, tab.dataset.page); } catch (e) {}
    });
  });
  if (saved && known[saved] && !known[saved].classList.contains('active')) {
    setTimeout(function () { known[saved].click(); }, 0);
  }
})();
</script>`

/** Runtime data lives outside the package so reinstalls keep watchlists and history. */
function dataDir() {
  const fromEnv = (process.env.ABOARD_DATA_DIR || '').trim()
  if (fromEnv) return fromEnv
  const profile = (process.env.DSH_PROFILE_DIR || '').trim() || join(homedir(), '.dsh', 'profiles', 'desktop')
  return join(profile, 'aboard-data')
}

function pythonCandidates() {
  const list = []
  for (const key of ['ABOARD_PYTHON', 'DSH_PYTHON', 'PYTHON']) {
    const value = (process.env[key] || '').trim()
    if (value) list.push(value)
  }
  list.push('python', 'python3', 'py')
  const local = process.env.LOCALAPPDATA
  if (local) {
    for (const version of ['Python313', 'Python312', 'Python311', 'Python310']) {
      list.push(join(local, 'Programs', 'Python', version, 'python.exe'))
    }
  }
  // The runtime that ships beside the Harness executable: usually has no Flask yet, but it is
  // the interpreter a user would install the dependencies into, so it must appear in the list
  // (and in the "install them here" hint) rather than looking like Python is missing.
  try {
    const exeDir = dirname(process.execPath)
    list.push(join(exeDir, 'resources', 'runtime', 'primary-runtime', 'dependencies', 'python', 'python.exe'))
  } catch {
    /* execPath always exists; ignore exotic platforms */
  }
  return [...new Set(list)]
}

/** Shown in the failure card when automatic dependency installation was not possible. */
const PACKAGES = ['flask', 'requests', 'pandas', 'numpy', 'akshare==1.20.37', 'yfinance', 'mini-racer', 'openpyxl']

function installHint(python) {
  if (existsSync(REQUIREMENTS_FILE)) return `${python || 'python'} -m pip install -r "${REQUIREMENTS_FILE}"`
  return `${python || 'python'} -m pip install ${PACKAGES.join(' ')}`
}

const PROBE_SNIPPET = 'import flask, pandas'
const PYTHON_CACHE = 'python.json'

/**
 * Probe one interpreter without blocking the host event loop (spawnSync here used to freeze
 * the whole Harness boot for ~1s while Python imported flask+pandas+akshare).
 */
function probeInterpreter(candidate, timeoutMs = 20000) {
  return new Promise((resolve) => {
    let settled = false
    const finish = (value) => {
      if (settled) return
      settled = true
      resolve(value)
    }
    let child
    try {
      child = spawn(candidate, ['-c', PROBE_SNIPPET], {
        windowsHide: true,
        stdio: ['ignore', 'pipe', 'pipe'],
      })
    } catch (error) {
      finish({ ok: false, status: null, detail: String(error?.message ?? error) })
      return
    }
    let stderr = ''
    child.stderr?.on('data', (chunk) => {
      stderr += chunk
    })
    child.on('error', (error) => finish({ ok: false, status: null, detail: String(error?.message ?? error) }))
    child.on('exit', (code) => finish({ ok: code === 0, status: code, detail: stderr.trim().split('\n').pop() || '' }))
    const timer = setTimeout(() => {
      try {
        child.kill()
      } catch {
        /* ignore */
      }
      finish({ ok: false, status: null, detail: `timeout ${timeoutMs}ms` })
    }, timeoutMs)
    child.on('exit', () => clearTimeout(timer))
  })
}

async function readPythonCache(dir) {
  try {
    return JSON.parse(await readFile(join(dir, PYTHON_CACHE), 'utf8'))
  } catch {
    return null
  }
}

async function writePythonCache(candidate, dir, log) {
  const target = join(dir, PYTHON_CACHE)
  try {
    await writeFile(target, JSON.stringify({ candidate, ts: Date.now() }))
    log(`python cache written: ${target}`)
  } catch (error) {
    log(`python cache write failed: ${target}: ${error?.message ?? error}`)
  }
}

/** First interpreter that can import the dashboard's dependencies (cached across restarts). */
async function resolvePython(log, report, dir) {
  const cache = await readPythonCache(dir)
  const cached = cache?.candidate
  // Bare command names ('python', 'py') are not files; only absolute paths can be checked directly.
  const cachedUsable = !!cached && (isAbsolute(cached) ? existsSync(cached) : true)
  if (cachedUsable && !process.env.ABOARD_FORCE_PROBE && Date.now() - (cache.ts || 0) < 7 * 86400000) {
    log(`python (cached, trusted): ${cached}`)
    return cached
  }
  if (cachedUsable) {
    const probe = await probeInterpreter(cached)
    if (probe.ok) {
      log(`python (cached): ${cached}`)
      return cached
    }
    log(`cached python rejected: ${cached}${probe.detail ? ` (${probe.detail})` : ''}`)
  }
  let last = null
  for (const candidate of pythonCandidates()) {
    if (cached && candidate === cached) continue
    const probe = await probeInterpreter(candidate)
    if (probe.ok) {
      await writePythonCache(candidate, dir, log)
      return candidate
    }
    // A non-zero status still proves the launcher ran: Python exists but lacks imports.
    if (report && probe.status !== null && probe.status !== undefined) report.found = true
    last = { candidate, detail: probe.detail }
    log(`python candidate rejected: ${candidate}${probe.detail ? ` (${probe.detail})` : ''}`)
  }
  if (report) report.last = last
  return null
}

/**
 * Fresh machine bootstrap: no interpreter can import the backend yet. Install the bundled
 * requirements into the first candidate that has pip, keeping only a short tail in the log.
 * The 15 minute cap keeps a stuck download from hanging the waiting page forever; on failure
 * the error card still shows the exact pip command. ABOARD_NO_AUTO_PIP=1 opts out.
 */
async function installDeps(toLog) {
  if (process.env.ABOARD_NO_AUTO_PIP === '1' || !existsSync(REQUIREMENTS_FILE)) return null
  const tried = new Set()
  for (const candidate of pythonCandidates()) {
    if (tried.has(candidate)) continue
    tried.add(candidate)
    let pip
    try {
      pip = spawnSync(candidate, ['-m', 'pip', '--version'], { windowsHide: true, timeout: 20000, encoding: 'utf8' })
    } catch {
      continue
    }
    if (pip.status !== 0) continue
    toLog(`installing backend dependencies with ${candidate}`)
    toLog(`command: ${candidate} -m pip install -r "${REQUIREMENTS_FILE}"`)
    state.stage = '安装 Python 依赖（首次运行，可能需要几分钟）'
    const ok = await new Promise((resolve) => {
      let child
      try {
        child = spawn(
          candidate,
          ['-m', 'pip', 'install', '--disable-pip-version-check', '--no-input', '--progress-bar', 'off', '-r', REQUIREMENTS_FILE],
          {
            windowsHide: true,
            stdio: ['ignore', 'pipe', 'pipe'],
            env: { ...process.env, PYTHONIOENCODING: 'utf-8', PIP_DISABLE_PIP_VERSION_CHECK: '1' },
          },
        )
      } catch (error) {
        toLog(`pip spawn failed: ${error?.message ?? error}`)
        resolve(false)
        return
      }
      let tail = ''
      const onData = (chunk) => {
        tail += chunk.toString('utf8')
        if (tail.length > 8000) tail = tail.slice(-8000)
      }
      child.stdout.on('data', onData)
      child.stderr.on('data', onData)
      const timer = setTimeout(() => {
        toLog('pip install timed out after 15 minutes, terminating')
        try {
          child.kill()
        } catch {
          /* already gone */
        }
      }, 15 * 60 * 1000)
      child.on('error', (error) => {
        clearTimeout(timer)
        toLog(`pip failed: ${error?.message ?? error}`)
        resolve(false)
      })
      child.on('exit', (code) => {
        clearTimeout(timer)
        const lines = tail.trim().split('\n').filter(Boolean)
        if (lines.length) toLog(lines.slice(-6).join('\n'))
        resolve(code === 0)
      })
    })
    if (ok) return candidate
  }
  return null
}

async function pickPort(host = '127.0.0.1') {
  return await new Promise((resolve, reject) => {
    const server = createNetServer()
    server.unref()
    server.on('error', reject)
    server.listen(0, host, () => {
      const address = server.address()
      const port = typeof address === 'object' && address ? address.port : 0
      server.close(() => (port ? resolve(port) : reject(new Error('no port'))))
    })
  })
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function probeHttp(port, path, timeoutMs = 2000) {
  return new Promise((resolve) => {
    const req = httpRequest({ host: '127.0.0.1', port, path, method: 'GET', timeout: timeoutMs }, (res) => {
      res.resume()
      res.on('end', () => resolve(res.statusCode ?? 0))
    })
    req.on('timeout', () => {
      req.destroy()
      resolve(0)
    })
    req.on('error', () => resolve(0))
    req.end()
  })
}

const HOP_BY_HOP = new Set(['connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization', 'te', 'trailer', 'transfer-encoding', 'upgrade'])

function filterHeaders(headers) {
  const out = {}
  for (const [key, value] of Object.entries(headers || {})) {
    if (value === undefined) continue
    if (HOP_BY_HOP.has(key.toLowerCase())) continue
    out[key] = value
  }
  return out
}

function escapeHtml(text) {
  return String(text).replace(/[&<>"]/g, (ch) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[ch]))
}

const WAITING_PAGE = `<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>A股市场看板 · 正在启动</title>
<style>
  html,body{height:100%;margin:0;background:#0a0d13;color:#e8ecf3;
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif}
  .wrap{min-height:100%;display:flex;align-items:center;justify-content:center;flex-direction:column;gap:14px;padding:32px 24px;text-align:center;box-sizing:border-box}
  .dot{width:9px;height:9px;border-radius:50%;background:#4e9cf5;animation:p 1.1s ease-in-out infinite}
  @keyframes p{0%,100%{opacity:.2;transform:scale(.7)}50%{opacity:1;transform:scale(1)}}
  h1{font-size:15px;font-weight:600;margin:0}
  p{margin:0;color:#8b96ab;font-size:12px;line-height:1.7;max-width:560px}
  .err{display:none;width:100%;max-width:620px;text-align:left}
  .err.on{display:block}
  .waiter.off{display:none}
  pre{background:#12161f;border:1px solid #232b3a;border-radius:8px;padding:10px 12px;margin:10px 0 0;
    color:#f0b90b;font-size:11px;line-height:1.6;white-space:pre-wrap;word-break:break-all;max-height:220px;overflow:auto}
  .row{display:flex;gap:8px;justify-content:center;flex-wrap:wrap;margin-top:14px}
  button,a.btn{background:#1b2230;color:#e8ecf3;border:1px solid #2c3a4f;border-radius:6px;
    padding:7px 14px;font-size:12px;cursor:pointer;text-decoration:none;display:inline-block;font-family:inherit}
  button:hover,a.btn:hover{border-color:#4e9cf5;color:#fff}
</style></head>
<body><div class="wrap">
  <div class="waiter" id="waiter">
    <div class="dot" style="margin:0 auto 12px"></div>
    <h1>A股市场看板正在启动…</h1>
    <p id="hint">首次启动需要加载 akshare / pandas 数据后端，通常 5～15 秒；就绪后本页会自动进入看板。</p>
    <p><code id="detail" style="color:#f0b90b;font-size:11px"></code></p>
  </div>
  <div class="err" id="err">
    <h1>看板后端启动失败</h1>
    <p id="errmsg">数据后端没有就绪。请按下面的提示处理后重试。</p>
    <pre id="errdetail"></pre>
    <p style="margin-top:10px">若缺少 Python 依赖，可在命令行安装后点“重试”；插件说明见侧边栏「插件」页里的 dsh-aboard。</p>
    <div class="row">
      <button id="retry">重试</button>
      <button id="copy">复制错误</button>
    </div>
  </div>
</div>
<script>
  var hint = document.getElementById('hint');
  var detail = document.getElementById('detail');
  var waiter = document.getElementById('waiter');
  var errBox = document.getElementById('err');
  var errDetail = document.getElementById('errdetail');
  var errMsg = document.getElementById('errmsg');
  var tries = 0;
  var done = false;
  var lastError = '';
  document.getElementById('retry').addEventListener('click', function () { location.reload(); });
  document.getElementById('copy').addEventListener('click', function () {
    var btn = document.getElementById('copy');
    try {
      navigator.clipboard.writeText(lastError || errDetail.textContent || '');
      btn.textContent = '已复制';
      setTimeout(function () { btn.textContent = '复制错误'; }, 1600);
    } catch (e) { btn.textContent = '复制失败'; }
  });
  function fail(message) {
    done = true;
    lastError = message || '';
    waiter.className = 'waiter off';
    errBox.className = 'err on';
    errDetail.textContent = lastError;
    if (lastError) errMsg.textContent = '数据后端没有就绪，错误信息如下：';
  }
  function poll() {
    if (done) return;
    tries += 1;
    fetch('/aboard/__status', { cache: 'no-store' }).then(function (r) { return r.json(); }).then(function (s) {
      if (s && s.ready) { done = true; location.replace('/aboard'); return; }
      if (s && s.error) { fail(s.error); return; }
      if (tries > 6) hint.textContent = '仍在启动中：' + (s && s.stage ? s.stage : '等待数据后端') + '。首次数十秒属正常，就绪后会自动进入。';
      setTimeout(poll, 1200);
    }).catch(function () { setTimeout(poll, 1500); });
  }
  setTimeout(poll, 400);
</script>
</body></html>`

export const inject = ['webServer']

export const name = 'dsh-aboard'

export function apply(ctx) {
  const log = (...args) => console.log('[dsh-aboard]', ...args)

  const state = {
    ready: false,
    error: null,
    stage: '初始化',
    port: null,
    child: null,
    logfile: null,
    html: null,
    htmlMtime: 0,
    stopped: false,
  }

  const data = dataDir()

  async function seedData() {
    await mkdir(data, { recursive: true })
    let entries = []
    try {
      entries = await readdir(BACKEND_DIR)
    } catch (error) {
      log('cannot read backend dir', error?.message ?? error)
      return
    }
    for (const entry of entries) {
      if (!entry.toLowerCase().endsWith('.json')) continue
      const target = join(data, entry)
      if (existsSync(target)) continue
      try {
        await copyFile(join(BACKEND_DIR, entry), target)
      } catch (error) {
        log(`seed failed for ${entry}:`, error?.message ?? error)
      }
    }
  }

  async function buildHtml() {
    const info = await stat(HTML_FILE)
    let html = await readFile(HTML_FILE, 'utf8')
    state.htmlMtime = info.mtimeMs
    if (html.includes(ECHARTS_CDN)) html = html.split(ECHARTS_CDN).join(`${PREFIX}/echarts.min.js`)
    // The dashboard only ever requests same-origin '/api/...' (single-quoted or template literal);
    // both quoting styles are rewritten so it keeps working under the plugin prefix.
    html = html.split("'/api/").join(`'${PREFIX}/api/`)
    html = html.split('`/api/').join('`' + PREFIX + '/api/')
    // The dashboard is remounted on every panel switch, so remember which of the seven
    // tabs was open instead of dropping the user back on 行情数据 each time.
    html = html.replace('</body>', `${TAB_MEMORY}</body>`)
    return html
  }

  /** 页面模板按 mtime 复用：改了 assets/dashboard.html 后刷新浏览器即可生效，无需重启插件。 */
  async function ensureHtml() {
    try {
      const info = await stat(HTML_FILE)
      if (!state.html || info.mtimeMs !== state.htmlMtime) state.html = await buildHtml()
    } catch (error) {
      log('rebuild dashboard html failed:', error?.message ?? error)
    }
    return state.html
  }

  async function startBackend() {
    const _t0 = Date.now()
    const _ms = () => Date.now() - _t0
    state.stage = '准备数据目录'
    await seedData()

    // Open the log before probing interpreters: a rejected candidate is exactly the
    // information the user needs, and it must survive in backend.log.
    state.stage = '查找 Python 解释器'
    const logfile = await open(join(data, 'backend.log'), 'a')
    state.logfile = logfile
    const toLog = (line) => {
      log(line)
      logfile.write(`[host] ${line}\n`).catch(() => {})
    }

    const report = {}
    let python = await resolvePython(toLog, report, data)
    toLog(`[boot] python resolved in ${_ms()}ms`)
    if (!python) {
      const installed = await installDeps(toLog)
      if (installed) {
        toLog('dependencies installed, probing interpreters again')
        python = await resolvePython(toLog, report, data)
      }
    }
    if (!python) {
      const tried = report.last ? `最后尝试：${report.last.candidate}${report.last.detail ? ` （${report.last.detail}）` : ''}` : ''
      const headline = report.found
        ? '未找到可用的 Python 3：已找到解释器，但缺少 flask / pandas / akshare 等依赖。'
        : '未找到 Python 3 解释器：请先安装 Python 3.10+（安装时勾选 Add Python to PATH），再点“重试”。'
      throw new Error(
        [
          headline,
          report.found ? '插件已尝试自动安装依赖（backend/requirements.txt），但没有成功，详见下方日志。' : '',
          tried,
          `请手动安装：${installHint(report.last?.candidate)}`,
          `或用环境变量 ABOARD_PYTHON 指定一个已装好依赖的 python.exe。`,
          `运行日志：${join(data, 'backend.log')}`,
        ]
          .filter(Boolean)
          .join('\n'),
      )
    }
    toLog(`python: ${python}`)

    state.stage = '分配后端端口'
    const port = await pickPort()
    state.port = port

    state.stage = '启动数据后端'
    const child = spawn(python, [join(BACKEND_DIR, 'server.py')], {
      cwd: BACKEND_DIR,
      windowsHide: true,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: {
        ...process.env,
        PYTHONIOENCODING: 'utf-8',
        PYTHONUNBUFFERED: '1',
        ABOARD_PORT: String(port),
        ABOARD_DATA_DIR: data,
      },
    })
    state.child = child
    toLog(`[boot] backend spawned at +${_ms()}ms`)

    const forward = (stream, tag) => {
      stream.setEncoding('utf8')
      stream.on('data', (chunk) => {
        logfile.write(`[${tag}] ${chunk}`).catch(() => {})
      })
    }
    forward(child.stdout, 'out')
    forward(child.stderr, 'err')

    child.on('exit', (code, signal) => {
      log(`backend exited (code=${code}, signal=${signal})`)
      child.__aboardExited = true
      if (!state.stopped) {
        state.ready = false
        state.error = `数据后端已退出（code=${code}${signal ? `, signal=${signal}` : ''}）。详见 ${join(data, 'backend.log')}`
      }
      logfile.close().catch(() => {})
    })

    state.stage = '等待数据后端就绪'
    const deadline = Date.now() + 90000
    for (;;) {
      if (child.__aboardExited) throw new Error(state.error || '数据后端启动后立即退出')
      const status = await probeHttp(port, '/api/watchlist', 2500)
      if (status === 200) break
      if (Date.now() > deadline) {
        throw new Error(`数据后端在 90 秒内未就绪（日志：${join(data, 'backend.log')}）`)
      }
      await delay(400)
    }

    toLog(`[boot] backend ready in ${_ms()}ms`)
    state.stage = '就绪'
    state.ready = true
    state.error = null
    toLog(`backend ready on 127.0.0.1:${port} (data: ${data})`)
  }

  /** Last few backend.log lines, so a failure card can show the real reason. */
  async function logTail(maxLines = 12) {
    try {
      const text = await readFile(join(data, 'backend.log'), 'utf8')
      const lines = text.trimEnd().split('\n')
      return lines.slice(-maxLines).join('\n')
    } catch {
      return ''
    }
  }

  function stopBackend() {
    state.stopped = true
    const child = state.child
    state.child = null
    state.ready = false
    if (!child || child.__aboardExited) return
    try {
      if (process.platform === 'win32') {
        spawnSync('taskkill', ['/pid', String(child.pid), '/T', '/F'], { windowsHide: true })
      } else {
        child.kill('SIGTERM')
      }
    } catch (error) {
      log('backend stop failed', error?.message ?? error)
    }
  }

  async function sendHtml(res) {
    // While the backend is down the waiting page is the honest answer: it explains the
    // failure (with the log tail) instead of letting the dashboard paint empty charts.
    const html = await ensureHtml()
    const body = state.error || !html ? WAITING_PAGE : html
    res.writeHead(200, {
      'content-type': 'text/html; charset=utf-8',
      'cache-control': 'no-store',
      'content-length': Buffer.byteLength(body),
    })
    res.end(body)
  }

  async function sendFile(res, file, contentType, status = 200) {
    try {
      const info = await stat(file)
      res.writeHead(status, {
        'content-type': contentType,
        'content-length': info.size,
        'cache-control': status === 200 ? 'public, max-age=86400' : 'no-store',
      })
      if (status === 200) {
        createReadStream(file).pipe(res)
      } else {
        res.end()
      }
    } catch {
      res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' })
      res.end('not found')
    }
  }

  async function sendStatus(res) {
    const failed = Boolean(state.error)
    let error = state.error
    if (error) {
      const tail = (await logTail()).trim()
      if (tail) error = `${error}\n\n--- backend.log 末尾 ---\n${tail}`
    }
    const payload = JSON.stringify({
      ready: state.ready && Boolean(state.port),
      error,
      stage: state.stage,
      failed,
    })
    res.writeHead(200, {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
      'content-length': Buffer.byteLength(payload),
    })
    res.end(payload)
  }

  function proxy(req, res, targetPath) {
    if (!state.port) {
      res.writeHead(503, { 'content-type': 'application/json; charset=utf-8' })
      res.end(JSON.stringify({ error: 'aboard backend not started' }))
      return
    }
    const upstream = httpRequest(
      {
        host: '127.0.0.1',
        port: state.port,
        method: req.method,
        path: targetPath,
        headers: { ...filterHeaders(req.headers), host: `127.0.0.1:${state.port}` },
      },
      (up) => {
        res.writeHead(up.statusCode ?? 502, filterHeaders(up.headers))
        up.pipe(res)
      },
    )
    upstream.on('error', (error) => {
      if (!res.headersSent) {
        const payload = JSON.stringify({ error: 'aboard backend unavailable', detail: String(error?.message ?? error) })
        res.writeHead(502, { 'content-type': 'application/json; charset=utf-8', 'content-length': Buffer.byteLength(payload) })
        res.end(payload)
      } else {
        res.destroy()
      }
    })
    req.pipe(upstream)
  }

  async function handler(req, res) {
    let url
    try {
      url = new URL(req.url ?? '/', 'http://aboard.invalid')
    } catch {
      res.writeHead(400, { 'content-type': 'text/plain; charset=utf-8' })
      res.end('bad request')
      return
    }
    const pathname = url.pathname
    const method = (req.method || 'GET').toUpperCase()

    if (pathname === `${PREFIX}/__status`) {
      sendStatus(res)
      return
    }

    if (pathname === `${PREFIX}/api` || pathname.startsWith(`${PREFIX}/api/`)) {
      proxy(req, res, pathname.slice(PREFIX.length) + url.search)
      return
    }

    if (method !== 'GET' && method !== 'HEAD') {
      res.writeHead(405, { allow: 'GET, HEAD', 'content-type': 'text/plain; charset=utf-8' })
      res.end('method not allowed')
      return
    }

    if (pathname === PREFIX || pathname === `${PREFIX}/` || pathname === `${PREFIX}/index.html`) {
      await sendHtml(res)
      return
    }

    if (pathname === `${PREFIX}/dashboard.html`) {
      await sendHtml(res)
      return
    }

    if (pathname === `${PREFIX}/echarts.min.js`) {
      await sendFile(res, ECHARTS_FILE, 'application/javascript; charset=utf-8')
      return
    }

    res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' })
    res.end('not found')
  }

  ctx.effect(
    () =>
      ctx.webServer.register({
        kind: 'prefix',
        path: PREFIX,
        handler,
      }),
    'dsh-aboard: web routes',
  )

  ctx.effect(() => {
    state.stopped = false
    state.stage = '编译看板页面'
    buildHtml()
      .then((html) => {
        state.html = html
        return startBackend()
      })
      .catch((error) => {
        state.ready = false
        state.error = String(error?.message ?? error)
        state.stage = '启动失败'
        log('startup failed:', state.error)
        if (error?.stack) log(error.stack)
      })
    return () => stopBackend()
  }, 'dsh-aboard: backend lifecycle')

  log(`mounted (assets: ${ASSETS_DIR}, data: ${data})`)
}
