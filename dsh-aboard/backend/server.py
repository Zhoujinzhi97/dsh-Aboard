# -*- coding: utf-8 -*-
"""A股市场看板 - 本地数据服务 (AkShare + yfinance)"""
import traceback, time, os, json, shutil, threading, re
import base64, hashlib, hmac, uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from flask import Flask, jsonify, send_from_directory, request
import akshare as ak
import pandas as pd

# ===== 修复 py_mini_racer 并发崩溃 =====
# akshare 部分接口（上交所/深交所成交、ETF 份额等）用 py_mini_racer 执行 JS 解密。
# 该库在 Windows 下重复或并发创建 V8 isolate 会直接 FATAL 崩掉整个进程：
#   [FATAL:partition_address_space.cc(243)] Check failed: !IsConfigurablePoolInitialized()
# 一旦崩溃，浏览器侧所有请求变成 ERR_CONNECTION_RESET，表现为"K线加载失败、页面全部空白"。
# 这里改为进程内复用同一个 V8 实例，并把 eval/call 串行化，杜绝并发初始化。
try:
    import py_mini_racer as _pmr
    _PMR_LOCK = threading.RLock()
    _PMR_INST = [None]
    _OriginMiniRacer = _pmr.MiniRacer

    class _SafeMiniRacer:
        """py_mini_racer.MiniRacer 的线程安全单例替身"""

        def __init__(self, *a, **k):
            with _PMR_LOCK:
                if _PMR_INST[0] is None:
                    _PMR_INST[0] = _OriginMiniRacer(*a, **k)
            self.__dict__['_ctx'] = _PMR_INST[0]

        def eval(self, *a, **k):
            with _PMR_LOCK:
                return self._ctx.eval(*a, **k)

        def call(self, *a, **k):
            with _PMR_LOCK:
                return self._ctx.call(*a, **k)

        def __getattr__(self, item):
            ctx = self.__dict__.get('_ctx')
            if ctx is None:
                raise AttributeError(item)
            return getattr(ctx, item)

    _pmr.MiniRacer = _SafeMiniRacer
    try:
        import py_mini_racer.py_mini_racer as _pmr_inner
        _pmr_inner.MiniRacer = _SafeMiniRacer
    except Exception:
        pass
except Exception:
    pass

app = Flask(__name__, static_folder='.', static_url_path='')

# 全局：分别标记不同数据的保存状态，适配恐贪更新晚的场景
_etf_save_date = None  # ETF/成交额等常规收盘数据，当日仅保存一次
_etf_last_pending = None  # 待确认的临时值，连续两次一致才保存，避免盘中变动值
_etf_fail_streak = 0  # ETF 数据源连续失败次数，用于失败退避
_etf_next_retry = 0.0  # 失败退避窗口结束时间戳
# 数据目录：DSH 插件宿主通过 ABOARD_DATA_DIR 指定可写目录，独立运行时就是脚本所在目录
BASE_DIR = os.environ.get('ABOARD_DATA_DIR', '').strip() or os.path.dirname(os.path.abspath(__file__))

_trade_dates_cache = None
_trade_dates_fail_ts = 0.0

def _trade_date_set():
    """真实交易日集合；取不到日历返回 None。

    审计教训：以前这里 except 直接 `return dt.weekday() < 5`，把 57 个法定节假日
    当成交易日，回填任务每 30 分钟对 57 个不存在数据的日子各发 2 个请求，
    日志永远是「待补 57、新增 0」。依赖交易日历做批量回填的地方（成交额回填等）
    必须用本函数并在返回 None 时整轮跳过，宁可不补也不能空跑。
    """
    global _trade_dates_cache, _trade_dates_fail_ts
    if _trade_dates_cache is not None:
        return _trade_dates_cache
    if _trade_dates_fail_ts and time.time() - _trade_dates_fail_ts < 600:
        return None
    try:
        cal = ak.tool_trade_date_hist_sina()
        # akshare 不同版本 trade_date 可能是 datetime 也可能是字符串，统一转一次。
        vals = pd.to_datetime(cal['trade_date'].astype(str)).dt.strftime('%Y-%m-%d')
        _trade_dates_cache = set(vals)
        print('[calendar] 交易日历已加载 %d 天（%s ~ %s）'
              % (len(_trade_dates_cache), min(_trade_dates_cache), max(_trade_dates_cache)))
        return _trade_dates_cache
    except Exception as e:
        _trade_dates_fail_ts = time.time()
        print('[calendar] 交易日历获取失败，跳过本轮批量回填: %r' % (e,))
        return None

def is_trading_day(dt=None):
    """判断是否为A股交易日：日历可用时严格按日历，不可用时退回工作日（仅用于盘中判断）。"""
    dt = dt or datetime.now()
    s = _trade_date_set()
    if s is None:
        return dt.weekday() < 5
    return dt.strftime('%Y-%m-%d') in s

def is_after_close(dt=None):
    """判断当前是否已过收盘结算时间（15:30）"""
    dt = dt or datetime.now()
    return is_trading_day(dt) and (dt.hour > 15 or (dt.hour == 15 and dt.minute >= 30))

def _session_progress(dt=None):
    """A股当日交易进度（0~1）：09:30-11:30、13:00-15:00 共 240 分钟。

    开盘前返回 0，收盘后（含非交易日）返回 1，午休按上午已走完计。
    用于把「盘中累计成交额」折算成「全天口径」后再算历史分位，
    否则拿半天/一小时的累计值直接和全天历史值比较，分位会系统性偏低。
    """
    dt = dt or datetime.now()
    if not is_trading_day(dt):
        return 1.0
    mins = dt.hour * 60 + dt.minute
    open_m, am_close, pm_open, close_m = 570, 690, 780, 900  # 09:30 / 11:30 / 13:00 / 15:00
    if mins <= open_m:
        return 0.0
    if mins >= close_m:
        return 1.0
    if mins <= am_close:
        elapsed = mins - open_m
    elif mins < pm_open:
        elapsed = am_close - open_m
    else:
        elapsed = (am_close - open_m) + (mins - pm_open)
    return max(0.0, min(1.0, elapsed / 240.0))

_save_locks = {}
_save_locks_guard = threading.Lock()

def _save_lock_for(file_path):
    """按文件路径取一把可重入锁。

    并发问题（审计实测）：6 个并发 POST /api/watchlist → 3 个 500，只剩 1 份数据；
    6 个并发 POST /api/bt_config → 全 200 但 5 份静默丢失。根因是「读-改-写」整份
    文件没有互斥，再加上 os.replace 撞上另一个线程持句柄时的 WinError 32。
    """
    key = os.path.abspath(file_path).lower()
    with _save_locks_guard:
        lock = _save_locks.get(key)
        if lock is None:
            lock = threading.RLock()
            _save_locks[key] = lock
        return lock

def safe_json_save(file_path, data):
    """原子化保存JSON，避免写坏文件，先写临时文件再替换。

    注意：Windows 上 shutil.move 目标已存在时会退化成 copy+unlink，
    中途被读到就会拿到截断的半份 JSON。os.replace 是真正的原子替换。
    另外 os.replace 在目标被其它线程持有句柄时会抛 PermissionError(WinError 32)，
    所以替换这一步带少量重试。
    """
    lock = _save_lock_for(file_path)
    with lock:
        tmp_path = '%s.%d.%d.tmp' % (file_path, os.getpid(), threading.get_ident())
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        last = None
        for attempt in range(6):
            try:
                os.replace(tmp_path, file_path)
                return
            except PermissionError as e:
                # 另一个线程/进程正持有目标句柄，稍等再试
                last = e
                time.sleep(0.05 * (attempt + 1))
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise last if last is not None else OSError('save failed: %s' % file_path)


from flask import Response
def json_dumps(data):
    return json.dumps(data, ensure_ascii=False).encode('utf-8')

# ==================== 通用 HTTP 工具（统一 UA / Referer / 重试） ====================
import urllib.request as _urlreq

_UA_DESKTOP = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
               '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

def http_get(url, referer=None, encoding='utf-8', timeout=12, retry=2, headers=None):
    """带重试的 GET，返回解码后的文本。失败抛异常，由调用方决定降级。"""
    last = None
    hd = {'User-Agent': _UA_DESKTOP, 'Accept': '*/*', 'Connection': 'close'}
    if referer:
        hd['Referer'] = referer
    if headers:
        hd.update(headers)
    for i in range(max(1, retry)):
        try:
            req = _urlreq.Request(url, headers=hd)
            with _urlreq.urlopen(req, timeout=timeout) as r:
                raw = r.read()
            try:
                return raw.decode(encoding)
            except UnicodeDecodeError:
                return raw.decode(encoding, 'ignore')
        except Exception as e:
            last = e
            if i + 1 < max(1, retry):
                time.sleep(0.6)
    raise last if last else RuntimeError('http_get failed')

def http_get_json(url, referer=None, timeout=12, retry=2, headers=None):
    txt = http_get(url, referer=referer, timeout=timeout, retry=retry, headers=headers)
    return json.loads(txt)

# ==================== 两市成交额历史（沪深交易所官方日终，多源+磁盘缓存） ====================
_amt_series_cache = {'ts': 0.0, 'map': {}}
_TURN_CACHE_FILE = os.path.join(BASE_DIR, 'turnover_hist.json')


def _turn_row(day):
    """单个交易日 (沪市, 深市) 股票成交额，单位亿元。

    数据源：上交所「每日股票成交概况」+ 深交所「市场总貌」官方日终数据。
    非交易日两个接口都会报错，此时返回 None —— 不会产生重复或串日记录。
    两市必须都拿到才算一条完整记录，避免半截数据把百分位压低。
    """
    d8 = str(day).replace('-', '')
    sh = sz = None
    try:
        df = ak.stock_sse_deal_daily(date=d8)
        first = df.columns[0]
        hit = df[df[first].astype(str).str.strip() == '成交金额']
        if len(hit):
            sh = float(hit['股票'].iloc[0])
    except Exception:
        sh = None
    try:
        df2 = ak.stock_szse_summary(date=d8)
        hit2 = df2[df2['证券类别'].astype(str).str.strip() == '股票']
        if len(hit2):
            sz = float(hit2['成交金额'].iloc[0]) / 1e8
    except Exception:
        sz = None
    if sh is None or sz is None:
        return None
    return [round(sh, 2), round(sz, 2)]


def _turn_rows_load():
    """读磁盘：返回 ({'2026-11-10': [沪市亿, 深市亿], ...}, {'2022-01-04', ...})"""
    try:
        with open(_TURN_CACHE_FILE, 'r', encoding='utf-8') as f:
            d = json.load(f)
        if isinstance(d.get('rows'), dict):
            neg = d.get('neg') if isinstance(d.get('neg'), list) else []
            return dict(d['rows']), set(str(x) for x in neg)
    except Exception:
        pass
    return {}, set()


def _turn_rows_save(rows, neg=None):
    try:
        safe_json_save(_TURN_CACHE_FILE, {
            'v': 3, 'ts': time.time(),
            'updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'rows': rows,
            'neg': sorted(neg or [])})
    except Exception:
        pass


_turn_bf = {'running': False, 'last': 0.0}
# 负缓存：源站确认取不到、且已过 7 天的日期，不再反复重试（例如沪市源 2022 年之前无数据）
_TURN_NEG_KEEP_DAYS = 7


def _turn_backfill_async(target_days=1500):
    """后台增量回填最近 target_days 个自然日的两市成交额。

    每个交易日 2 个请求、6 线程并发；已存在的日期和负缓存日期跳过，
    因此日常运行只补最近几天。回填期间接口照常返回已有部分，不阻塞页面。
    """
    now = time.time()
    if _turn_bf['running'] or now - _turn_bf['last'] < 1800:
        return
    _turn_bf['running'] = True
    _turn_bf['last'] = now

    def _worker():
        global _amt_series_cache
        try:
            cal = _trade_date_set()
            if cal is None:
                print('[成交额回填] 交易日历不可用，本轮跳过（不再把节假日当交易日空跑）')
                return
            rows, neg = _turn_rows_load()
            miss = []
            # 从昨天往前补：当天盘中官方日终数据还没出，交给实时行情口径处理
            for i in range(1, target_days + 1):
                dt = datetime.now() - timedelta(days=i)
                d = dt.strftime('%Y-%m-%d')
                if d in rows or d in neg:
                    continue
                if d not in cal:
                    continue
                miss.append(d)
            if not miss:
                print('[成交额回填] 无需补数（已覆盖 %d 个交易日）' % len(rows))
                return
            print('[成交额回填] 待补 %d 个交易日，开始…' % len(miss))
            from concurrent.futures import ThreadPoolExecutor
            new = 0
            cutoff = (datetime.now() - timedelta(days=_TURN_NEG_KEEP_DAYS)).strftime('%Y-%m-%d')
            with ThreadPoolExecutor(max_workers=6) as ex:
                for d, val in zip(miss, ex.map(_turn_row, miss)):
                    if val:
                        rows[d] = val
                        new += 1
                    elif d < cutoff:
                        neg.add(d)   # 老日期拿不到就记住，避免每 30 分钟重试
            _turn_rows_save(rows, neg)
            print('[成交额回填] 完成，新增 %d 条，失败 %d 条，共 %d 条（%s ~ %s）'
                  % (new, len(miss) - new, len(rows), min(rows), max(rows)))
            _amt_series_cache = {'ts': 0.0, 'map': {}}   # 失效内存缓存
        except Exception:
            traceback.print_exc()
        finally:
            _turn_bf['running'] = False
            _turn_bf['last'] = time.time()

    threading.Thread(target=_worker, daemon=True).start()


# ---- 长历史成交额（腾讯指数日线，2015 年以前也有） ----
# 交易所官方「股票成交额」接口只回溯到 2022-10 左右，而回测 5y/10y 需要更早的
# 两市成交额。腾讯指数日线（sh000001 / sz399106）的 amount 字段就是全市场成交额，
# 与官方日终值的偏差在 0.1%~0.2%（盘后固定价格/大宗计入口径差异），用于回测分位足够。
# 官方数据仍然优先（turnover_hist.json 中已有的日期不会被覆盖）。
_TX_TURN_FILE = os.path.join(BASE_DIR, 'turnover_tx.json')
_tx_turn_cache = {'ts': 0.0, 'rows': None}
_tx_turn_bf = {'running': False}


def _tx_turn_load():
    """读腾讯长历史成交额缓存（内存 12h；文件 7 天有效）。"""
    now = time.time()
    if _tx_turn_cache['rows'] is not None and now - _tx_turn_cache['ts'] < 12 * 3600:
        return _tx_turn_cache['rows']
    rows = None
    try:
        if os.path.exists(_TX_TURN_FILE):
            with open(_TX_TURN_FILE, 'r', encoding='utf-8') as f:
                d = json.load(f)
            if now - float(d.get('ts') or 0) < 7 * 86400:
                rows = {str(k): float(v) for k, v in (d.get('rows') or {}).items()}
    except Exception:
        traceback.print_exc()
    if rows is None:
        rows = {}
    if rows:
        _tx_turn_cache.update(ts=now, rows=rows)
    return rows


def _tx_turn_refresh_async():
    """后台拉取/刷新腾讯指数长历史成交额（两次请求≈30s，只在缓存过期时跑）。"""
    if _tx_turn_bf['running']:
        return
    now = time.time()
    try:
        if os.path.exists(_TX_TURN_FILE):
            with open(_TX_TURN_FILE, 'r', encoding='utf-8') as f:
                if now - float(json.load(f).get('ts') or 0) < 7 * 86400:
                    return
    except Exception:
        pass

    def _worker():
        try:
            sh = ak.stock_zh_index_daily_tx(symbol='sh000001')
            sz = ak.stock_zh_index_daily_tx(symbol='sz399106')
            shm = {str(r.date): float(r.amount) / 1e8 for r in sh.itertuples()
                   if float(r.amount or 0) > 0}
            szm = {str(r.date): float(r.amount) / 1e8 for r in sz.itertuples()
                   if float(r.amount or 0) > 0}
            rows = {d: round(shm[d] + szm[d], 2) for d in shm
                    if d in szm and d >= '2014-01-01'}
            if rows:
                safe_json_save(_TX_TURN_FILE, {'ts': time.time(),
                                               'updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                               'rows': rows})
                _tx_turn_cache.update(ts=time.time(), rows=rows)
                _amt_series_cache['ts'] = 0.0
                print('[成交额] 腾讯长历史已加载 %d 条（%s ~ %s）'
                      % (len(rows), min(rows), max(rows)))
        except Exception:
            traceback.print_exc()
        finally:
            _tx_turn_bf['running'] = False

    _tx_turn_bf['running'] = True
    threading.Thread(target=_worker, daemon=True).start()


def _turnover_amount_map(force=False):
    """两市成交额历史 {日期: 亿元}，口径 = 沪市 + 深市 全部股票成交额（官方日终）。

    官方日终（turnover_hist.json）优先；更早的日期用腾讯指数长历史补（见上）。
    为什么不再用上证指数成交量：上证综指 volume 是"沪市一只指数的成交量（股）"，
    与"两市成交额（元）"既不同市场也不同单位，拿它算历史百分位会系统性偏错，
    量级差 3 个数量级时百分位基本随机，屏幕上就会长期停在 50 附近。
    """
    global _amt_series_cache
    if not force and _amt_series_cache['map'] and time.time() - _amt_series_cache['ts'] < 6 * 3600:
        return _amt_series_cache['map']
    rows, _neg = _turn_rows_load()
    agg = {}
    for d, v in (rows or {}).items():
        try:
            agg[d] = round(float(v[0]) + float(v[1]), 2)
        except Exception:
            continue
    tx = _tx_turn_load()
    added = 0
    for d, v in tx.items():
        if d not in agg and v > 0:
            agg[d] = v
            added += 1
    if agg:
        _amt_series_cache = {'ts': time.time(), 'map': agg}
        if added:
            print('[成交额] 已用腾讯长历史补 %d 个更早的交易日（共 %d 条）'
                  % (added, len(agg)))
    _turn_backfill_async()
    _tx_turn_refresh_async()
    return _amt_series_cache['map']


def _percentile_below(values, cur):
    """历史分位（0-100）：低于当前值的样本占比。values 需已按时间升序、且不含当日。"""
    n = len(values)
    if n <= 0:
        return None
    below = sum(1 for v in values if v < cur)
    return min(100, max(0, round(below / n * 100, 0)))


def _pct_position(values, cur):
    """区间位置（0-100）：当前值在此前窗口「最低~最高」之间的相对位置。

    与 _percentile_below（低于当前值的样本占比）不同，这里直接按极值归一：
    (cur - min) / (max - min)。成交额这类右偏分布用排名分位会系统性偏高
    （3 年窗口里大量低量日把中位值拉低），用户口径就是「占三年内最高值和最低值的百分比」。
    """
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    lo, hi = min(vals), max(vals)
    if hi <= lo:
        return 50
    return min(100, max(0, round((cur - lo) / (hi - lo) * 100, 0)))


def _rolling_pos_pct(values_map, window, min_samples=60):
    """按日期升序计算「当日值在此前 window 个交易日的最高~最低区间位置」（不含当日）。"""
    out = {}
    past = []
    for d in sorted(values_map.keys()):
        v = values_map[d]
        if v is not None and len(past) >= min_samples:
            out[d] = _pct_position(past, v)
        past.append(v)
        if len(past) > window:
            past.pop(0)
    return out

# ========== 云同步已移除（2026-11-11） ==========
# 历史版本提供过「云文档备份下载 / 同步目录」能力，已按需求整体删除；
# sync_file 保留为空实现，仅为兼容老代码里的零星调用。
def _static_rank_map(values_map, window=None, min_samples=60):
    """静态窗口排名分位（原版口径，0~100）。

    取「截止最新」的最近 window 个样本作为固定分布，对每个日期取
    「低于当前值的样本占比」×100。窗口内样本不足 min_samples 返回 {}。

    这是原版 /api/bt_backtest 的做法（成交额 1200、融资/ETF 近三年），
    分布接近均匀，不会在单边趋势里长成片的 0/100。
    """
    dates = sorted(values_map)
    if not dates:
        return {}
    win_dates = dates[-window:] if (window and len(dates) > window) else dates
    vals = sorted(v for v in (values_map.get(d) for d in win_dates) if v is not None)
    n = len(vals)
    if n < min_samples:
        return {}
    out = {}
    # 与原版一致：只对「窗口内」样本日期输出分位，窗口外由调用方回落 50。
    for d in win_dates:
        v = values_map.get(d)
        if v is None:
            continue
        below = sum(1 for x in vals if x < v)
        out[d] = min(100.0, max(0.0, below / max(n - 1, 1) * 100))
    return out


def sync_file(filename):
    return


@app.after_request
def add_cors(resp):
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
    resp.headers['Access-Control-Allow-Headers'] = '*'
    return resp

INDEX_MAP = {
    'sh000001': 'sh000001', 'sz399001': 'sz399001', 'sz399006': 'sz399006',
    'sh000688': 'sh000688', 'sh000016': 'sh000016', 'sh000300': 'sh000300', 'sh000852': 'sh000852',
}
FUT_MAP = {'nf_IF0': 'IF0', 'nf_IH0': 'IH0', 'nf_IM0': 'IM0'}
GLOBAL_MAP = {'hf_OIL': 'CL', 'hf_GC': 'GC', 'hf_SI': 'SI'}
US_MAP = {
    'usNDX': ('.NDX', 'index_us_stock_sina'),
    'usIXIC': ('.IXIC', 'index_us_stock_sina'),
    'usSOXX': ('SOXX', 'stock_us_daily'),
}
# yfinance 代码映射（全球指数/外汇/商品期货）
YF_MAP = {
    'yf_N225': '^N225',      # 日经225
    'yf_KS11': '^KS11',      # 韩国综合
    'yf_USDCNY': 'USDCNY=X', # 离岸人民币
    'yf_GC': 'GC=F',         # 纽约黄金
    'yf_CL': 'CL=F',         # WTI原油
    'yf_SI': 'SI=F',         # 纽约白银
}

def df_to_kline(df):
    dates, ohlc, closes, vols = [], [], [], []
    for _, row in df.iterrows():
        d = row.get('date', row.get('day', row.iloc[0]))
        if hasattr(d, 'strftime'):
            d = d.strftime('%Y-%m-%d %H:%M')
        dates.append(str(d))
        o = round(float(row['open']), 2)
        c = round(float(row['close']), 2)
        h = round(float(row['high']), 2)
        l = round(float(row['low']), 2)
        v = float(row['volume']) if 'volume' in row else 0
        ohlc.append([o, c, h, l])
        closes.append(c)
        vols.append(v)
    return {'code': 0, 'dates': dates, 'ohlc': ohlc, 'closes': closes, 'volumes': vols}

def fill_full_day_minute(df, date_col='day'):
    """把当日1分钟K线补齐为完整交易日时间轴(9:31-15:00)，未到时间留空"""
    import datetime as _dt
    today = datetime.now().strftime('%Y-%m-%d')
    # 生成完整交易日时间点（每分钟一根）
    times = []
    base = _dt.datetime(2000, 1, 1, 9, 30)
    for m in range(120):  # 9:31-11:30
        t = base + _dt.timedelta(minutes=m+1)
        times.append(today + ' ' + t.strftime('%H:%M'))
    base2 = _dt.datetime(2000, 1, 1, 13, 0)
    for m in range(120):  # 13:00-14:59
        t = base2 + _dt.timedelta(minutes=m)
        times.append(today + ' ' + t.strftime('%H:%M'))
    # 把已有数据按时间索引
    existing = {}
    for _, row in df.iterrows():
        d = row.get(date_col, row.iloc[0])
        if hasattr(d, 'strftime'):
            d = d.strftime('%Y-%m-%d %H:%M')
        else:
            d = str(d)[:16]
        existing[d] = row
    # 构建完整数组
    dates, ohlc, closes, vols = [], [], [], []
    for t in times:
        dates.append(t)
        if t in existing:
            row = existing[t]
            o = round(float(row['open']), 2)
            c = round(float(row['close']), 2)
            h = round(float(row['high']), 2)
            l = round(float(row['low']), 2)
            v = float(row['volume']) if 'volume' in row else 0
            ohlc.append([o, c, h, l])
            closes.append(c)
            vols.append(v)
        else:
            ohlc.append(None)
            closes.append(None)
            vols.append(None)
    return {'code': 0, 'dates': dates, 'ohlc': ohlc, 'closes': closes, 'volumes': vols}

def yf_kline(symbol):
    """用 yfinance 获取全球品种日K，6小时文件缓存"""
    import json as _json
    cache_dir = os.path.join(BASE_DIR, 'kline_cache')
    os.makedirs(cache_dir, exist_ok=True)
    safe = symbol.replace('=','_').replace('^','').replace('/','_')
    cache_file = os.path.join(cache_dir, f'{safe}.json')

    # 1分钟缓存
    if os.path.exists(cache_file):
        age = time.time() - os.path.getmtime(cache_file)
        if age < 60:
            try:
                with open(cache_file, 'r') as f:
                    return _json.load(f)
            except:
                pass

    import yfinance as yf
    import time as _time
    for attempt in range(3):
        try:
            t = yf.Ticker(symbol)
            h = t.history(period='1y')
            if len(h) > 0:
                break
        except Exception as e:
            if attempt == 2:
                raise
            _time.sleep(2)
    rows = []
    for idx, row in h.iterrows():
        d = idx.strftime('%Y-%m-%d')
        rows.append({
            'date': d,
            'open': round(row['Open'], 2),
            'high': round(row['High'], 2),
            'low': round(row['Low'], 2),
            'close': round(row['Close'], 2),
            'volume': float(row['Volume']) if pd.notna(row['Volume']) else 0,
        })
    df = pd.DataFrame(rows)
    result = df_to_kline(df)
    # 写缓存
    try:
        with open(cache_file, 'w') as f:
            _json.dump(result, f)
    except:
        pass
    return result

@app.route('/')
def index():
    return send_from_directory('.', 'A股市场看板.html')

def _tencent_minute(sym, period):
    """从腾讯ifzq获取指数分钟数据"""
    import requests as _req
    try:
        if period == '1':
            url = f'https://web.ifzq.gtimg.cn/appstock/app/minute/query?code={sym}'
            r = _req.get(url, timeout=8)
            j = r.json()
            d = j['data'][sym]
            # 结构: d['data']['data'] = ["0930 price vol amt", ...]
            inner = d.get('data', {})
            mins = inner.get('data', []) if isinstance(inner, dict) else []
            trade_date = inner.get('date', '') if isinstance(inner, dict) else ''
            rows = []
            prev_vol = 0
            for m in mins:
                parts = m.split(' ')
                if len(parts) < 2: continue
                t = parts[0]
                price = float(parts[1])
                cum_vol = float(parts[2]) if len(parts) > 2 else 0
                # 腾讯返回的是累计成交量，转为每分钟成交量
                min_vol = max(0, cum_vol - prev_vol)
                prev_vol = cum_vol
                rows.append({'day': f"{trade_date} {t}", 'open': price, 'high': price, 'low': price, 'close': price, 'volume': min_vol})
            import pandas as _pd
            return _pd.DataFrame(rows)
        else:
            mk = 'm' + period
            url = f'https://web.ifzq.gtimg.cn/appstock/app/kline/mkline?param={sym},{mk},,320'
            r = _req.get(url, timeout=8)
            j = r.json()
            d = j['data'][sym]
            rows_raw = d.get(mk, [])
            rows = []
            for r0 in rows_raw:
                rows.append({'day': r0[0], 'open': float(r0[1]), 'high': float(r0[3]), 'low': float(r0[4]), 'close': float(r0[2]), 'volume': float(r0[5]) if len(r0) > 5 else 0})
            import pandas as _pd
            return _pd.DataFrame(rows)
    except Exception as e:
        traceback.print_exc()
        return None

@app.route('/api/kline/<code>')
def kline(code):
    period = request.args.get('period', 'day')
    try:
        # A股指数
        if code in INDEX_MAP:
            sym = INDEX_MAP[code]
            if period in ('1', '5', '15', '30', '60'):
                # 先试akshare，失败/空则用腾讯
                df = None
                try:
                    df = ak.stock_zh_a_minute(symbol=sym, period=period)
                    if period == '1':
                        today = datetime.now().strftime('%Y-%m-%d')
                        df = df[df['day'].astype(str).str.startswith(today)]
                    else:
                        df = df.tail(500)
                    if df is None or len(df) == 0:
                        df = None
                except:
                    df = None
                if df is None:
                    df = _tencent_minute(sym, period)
                if df is None or len(df) == 0:
                    return jsonify({'code': 0, 'dates': [], 'ohlc': [], 'closes': [], 'volumes': []})
                return jsonify(df_to_kline(df))
            elif period == '120':
                df = None
                try:
                    df = ak.stock_zh_a_minute(symbol=sym, period='60')
                    df = df.tail(500)
                except:
                    df = None
                if df is None or len(df) == 0:
                    df = _tencent_minute(sym, '60')
                return jsonify(df_to_kline(df))
            else:
                df = ak.stock_zh_index_daily(symbol=sym)
                df = df.tail(300)
                return jsonify(df_to_kline(df))

        # 期货
        if code in FUT_MAP:
            sym = FUT_MAP[code]
            if period in ('1', '5', '15', '30', '60'):
                df = ak.futures_zh_minute_sina(symbol=sym, period=period)
                if period == '1':
                    # 取最近交易日的数据（周末显示周五数据）
                    df['date_only'] = df['datetime'].astype(str).str[:10]
                    last_day = df['date_only'].iloc[-1]
                    df = df[df['date_only'] == last_day].drop(columns=['date_only'])
                else:
                    df = df.tail(500)
                return jsonify(df_to_kline(df))
            elif period == '120':
                df = ak.futures_zh_minute_sina(symbol=sym, period='60')
                df = df.tail(500)
                return jsonify(df_to_kline(df))
            else:
                df = ak.futures_zh_daily_sina(symbol=sym)
                df = df.tail(300)
                # 追加今日实时K线（从1分钟数据拼接）
                try:
                    import pandas as _pd
                    min_df = ak.futures_zh_minute_sina(symbol=sym, period='1')
                    min_df['date_only'] = min_df['datetime'].astype(str).str[:10]
                    last_day = min_df['date_only'].iloc[-1]
                    today_df = min_df[min_df['date_only'] == last_day].drop(columns=['date_only'])
                    if len(today_df) > 0:
                        today_row = _pd.DataFrame([{
                            'date': last_day,
                            'open': float(today_df.iloc[0]['open']),
                            'high': float(today_df['high'].max()),
                            'low': float(today_df['low'].min()),
                            'close': float(today_df.iloc[-1]['close']),
                            'volume': float(today_df['volume'].sum()),
                        }])
                        if str(df.iloc[-1]['date'])[:10] == str(last_day)[:10]:
                            df = df.iloc[:-1]
                        df = _pd.concat([df, today_row], ignore_index=True)
                except Exception as e:
                    traceback.print_exc()
                return jsonify(df_to_kline(df))

        # 美股指数/ETF
        if code in US_MAP:
            sym, fn_name = US_MAP[code]
            fn = getattr(ak, fn_name)
            df = fn(symbol=sym)
            df = df.tail(300)
            return jsonify(df_to_kline(df))

        # 外盘商品（AkShare）
        if code in GLOBAL_MAP:
            df = ak.futures_foreign_hist(symbol=GLOBAL_MAP[code])
            df = df.tail(300)
            return jsonify(df_to_kline(df))

        # yfinance 全球品种
        if code in YF_MAP:
            # 日经和韩国用新浪接口（yfinance被限流）
            if code == 'yf_N225':
                df = ak.index_global_hist_sina(symbol="日经225指数")
                df = df.tail(300)
                return jsonify(df_to_kline(df))
            if code == 'yf_KS11':
                df = ak.index_global_hist_sina(symbol="首尔综合指数")
                df = df.tail(300)
                return jsonify(df_to_kline(df))
            j = yf_kline(YF_MAP[code])
            return jsonify(j)

        return jsonify({'code': -1, 'msg': 'unknown: ' + code})
    except Exception as e:
        traceback.print_exc()
        return jsonify({'code': -2, 'msg': str(e)}), 500

# ================== 个股分析 ==================
WATCHLIST_FILE = os.path.join(BASE_DIR, 'watchlist.json')
_stock_cache = None  # 股票列表缓存

def _get_stock_list():
    """用腾讯搜索接口获取股票列表（轻量，不需要全市场快照）"""
    global _stock_cache
    if _stock_cache is not None:
        return _stock_cache
    # 先用akshare拉一次全市场代码列表，缓存到内存
    try:
        df = ak.stock_zh_a_spot_em()
        _stock_cache = []
        for _, row in df.iterrows():
            _stock_cache.append({
                'code': row['代码'],
                'name': row['名称'],
                'price': float(row['最新价']) if pd.notna(row['最新价']) else 0,
                'pct': float(row['涨跌幅']) if pd.notna(row['涨跌幅']) else 0,
            })
        return _stock_cache
    except Exception as e:
        traceback.print_exc()
        return []

@app.route('/api/stock_search')
def stock_search():
    """搜索A股股票（代码或名称）- 用腾讯搜索接口，秒级响应"""
    kw = request.args.get('q', '').strip()
    if not kw:
        return jsonify({'code': 0, 'results': []})
    try:
        import requests
        # 腾讯智能搜索接口
        url = f'https://smartbox.gtimg.cn/s3/?v=2&q={kw}&t=all'
        headers = {
            'Referer': 'https://gu.qq.com/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        resp = requests.get(url, headers=headers, timeout=5)
        resp.encoding = 'gbk'
        text = resp.text
        # 解析: v_hint="sh~600519~贵州茅台~gzmt~GP-A^sz~000001~平安银行~payh~GP-A^..."
        results = []
        if 'v_hint=' in text:
            raw = text.split('v_hint="')[1].split('"')[0]
            items = raw.split('^')
            for item in items[:20]:
                parts = item.split('~')
                if len(parts) >= 3:
                    market = parts[0]  # sh / sz / hk / jj
                    code = parts[1]
                    name = parts[2]
                    # 解码unicode转义
                    try:
                        name = name.encode('latin-1').decode('unicode_escape')
                    except:
                        pass
                    # 只保留A股
                    if market in ('sh', 'sz') and code.isdigit():
                        results.append({'code': code, 'name': name, 'price': 0, 'pct': 0, 'market': market})
        # 批量查实时行情（腾讯行情接口）
        if results:
            try:
                codes = ','.join([r['market'] + r['code'] for r in results])
                qurl = f'https://qt.gtimg.cn/q={codes}'
                qresp = requests.get(qurl, timeout=5)
                qresp.encoding = 'gbk'
                for line in qresp.text.strip().split(';'):
                    if '=' not in line: continue
                    var = line.split('=')[0].strip()  # v_sh600519
                    data = line.split('="')[1].rstrip('"') if '="' in line else ''
                    parts = data.split('~')
                    if len(parts) > 32:
                        sym = var.replace('v_', '')
                        price = float(parts[3]) if parts[3] else 0
                        pct = float(parts[32]) if parts[32] else 0
                        for r in results:
                            if r['market'] + r['code'] == sym:
                                r['price'] = price
                                r['pct'] = pct
                                break
            except Exception as e:
                traceback.print_exc()
        return Response(json_dumps({'code': 0, 'results': results}), mimetype='application/json')
    except Exception as e:
        traceback.print_exc()
        return Response(json_dumps({'code': -1, 'msg': str(e)}), mimetype='application/json', status=500)

@app.route('/api/watchlist', methods=['GET', 'POST'])
def watchlist():
    """获取/保存自选股列表"""
    if request.method == 'POST':
        body = request.json or {}
        # 用 safe_json_save 原子替换：并发写时不会留下半份 JSON，也不会丢写
        safe_json_save(WATCHLIST_FILE, body)
        sync_file('watchlist.json')
        return Response(json_dumps({'code': 0}), mimetype='application/json')
    else:
        if os.path.exists(WATCHLIST_FILE):
            try:
                with open(WATCHLIST_FILE, 'r', encoding='utf-8') as f:
                    return Response(json_dumps({'code': 0, 'data': json.load(f)}), mimetype='application/json')
            except:
                pass
        return Response(json_dumps({'code': 0, 'data': {'stocks': []}}), mimetype='application/json')

@app.route('/api/watchlist_quotes')
def watchlist_quotes():
    """批量获取自选股实时行情"""
    codes = request.args.get('codes', '')
    if not codes:
        return Response(json_dumps({'code': 0, 'data': []}), mimetype='application/json')
    try:
        import requests
        qurl = f'https://qt.gtimg.cn/q={codes}'
        resp = requests.get(qurl, timeout=5)
        resp.encoding = 'gbk'
        data = []
        for line in resp.text.strip().split(';'):
            if '=' not in line: continue
            var = line.split('=')[0].strip()  # v_sh600519
            payload = line.split('="')[1].rstrip('"') if '="' in line else ''
            parts = payload.split('~')
            if len(parts) > 34:
                sym = var.replace('v_', '')  # sh600519
                code = sym[2:]  # 600519
                price = float(parts[3]) if parts[3] else 0
                pct = float(parts[32]) if parts[32] else 0
                open = float(parts[5]) if parts[5] else 0
                high = float(parts[33]) if parts[33] else 0
                low = float(parts[34]) if parts[34] else 0
                data.append({'code': code, 'price': price, 'pct': pct, 'open': open, 'high': high, 'low': low})
        return Response(json_dumps({'code': 0, 'data': data}), mimetype='application/json')
    except Exception as e:
        traceback.print_exc()
        return Response(json_dumps({'code': -1, 'msg': str(e)}), mimetype='application/json', status=500)

_TX_STOCK_CACHE = {}


def _sina_stock_kline(sym, period='day', datalen=1800):
    """新浪个股日/周K（不复权）兜底源，返回与腾讯一致的行格式。"""
    scale = 1200 if period == 'week' else 240
    url = ('https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData'
           '?symbol=%s&scale=%d&datalen=%d' % (sym, scale, datalen))
    txt = http_get(url, referer='https://finance.sina.com.cn')
    arr = json.loads(txt)
    rows = []
    for it in (arr or []):
        try:
            rows.append([str(it['day'])[:10], float(it['open']), float(it['close']),
                         float(it['high']), float(it['low']),
                         float(it.get('volume') or 0)])
        except Exception:
            continue
    return rows


def _tx_stock_kline(sym, period='day', years=6):
    """腾讯前复权个股日K/周K，返回 [[date, open, close, high, low, volume], ...] 升序。

    腾讯单次最多返回约 640 根，日K按年份分段请求再按日期合并去重。
    分段请求之间前复权基准一致（都相对最新价复权），实测重叠交易日收盘价完全相同，
    因此拼接不会出现跳空断层。周K一次 800 根足够覆盖 15 年，无需分段。
    腾讯有 WAF 限流（会返回 501 验证页），此时自动降级新浪日K。
    """
    key = (sym, period)
    hit = _TX_STOCK_CACHE.get(key)
    if hit and time.time() - hit['ts'] < 300:
        return hit['rows']
    import requests
    hdr = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://gu.qq.com/'}
    tpl = ('https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=%s,%s,%s,%s,%d,qfq'
           % (sym, period, '%s', '%s', 640))
    merged = {}

    def _absorb(url):
        try:
            r = requests.get(url, timeout=12, headers=hdr)
            d = (r.json().get('data') or {}).get(sym) or {}
        except Exception as e:
            print('腾讯个股K线失败 %s: %s' % (sym, str(e)[:80]))
            return 0
        arr = None
        for k in ('qfq' + period, period):
            if d.get(k):
                arr = d[k]
                break
        if not arr:
            return 0
        n = 0
        for it in arr:
            if len(it) < 5:
                continue
            try:
                merged[str(it[0])[:10]] = [
                    float(it[1]), float(it[2]), float(it[3]), float(it[4]),
                    float(it[5]) if len(it) > 5 and it[5] not in ('', None) else 0.0]
                n += 1
            except Exception:
                continue
        return n

    now = datetime.now()
    if period == 'week':
        _absorb(tpl % ('', ''))
    else:
        # 按自然年分段；首年从1月1日开始，末年为今天
        for y in range(now.year - years, now.year + 1):
            s = '%d-01-01' % y
            e = '%d-12-31' % y
            if y == now.year:
                e = now.strftime('%Y-%m-%d')
            _absorb(tpl % (s, e))
    rows = [[d] + merged[d] for d in sorted(merged)]
    # 腾讯被 WAF 拦（501 验证页）时 merged 为空，降级新浪，避免个股K线显示"暂无数据"
    if not rows:
        try:
            rows = _sina_stock_kline(sym, period)
            if rows:
                print('[stock_kline] 腾讯不可用，降级新浪 %s %s %d条' % (sym, period, len(rows)), flush=True)
        except Exception as e:
            print('[stock_kline] 新浪兜底失败 %s: %s' % (sym, str(e)[:80]))
    if not rows:
        try:
            df = ak.stock_zh_a_daily(symbol=sym, adjust='qfq')
            if period == 'week':
                df['date'] = pd.to_datetime(df['date'])
                df = df.set_index('date').resample('W-FRI').agg(
                    {'open': 'first', 'close': 'last', 'high': 'max', 'low': 'min', 'volume': 'sum'}
                ).dropna().reset_index()
            for _, r in df.iterrows():
                rows.append([str(r['date'])[:10], float(r['open']), float(r['close']),
                             float(r['high']), float(r['low']), float(r.get('volume') or 0)])
            if rows:
                print('[stock_kline] 降级 akshare(qfq) %s %s %d条' % (sym, period, len(rows)), flush=True)
        except Exception as e:
            print('[stock_kline] akshare 兜底失败 %s: %s' % (sym, str(e)[:80]))
    if rows:
        _TX_STOCK_CACHE[key] = {'ts': time.time(), 'rows': rows}
    return rows


@app.route('/api/stock_kline/<code>')
def stock_kline(code):
    """个股K线数据 - 用腾讯接口"""
    period = request.args.get('period', 'day')
    try:
        import requests
        # 确定市场前缀
        if code.startswith('sh') or code.startswith('sz'):
            sym = code
        else:
            sym = ('sh' if code.startswith('6') else 'sz') + code
        if period in ('1', '5', '15', '30', '60', '120'):
            # 分钟K线
            p_map = {'1':'1','5':'5','15':'15','30':'30','60':'60','120':'60'}
            p = p_map.get(period, '5')
            url = f'https://ifzq.gtimg.cn/appstock/app/kline/mkline?param={sym},m{p},,300'
            r = requests.get(url, timeout=8)
            j = r.json()
            data = j['data'][sym][f'm{p}']
            dates, ohlc, closes, vols = [], [], [], []
            for item in data:
                dates.append(item[0])
                o = float(item[1]); c = float(item[2]); h = float(item[3]); l = float(item[4])
                v = float(item[5]) if len(item)>5 else 0
                ohlc.append([o, c, h, l])
                closes.append(c)
                vols.append(v)
            if period == '120':
                # 60分钟合成120分钟
                dates2, ohlc2, closes2, vols2 = [], [], [], []
                i = 0
                while i < len(ohlc):
                    if i+1 < len(ohlc):
                        o = ohlc[i][0]; c = ohlc[i+1][1]; h = max(ohlc[i][2],ohlc[i+1][2]); l = min(ohlc[i][3],ohlc[i+1][3])
                        dates2.append(dates[i])
                        ohlc2.append([o,c,h,l])
                        closes2.append(c)
                        vols2.append(vols[i]+vols[i+1])
                        i += 2
                    else:
                        dates2.append(dates[i]); ohlc2.append(ohlc[i]); closes2.append(closes[i]); vols2.append(vols[i])
                        i += 1
                dates, ohlc, closes, vols = dates2, ohlc2, closes2, vols2
            if period == '1':
                # 分时只取当天
                today = datetime.now().strftime('%Y-%m-%d')
                filtered = [(d,o,c,v) for d,o,c,v in zip(dates,ohlc,closes,vols) if d.startswith(today)]
                if filtered:
                    dates = [x[0] for x in filtered]
                    ohlc = [x[1] for x in filtered]
                    closes = [x[2] for x in filtered]
                    vols = [x[3] for x in filtered]
            return Response(json_dumps({'code': 0, 'dates': dates, 'ohlc': ohlc, 'closes': closes, 'volumes': vols}), mimetype='application/json')
        else:
            # 日K/周K：腾讯前复权（东财 push2 对本机 IP 限流，akshare 主源不可用）
            rows = _tx_stock_kline(sym, 'week' if period == 'week' else 'day')
            if not rows:
                raise ValueError('腾讯个股K线为空（%s %s）' % (sym, period))
            dates = [r[0] for r in rows]
            ohlc = [[r[1], r[2], r[3], r[4]] for r in rows]   # open, close, high, low
            closes = [r[2] for r in rows]
            vols = [r[5] for r in rows]
            return Response(json_dumps({'code': 0, 'dates': dates, 'ohlc': ohlc,
                                        'closes': closes, 'volumes': vols,
                                        'source': '腾讯前复权'}), mimetype='application/json')
    except Exception as e:
        traceback.print_exc()
        return Response(json_dumps({'code': -1, 'msg': str(e)}), mimetype='application/json', status=500)

@app.route('/api/erp')
def erp():
    """计算沪深300股债性价比ERP = 1/PE - 十年期国债收益率"""
    import akshare as ak
    try:
        # 沪深300最新PE
        df = ak.stock_zh_index_value_csindex(symbol="000300")
        pe = float(df['市盈率1'].iloc[0])
        pe_date = str(df['日期'].iloc[0])
        # 十年期国债收益率
        df2 = ak.bond_zh_us_rate()
        y10 = float(df2['中国国债收益率10年'].dropna().iloc[-1])
        bond_date = str(df2['日期'].iloc[-1])
        # 计算
        earnings_yield = 100.0 / pe
        erp_val = earnings_yield - y10
        return jsonify({
            'code': 0, 'erp': round(erp_val, 2),
            'pe': pe, 'pe_date': pe_date,
            'bond_yield': y10, 'bond_date': bond_date,
            'earnings_yield': round(earnings_yield, 2)
        })
    except Exception as e:
        # 不再兜底一个假的 erp=5.55：那个数恰好等于页面默认值 dashboard.html 的 cur:5.55，
        # 上游挂掉时看起来完全正常，故障被永久掩盖。宁可让页面显示"数据不可用"。
        return jsonify({'code': 1, 'msg': str(e), 'erp': None})

@app.route('/api/sentiment')
def sentiment():
    """情绪资金指标：成交额、融资余额、ETF净流入"""
    import urllib.request, json as _json
    cache_file = os.path.join(BASE_DIR, 'sent_cache.json')

    stale_rejected = False
    if os.path.exists(cache_file):
        age = time.time() - os.path.getmtime(cache_file)
        if age < 3600:
            try:
                with open(cache_file, 'r') as f:
                    cached = _json.load(f)
                # 旧口径缓存（沪市成交量降级 / 无两市拆分 / 无 v3 版本号）直接作废，
                # 避免把错数再显示一小时；同时置 stale_rejected，让本轮结果必定写盘覆盖，
                # 否则「业务日期没变化」会让错误缓存一直留着、每次请求都重算（实测 20s）。
                turn = cached.get('turnover') or {}
                if cached.get('v') == 3 and turn.get('sh_yi') is not None \
                        and '交易所' in str(turn.get('pct_source') or ''):
                    return jsonify(cached)
                stale_rejected = True
            except:
                stale_rejected = True

    result = {'code': 0, 'v': 3, 'turnover': {}, 'margin': {}}

    # === 1. 两市成交额（腾讯实时） ===
    try:
        # sh000001=上证综指（沪市全部A股）；sz399106=深证综指（深市全部A股）。
        # 深证成指(399001)只覆盖成分股，口径上不如深证综指干净，故优先 399106。
        url = "https://qt.gtimg.cn/q=sh000001,sz399106,sz399001"
        req = urllib.request.Request(url, headers={'Referer': 'https://gu.qq.com/'})
        raw = urllib.request.urlopen(req, timeout=8).read().decode('gbk')
        sh_amt = sz_amt = sz_cz = 0
        trade_date = None
        for line in raw.strip().split(';'):
            line = line.strip()
            if not line or '="' not in line: continue
            parts = line.split('="')[1].rstrip('"').split('~')
            # parts[30]为接口返回的实际交易日期，格式YYYYMMDD
            if len(parts)>30 and parts[30]:
                raw_d = parts[30]
                trade_date = f'{raw_d[:4]}-{raw_d[4:6]}-{raw_d[6:8]}'
            if line.startswith('v_sh000001'):
                sh_amt = float(parts[37])  # 万元
            elif line.startswith('v_sz399106'):
                sz_amt = float(parts[37])
            elif line.startswith('v_sz399001'):
                sz_cz = float(parts[37])
        if sz_amt <= 0:
            sz_amt = sz_cz      # 深证综指取不到时退回深证成指（两者成交额实际一致）
        total_yi = (sh_amt + sz_amt) / 10000.0  # 万元→亿
        result['turnover']['value_yi'] = round(total_yi, 0)
        result['turnover']['value_wanyi'] = round(total_yi / 10000.0, 2)
        result['turnover']['sh_yi'] = round(sh_amt / 10000.0, 0)
        result['turnover']['sz_yi'] = round(sz_amt / 10000.0, 0)
        result['turnover']['source'] = '腾讯行情实时汇总（沪市上证综指 + 深市深证综指）'
        # 使用接口实际交易日期，不强制用当前日期
        result['turnover']['updated'] = (trade_date or datetime.now().strftime('%Y-%m-%d')) + ' 15:00'

        # 历史百分位：沪市+深市交易所官方日终股票成交额，近三年窗口，排除当日
        try:
            amt_map = _turnover_amount_map()
            today_key = trade_date or datetime.now().strftime('%Y-%m-%d')
            cutoff_3y = (datetime.now() - timedelta(days=3 * 365)).strftime('%Y-%m-%d')
            hist_amt = [v for d, v in sorted(amt_map.items())
                        if cutoff_3y <= d < today_key][-750:]
            if len(hist_amt) < 30:
                raise RuntimeError('成交额历史样本不足（%d 个交易日，回填中）' % len(hist_amt))
            # 盘中：当前累计成交额不能直接与全天历史值比较（会系统性低估分位），
            # 按已走交易时间折算成「全天估算值」再算分位；收盘后用真实全天值。
            # A 股成交额呈 U 型（开盘/尾盘放量），线性折算早盘略高估、尾盘略低估，
            # 但远好于「拿一小时的量去比全天」。
            _today_s = datetime.now().strftime('%Y-%m-%d')
            _prog = 1.0 if (trade_date and trade_date != _today_s) else _session_progress()
            _pct_value = total_yi
            _pct_basis = '全天实际'
            if 0.0 < _prog < 1.0:
                _pct_value = total_yi / max(_prog, 0.10)  # 下限 10%：开盘 10 分钟内不做极端放大
                _pct_basis = '盘中按时间进度折算'
                result['turnover']['est_full_yi'] = round(_pct_value, 0)
            result['turnover']['pct_basis'] = _pct_basis
            result['turnover']['percentile'] = _percentile_below(hist_amt, _pct_value)
            result['turnover']['hist_days'] = len(hist_amt)
            result['turnover']['hist_median_yi'] = round(float(pd.Series(hist_amt).median()), 0)
            _src = '沪深交易所官方日终：沪市+深市股票成交额（亿元，三年窗口内最高/最低位置）'
            if 0.0 < _prog < 1.0:
                _src += '；盘中按已走交易时间折算为全天口径（进度 %d%%）' % int(round(_prog * 100))
            result['turnover']['pct_source'] = _src
        except Exception as e:
            # 只有两市成交额历史完全不可用时才降级，且明确标注口径（量级不同，仅供参照）
            try:
                hist2 = ak.stock_zh_index_daily(symbol='sh000001').tail(1200)
                hist_vol = hist2['volume'].tolist()
                today_sh_vol = 0.0
                for line in raw.strip().split(';'):
                    line = line.strip()
                    if line.startswith('v_sh000001'):
                        parts2 = line.split('="')[1].rstrip('"').split('~')
                        today_sh_vol = float(parts2[36]) * 100
                below = sum(1 for v in hist_vol[:-1] if v < today_sh_vol)
                result['turnover']['percentile'] = round(below / max(len(hist_vol) - 1, 1) * 100, 0)
                result['turnover']['hist_days'] = len(hist_vol) - 1
                result['turnover']['pct_source'] = '降级（沪市单市成交量口径，仅供参照）：%s' % e
            except Exception as e2:
                result['turnover']['percentile'] = None
                result['turnover']['pct_error'] = '%s; fallback:%s' % (e, e2)
    except Exception as e:
        result['turnover']['error'] = str(e)

    # === 2. 融资余额净流入 ===
    try:
        sse = ak.stock_margin_sse(start_date="20231101", end_date=datetime.now().strftime("%Y%m%d"))
        sse = sse.sort_values('信用交易日期')
        sse_dates = [str(x) for x in sse['信用交易日期']]
        # 上交所返回的是紧凑格式 20261113，而深市缓存/金十序列的键是 2026-11-13。
        # 不统一会导致 sz_map 一个都对不上 → 合计口径静默退化为沪市口径（审计发现的 62 分位偏差）。
        sse_dates = [d if '-' in d else '%s-%s-%s' % (d[:4], d[4:6], d[6:8]) for d in sse_dates]
        sse_bal = {d: float(b) / 1e8 for d, b in zip(sse_dates, sse['融资余额'])}
        sse_bal_latest = sse_bal[sse_dates[-1]]
        sse_bal_prev = sse_bal[sse_dates[-2]]
        latest_d = sse_dates[-1]
        prev_d = sse_dates[-2]

        def _szse_balance(day):
            """深市融资余额（亿元）。深交所只提供单日快照，取不到返回 None。"""
            try:
                df_sz = ak.stock_margin_szse(date=day)
                if len(df_sz) > 0:
                    return float(df_sz['融资余额'].iloc[0])
            except Exception:
                return None
            return None

        # 深市必须取「同一对交易日」的两个快照才能相减。
        # 旧代码用当天的深市余额既当今日又当昨日，delta 里深市部分恒为 0
        # （实测 11692.64 沪市昨日 + 10805.21 深市今日），净流入实际只剩沪市。
        sz_map = _margin_sz_map(sse_dates)
        # 注意：dict.get 的默认参数会被「先求值」，写成 sz_map.get(d, _szse_balance(d))
        # 会让每次 sentiment 请求都同步打两次深交所（源站不通时每次 10~20s）。
        # 这里必须先查缓存，只有真的缺失才发请求。
        szse_latest = sz_map.get(latest_d)
        if szse_latest is None:
            szse_latest = _szse_balance(latest_d)
        szse_prev = sz_map.get(prev_d)
        if szse_prev is None:
            szse_prev = _szse_balance(prev_d)
        if szse_latest is not None and szse_prev is not None:
            total_latest = sse_bal_latest + szse_latest
            total_prev = sse_bal_prev + szse_prev
            result['margin']['scope'] = '沪深两市'
        else:
            total_latest = sse_bal_latest
            total_prev = sse_bal_prev
            result['margin']['scope'] = '仅沪市（深市数据不可用）'
        net = total_latest - total_prev
        result['margin']['net_inflow_yi'] = round(net, 1)
        result['margin']['total_balance_yi'] = round(total_latest, 0)
        result['margin']['sh_balance_yi'] = round(sse_bal_latest, 0)
        if szse_latest is not None:
            result['margin']['sz_balance_yi'] = round(szse_latest, 0)
        result['margin']['sse_date'] = latest_d
        result['margin']['source'] = '上交所官方 + 深市（东方财富日终）'

        # 历史百分位：必须用「沪深合计」同一口径与同一窗口。
        # 审计教训：旧代码拿沪市单一序列（3y 最大值 15211 亿）去给合计值 22498 亿算分位，
        # 结果永远显示在顶部附近（62 分位），与合计口径真实分位（≈49）差 13~14pp。
        cutoff_3y = (datetime.now() - timedelta(days=3 * 365)).strftime('%Y-%m-%d')
        pairs = [(d, sse_bal[d] + sz_map[d]) for d in sse_dates
                 if sz_map.get(d) and d < latest_d]
        pairs = [(d, v) for d, v in pairs if d >= cutoff_3y][-750:]
        if len(pairs) >= 30:
            hist_vals = [v for _d, v in pairs]
            cur_total = sse_bal_latest + (szse_latest if szse_latest is not None else 0.0)
            result['margin']['percentile'] = _percentile_below(hist_vals, cur_total)
            result['margin']['hist_days'] = len(hist_vals)
            result['margin']['pct_source'] = '沪深两市融资余额合计（三年窗口内最高/最低位置，排除当日）'
            # 沪市单口径分位保留，方便对照（回测旧结果解释用）
            sh_win = [sse_bal[d] for d in sse_dates
                      if cutoff_3y <= d < latest_d][-750:]
            if len(sh_win) >= 30:
                result['margin']['percentile_sh'] = _percentile_below(sh_win, sse_bal_latest)
        else:
            # 深市历史还没补齐：退回沪市口径，但显式标注，不再冒充合计口径
            sse_series = [sse_bal[d] for d in sse_dates if d < latest_d][-750:]
            result['margin']['percentile'] = _percentile_below(sse_series, sse_bal_latest)
            result['margin']['hist_days'] = len(sse_series)
            result['margin']['pct_source'] = '降级（仅沪市融资余额口径，深市历史补齐中）'
        # 数据滞后提示：官方 T-1 是常态，滞后 2 个交易日以上说明源端没更新。
        # 「滞后」按「已经走完的交易日」算：盘中不把当天算进去，否则每天 9:30 就开始显示 lag=1。
        try:
            cal = _trade_date_set()
            today_s = datetime.now().strftime('%Y-%m-%d')
            if cal:
                done = [d for d in cal if d < today_s]
                if is_after_close() and today_s in cal:
                    done.append(today_s)
                last_done = max(done) if done else latest_d
                lag = sum(1 for d in cal if latest_d < d <= last_done)
            else:
                # 日历不可用时用「工作日」近似，且盘中不把当天算成已走完
                d0 = datetime.strptime(latest_d, '%Y-%m-%d').date()
                d1 = datetime.now().date()
                if not is_after_close():
                    d1 = d1 - timedelta(days=1)
                lag = sum(1 for i in range((d1 - d0).days)
                          if (d0 + timedelta(days=1 + i)).weekday() < 5)
            result['margin']['lag_days'] = lag
        except Exception:
            result['margin']['lag_days'] = None
        result['margin']['latest_date'] = latest_d
    except Exception as e:
        result['margin']['error'] = str(e)


    # 说明：这里原本还有一段「宽基ETF主力净流入」抓取（东财 clist f62），
    # 但它曾被误粘贴到 market_breadth() 的 return 之后永不可达，且页面从来没读过
    # sentiment.etf —— 看板显示的 ETF 数据走 /api/etf_share（510300 份额 + 历史）。
    # 该东财接口对本机长期 RemoteDisconnected，留着只会给每次请求加几秒延迟，
    # 所以直接删除，不再保留 etf 字段。

    # 盘中仅返回数据，不自动落盘；仅收盘后校验日期，新日期才允许写入
    if is_after_close():
        try:
            old_cache = None
            if os.path.exists(cache_file):
                with open(cache_file,'r',encoding='utf-8') as f:
                    old_cache = json.load(f)
            # 提取新数据业务日期
            new_date = result.get('turnover',{}).get('updated','')[:10]
            old_date = old_cache.get('turnover',{}).get('updated','')[:10] if old_cache else ''
            # 只有新日期晚于旧日期才保存，旧数据永远不能覆盖新数据
            if not old_date or (new_date and new_date > old_date) or stale_rejected:
                safe_json_save(cache_file, result)
            else:
                print(f'情绪缓存日期{new_date}未更新，保留旧缓存{old_date}')
        except Exception as e:
            print('情绪缓存保存失败:',e)
    return jsonify(result)

# ==================== 沪深两市融资余额历史（交易所官方日终 + 磁盘缓存） ====================
# 旧实现用 ak.macro_china_market_margin_sh/sz()（akshare 内部取金十 fs_1.json），
# 该源实测滞后约 6 个交易日（2026-11-13 时最新只到 2026-11-05），
# 图表永远停在上一周，看起来像「不会自动更新」。
# 现在改为：沪市走交易所官方 stock_margin_sse（单次请求即可拿到近3年全部日期，最新到 T-1），
# 深市长历史仍用金十（实测与交易所口径一致：2026-11-05 均为 10901.8 亿），
# 最新缺口用深交所官方单日快照 stock_margin_szse 补齐并落盘缓存，之后每天只补新增的 1~2 天。
_MARGIN_CACHE_FILE = os.path.join(BASE_DIR, 'margin_hist.json')
_margin_hist_cache = {'ts': 0.0, 'data': None}
_MARGIN_TTL = 300          # 结果内存缓存 5 分钟


def _margin_sz_rows_load():
    """读磁盘：{'2026-11-13': 10805.21, ...}（深市融资余额，亿元）"""
    try:
        with open(_MARGIN_CACHE_FILE, 'r', encoding='utf-8') as f:
            d = json.load(f)
        if d.get('v') == 1 and isinstance(d.get('sz'), dict):
            return dict(d['sz'])
    except Exception:
        pass
    return {}


def _margin_sz_rows_save(rows):
    try:
        safe_json_save(_MARGIN_CACHE_FILE, {
            'v': 1, 'ts': time.time(),
            'updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'sz': rows})
    except Exception:
        pass


def _sz_margin_one(day):
    """深交所官方单日融资余额（亿元）。非交易日/未发布时返回 None。"""
    global _sz_fail_ts
    d8 = str(day).replace('-', '')
    try:
        df = ak.stock_margin_szse(date=d8)
        cols = [c for c in df.columns if '融资余额' in str(c)]
        if len(df) and cols:
            return round(float(df[cols[0]].iloc[0]), 2)
    except Exception:
        # szse.cn 偶发整站连接超时（单次约 10~20s）。失败的瞬间记一笔，
        # 后续 15 分钟内不再同步重试，避免一次 sentiment 请求被拖到 20s。
        _sz_fail_ts = time.time()
        return None
    return None


# szse.cn 最近一次失败时间：非 0 表示下游不可达，15 分钟内跳过同步补深市
_sz_fail_ts = 0.0
_SZ_RETRY_GAP = 900


# 深市融资余额：东财「按市场日频」接口（替代已不可用的深交所官网单日快照）
# 背景：www.szse.cn 在部分网络下整站连接超时，ak.stock_margin_szse 单日请求不可用，
# 导致深市缺最新数据 → 融资余额图停在几天前、sentiment 只剩沪市口径（总额显示 1.16 万亿）。
# 东财 datacenter 的 RPTA_WEB_RZRQ_LSSH 提供分市场完整日频历史，实测与交易所口径一致
# （2026-11-16 沪 11598.13 / 深 10685.63 亿元，沪市与上交所官方完全相同）。
# SCDM：007=沪市、001=深市、002=北交所。
_em_margin_cache = {'ts': 0.0, 'sz': None}
_EM_MARGIN_TTL = 600


def _em_margin_market(scdm, pages=2):
    """东财按市场融资余额日频序列 {YYYY-MM-DD: 亿元}，取最近 pages*500 条。"""
    import urllib.parse as _up
    rows = {}
    for pn in range(1, max(1, pages) + 1):
        url = ('https://datacenter-web.eastmoney.com/api/data/v1/get'
               '?reportName=RPTA_WEB_RZRQ_LSSH&columns=DIM_DATE,RZYE'
               '&sortColumns=DIM_DATE&sortTypes=-1&pageNumber=%d&pageSize=500'
               '&source=WEB&client=WEB&filter=%s'
               % (pn, _up.quote('(SCDM=%s)' % scdm)))
        try:
            j = http_get_json(url, referer='https://data.eastmoney.com/', timeout=15)
        except Exception:
            traceback.print_exc()
            break
        data = ((j or {}).get('result') or {}).get('data') or []
        if not data:
            break
        for r in data:
            try:
                d = str(r.get('DIM_DATE') or '')[:10]
                v = float(r.get('RZYE') or 0)
            except (TypeError, ValueError):
                continue
            if d and v > 0:
                rows[d] = round(v / 1e8, 2)
        if len(data) < 500:
            break
    return rows


def _em_margin_series():
    """深市融资余额（东财，约 1000 个交易日），内存缓存 10 分钟。"""
    now = time.time()
    hit = _em_margin_cache.get('sz')
    if hit is not None and now - _em_margin_cache['ts'] < _EM_MARGIN_TTL:
        return hit
    rows = {}
    try:
        rows = _em_margin_market('001', pages=2)
    except Exception:
        traceback.print_exc()
    if rows:
        _em_margin_cache['sz'] = rows
        _em_margin_cache['ts'] = now
        print('[融资余额] 深市（东财日频）%d 条（%s ~ %s）'
              % (len(rows), min(rows), max(rows)))
    return rows or {}


_j10_margin_cache = {'ts': 0.0, 'rows': None}


def _jin10_margin_sz():
    """金十深市融资余额长历史（2010 年起，滞后约 5~10 个交易日）。

    东财日频接口只给最近 ~1000 个交易日（2022-09 起），回测 5y/10y 的更早区间
    需要长历史。金十序列与交易所口径一致（已核对 2026-11-05 深市 10901.8 亿），
    只是更新慢，因此只用于「东财没有、且更早」的日期。
    """
    now = time.time()
    if _j10_margin_cache['rows'] is not None and now - _j10_margin_cache['ts'] < 86400:
        return _j10_margin_cache['rows']
    rows = {}
    try:
        df = ak.macro_china_market_margin_sz()
        ds = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
        for d, v in zip(ds, df['融资余额'].astype(float)):
            if pd.notna(v) and float(v) > 0:
                rows[d] = round(float(v) / 1e8, 2)
    except Exception:
        traceback.print_exc()
    if rows:
        _j10_margin_cache.update(ts=now, rows=rows)
        print('[融资余额] 金十深市长历史 %d 条（%s ~ %s）'
              % (len(rows), min(rows), max(rows)))
    return rows


def _margin_sz_map(need_dates):
    """深市融资余额历史 {日期: 亿元}。

    need_dates 为沪市官方数据覆盖的交易日（升序）。数据源优先级：
      1) 东财按市场日频（2022-09 起，含 T-1 最新，不依赖 szse.cn）；
      2) 金十长历史（2010 起，滞后但口径一致）——只补东财覆盖不到的更早日期，
         这样回测 5y/10y 的「沪深合计」在 2022-09 前也是真两市的合计值；
      3) 磁盘缓存 margin_hist.json。
    """
    rows = _margin_sz_rows_load()
    # 金十深市序列里偶发 0/负值（如 2024-08-08 的 0），会让「沪深合计」当天凭空掉一截，
    # 这里统一当作缺失，交给东财序列覆盖。
    rows = {d: v for d, v in rows.items() if isinstance(v, (int, float)) and v > 0}
    changed = False
    em = _em_margin_series()
    if em:
        changed = any(rows.get(d) != v for d, v in em.items())
        rows.update(em)
    need = [d for d in (need_dates or []) if d not in rows]
    if need:
        j10 = _jin10_margin_sz()
        add = {d: j10[d] for d in need if d in j10}
        if add:
            rows.update(add)
            changed = True
            print('[融资余额] 金十长历史补 %d 个深市缺口（%s ~ %s）'
                  % (len(add), min(add), max(add)))
    if changed and rows:
        _margin_sz_rows_save(rows)
    return rows


_margin_bf = {'running': False, 'last': 0.0}
_MARGIN_FILL_MAX = 400


def _margin_fill_async(need_dates):
    """后台补齐深市历史空洞：每天一次深交所单日请求（约 0.5s），6 线程约 20s 跑完。"""
    now = time.time()
    if _sz_fail_ts and now - _sz_fail_ts < _SZ_RETRY_GAP:
        return
    if _margin_bf['running'] or now - _margin_bf['last'] < 1800:
        return
    _margin_bf['running'] = True
    _margin_bf['last'] = now

    def _worker():
        try:
            rows = _margin_sz_rows_load()
            miss = [d for d in need_dates if d not in rows][:_MARGIN_FILL_MAX]
            if not miss:
                return
            from concurrent.futures import ThreadPoolExecutor
            new = 0
            with ThreadPoolExecutor(max_workers=6) as ex:
                for d, v in zip(miss, ex.map(_sz_margin_one, miss)):
                    if v is not None:
                        rows[d] = v
                        new += 1
            if new:
                _margin_sz_rows_save(rows)
                _margin_hist_cache['ts'] = 0.0
                _margin_hist_cache['data'] = None
                print('[融资余额] 深市历史补齐 %d 条，共 %d 条（%s ~ %s）'
                      % (new, len(rows), min(rows), max(rows)))
        except Exception:
            traceback.print_exc()
        finally:
            _margin_bf['running'] = False
            _margin_bf['last'] = time.time()

    threading.Thread(target=_worker, daemon=True).start()


@app.route('/api/margin_history')
def margin_history():
    """沪深两市融资余额历史（近3年）+ 沪深300指数（交易所官方口径，T-1 自动更新）"""
    force = request.args.get('force') == '1'
    try:
        now_ts = time.time()
        if not force and _margin_hist_cache['data'] and now_ts - _margin_hist_cache['ts'] < _MARGIN_TTL:
            return jsonify(_margin_hist_cache['data'])

        today = datetime.now()
        start8 = (today - timedelta(days=365 * 3 + 40)).strftime('%Y%m%d')
        sse = ak.stock_margin_sse(start_date=start8, end_date=today.strftime('%Y%m%d'))
        if sse is None or len(sse) == 0:
            raise RuntimeError('上交所融资余额为空')
        sse = sse.copy()
        dcol = sse.columns[0]
        bcol = [c for c in sse.columns if '融资余额' in str(c)][0]
        sse['_d'] = pd.to_datetime(sse[dcol].astype(str)).dt.strftime('%Y-%m-%d')
        sh = {d: round(float(v) / 1e8, 2)
              for d, v in zip(sse['_d'], sse[bcol].astype(float)) if pd.notna(v)}

        sz = _margin_sz_map(sorted(sh))

        spot = ak.stock_zh_index_daily(symbol='sh000300').copy()
        spot['_d'] = pd.to_datetime(spot['date']).dt.strftime('%Y-%m-%d')
        csi = {d: float(v) for d, v in zip(spot['_d'], spot['close'].astype(float)) if pd.notna(v)}

        cutoff = (today - timedelta(days=365 * 3)).strftime('%Y-%m-%d')
        dates = [d for d in sorted(sh) if d >= cutoff and sz.get(d) and d in csi]
        if not dates:
            raise RuntimeError('融资余额与指数无交集日期')
        data = {
            'code': 0,
            'dates': dates,
            'sh_balance': [int(round(sh[d])) for d in dates],
            'sz_balance': [int(round(sz[d])) for d in dates],
            'csi300': [round(csi[d], 1) for d in dates],
            'total_balance': [int(round(sh[d] + sz[d])) for d in dates],
            'start': dates[0], 'end': dates[-1],
            'source': '上交所官方日终 + 深市（东方财富按市场日终）',
            'updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        _margin_hist_cache['data'] = data
        _margin_hist_cache['ts'] = time.time()
        return jsonify(data)
    except Exception as e:
        traceback.print_exc()
        return jsonify({'code': 1, 'msg': str(e)})

@app.route('/api/market_breadth')
def market_breadth():
    """涨跌家数（乐咕乐股）"""
    try:
        df = ak.stock_market_activity_legu()
        d = dict(zip(df['item'], df['value']))
        return jsonify({
            'code': 0,
            'adv': int(float(d.get('上涨', 0))),
            'dec': int(float(d.get('下跌', 0))),
            'limit_up': int(float(d.get('涨停', 0))),
            'limit_down': int(float(d.get('跌停', 0))),
            'flat': int(float(d.get('平盘', 0))),
            'date': str(d.get('统计日期', '')),
        })
    except Exception as e:
        return jsonify({'code': 1, 'msg': str(e)})

@app.route('/api/global_quotes')
def global_quotes():
    """全球品种实时价格（新浪代理）"""
    import urllib.request as _ur
    try:
        url = "https://hq.sinajs.cn/list=b_NKY,b_KOSPI,fx_susdcnh"
        req = _ur.Request(url, headers={
            'Referer': 'https://finance.sina.com.cn/',
            'User-Agent': 'Mozilla/5.0'
        })
        data = _ur.urlopen(req, timeout=8).read().decode('gbk')
        result = {}
        for line in data.strip().split('\n'):
            line = line.strip().rstrip(';')
            if not line or '="' not in line: continue
            var_name, val = line.split('="', 1)
            code = var_name.replace('var hq_str_', '')
            val = val.rstrip('"')
            if not val: continue
            p = val.split(',')
            if code == 'b_NKY':
                result['n225'] = {'price': float(p[1]), 'pct': float(p[3])}
            elif code == 'b_KOSPI':
                result['kospi'] = {'price': float(p[1]), 'pct': float(p[3])}
            elif code == 'fx_susdcnh':
                result['usdcny'] = {'price': float(p[1]), 'pct': float(p[11])}
        return jsonify({'code': 0, 'data': result})
    except Exception as e:
        return jsonify({'code': 1, 'msg': str(e)})

@app.route('/api/futures_quotes')
def futures_quotes():
    """中金所股指期货实时行情（新浪）"""
    import urllib.request as _ur
    try:
        url = "https://hq.sinajs.cn/list=nf_IF0,nf_IH0,nf_IM0"
        req = _ur.Request(url, headers={
            'Referer': 'https://finance.sina.com.cn/',
            'User-Agent': 'Mozilla/5.0'
        })
        raw = _ur.urlopen(req, timeout=8).read().decode('gbk')
        result = {}
        mapping = {'nf_IF0': 'if', 'nf_IH0': 'ih', 'nf_IM0': 'im'}
        for line in raw.strip().split('\n'):
            line = line.strip().rstrip(';')
            if not line or '="' not in line: continue
            var, val = line.split('="', 1)
            code = var.replace('var hq_str_', '')
            if code not in mapping: continue
            val = val.rstrip('"')
            p = val.split(',')
            if len(p) < 35: continue
            price = float(p[3])  # 最新价
            # 新浪 nf_* 的字段：p[13] 是昨收盘，p[14] 才是昨结算。
            # 交叉验证 IF0/IH0/IC0 三组：p[13] 恒等于上一交易日 close，
            # 旧代码用 p[13] 却标注"昨结算"，涨跌幅系统性偏小。
            # p[14] 为 0（新合约/停牌）时退回 p[13]。
            prev_settle = float(p[14] or 0) or float(p[13])
            pct = round((price - prev_settle) / prev_settle * 100, 2) if prev_settle > 0 else 0
            result[mapping[code]] = {'price': price, 'pct': pct}
        return jsonify({'code': 0, 'data': result})
    except Exception as e:
        return jsonify({'code': 1, 'msg': str(e)})

@app.route('/api/futures_basis')
def futures_basis():
    """股指期货基差：期货主连收盘价 - 现货指数收盘价，标注交割日"""
    from datetime import date as _date, timedelta as _td
    try:
        def _third_friday_dates(start, end):
            """生成范围内每月第三个周五"""
            result = []
            d = start.replace(day=1)
            while d <= end:
                # 找当月第一个周五
                first = _date(d.year, d.month, 1)
                while first.weekday() != 4:
                    first += _td(days=1)
                third = first + _td(days=14)
                if start <= third <= end:
                    result.append(third.strftime('%Y-%m-%d'))
                # 下月
                if d.month == 12:
                    d = _date(d.year+1, 1, 1)
                else:
                    d = _date(d.year, d.month+1, 1)
            return result

        pairs = [
            ('IF0', 'sh000300', '沪深300'),
            ('IH0', 'sh000016', '上证50'),
            ('IM0', 'sh000852', '中证1000'),
        ]
        result = {}
        for fut_sym, idx_sym, name in pairs:
            fut = ak.futures_zh_daily_sina(symbol=fut_sym)
            fut['date'] = pd.to_datetime(fut['date'])
            fut = fut[['date','close']].rename(columns={'close':'fut_close'})
            spot = ak.stock_zh_index_daily(symbol=idx_sym)
            spot['date'] = pd.to_datetime(spot['date'])
            spot = spot[['date','close']].rename(columns={'close':'spot_close'})
            merged = pd.merge(fut, spot, on='date', how='inner').sort_values('date').tail(250)
            merged['basis'] = (merged['fut_close'] - merged['spot_close']).round(1)
            # 生成交割日（每月第三个周五）
            start_d = merged['date'].iloc[0].date()
            end_d = merged['date'].iloc[-1].date()
            roll_dates = _third_friday_dates(start_d, end_d)
            result[fut_sym] = {
                'name': name,
                'dates': [d.strftime('%Y-%m-%d') for d in merged['date']],
                'basis': merged['basis'].tolist(),
                'rollover_dates': roll_dates,
                'current': float(merged['basis'].iloc[-1]) if len(merged) else None,
            }
        return jsonify({'code': 0, 'data': result})
    except Exception as e:
        return jsonify({'code': 1, 'msg': str(e)})

# ================== 技术分析 ==================
def _ma(closes, n):
    if len(closes) < n: return None
    return round(sum(closes[-n:]) / n, 2)

def _macd(closes):
    if len(closes) < 35: return None, None, None
    ema12 = closes[0]
    ema26 = closes[0]
    dif_list = []
    for c in closes:
        ema12 = c * (2/13) + ema12 * (11/13)
        ema26 = c * (2/27) + ema26 * (25/27)
        dif_list.append(ema12 - ema26)
    dea = dif_list[0]
    for d in dif_list:
        dea = d * (2/10) + dea * (8/10)
    dif = dif_list[-1]
    macd_bar = 2 * (dif - dea)
    return round(dif, 2), round(dea, 2), round(macd_bar, 2)

def _bias(closes, n):
    if len(closes) < n: return None
    ma = sum(closes[-n:]) / n
    return round((closes[-1] - ma) / ma * 100, 2)

def _boll(closes, n=20, k=2):
    if len(closes) < n: return None, None, None
    slice_ = closes[-n:]
    mid = sum(slice_) / n
    std = (sum((x - mid) ** 2 for x in slice_) / n) ** 0.5
    return round(mid + k * std, 2), round(mid, 2), round(mid - k * std, 2)

def _prev_high_low(highs, lows, n):
    if len(highs) < n: return None, None
    return round(max(highs[-n:]), 2), round(min(lows[-n:]), 2)

def _gaps(opens, closes, n=5):
    """检测近n日缺口"""
    gaps = []
    for i in range(-n, 0):
        if abs(i - 1) > len(closes): break
        prev_close = closes[i-1]
        curr_open = opens[i]
        if curr_open > prev_close * 1.005:  # 向上跳空>0.5%
            gaps.append({'dir': 'up', 'date': str(i), 'size': round((curr_open-prev_close)/prev_close*100, 2)})
        elif curr_open < prev_close * 0.995:
            gaps.append({'dir': 'down', 'date': str(i), 'size': round((prev_close-curr_open)/prev_close*100, 2)})
    return gaps

def _fib_levels(high, low):
    """斐波那契回撤位"""
    diff = high - low
    return {
        'r23.6': round(high - diff * 0.236, 2),
        'r38.2': round(high - diff * 0.382, 2),
        'r50': round(high - diff * 0.5, 2),
        'r61.8': round(high - diff * 0.618, 2),
    }

def _vol_ma(volumes, n):
    if len(volumes) < n: return None
    return round(sum(volumes[-n:]) / n, 0)

def _divergence(closes, highs, n=20):
    """量价背离检测"""
    if len(closes) < n: return None
    recent_closes = closes[-n:]
    recent_highs = highs[-n:]
    # 找两个局部高点
    peaks = []
    for i in range(2, len(recent_closes)-2):
        if recent_closes[i] == max(recent_closes[i-2:i+3]):
            peaks.append((i, recent_closes[i]))
    if len(peaks) >= 2:
        p1, p2 = peaks[-2], peaks[-1]
        if p2[1] > p1[1]:  # 价格创新高
            return 'bullish_divergence' if recent_highs[peaks[-1][0]] < recent_highs[peaks[-2][0]] else None
        elif p2[1] < p1[1]:  # 价格创新低
            return 'bearish_divergence' if recent_highs[peaks[-1][0]] > recent_highs[peaks[-2][0]] else None
    return None

def analyze_timeframe(kline_df):
    """对单个周期K线计算所有指标"""
    closes = kline_df['close'].tolist()
    highs = kline_df['high'].tolist()
    lows = kline_df['low'].tolist()
    opens = kline_df['open'].tolist()
    vols = kline_df['volume'].tolist()
    price = closes[-1]

    result = {'price': price}
    # MA
    result['ma'] = {f'ma{n}': _ma(closes, n) for n in [5, 10, 20, 60]}
    # MACD
    dif, dea, bar = _macd(closes)
    result['macd'] = {'dif': dif, 'dea': dea, 'bar': bar,
                      'signal': 'golden' if dif and dea and dif > dea else 'dead'}
    # BIAS
    result['bias'] = {f'bias{n}': _bias(closes, n) for n in [6, 12, 24]}
    # BOLL
    upper, mid, lower = _boll(closes)
    result['boll'] = {'upper': upper, 'mid': mid, 'lower': lower,
                      'position': 'upper' if price > mid else ('lower' if price < mid else 'mid')}
    # 前高前低
    ph20, pl20 = _prev_high_low(highs, lows, 20)
    ph60, pl60 = _prev_high_low(highs, lows, 60)
    result['prev_levels'] = {'high20': ph20, 'low20': pl20, 'high60': ph60, 'low60': pl60}
    # 缺口
    result['gaps'] = _gaps(opens, closes)
    # 斐波那契
    result['fib'] = _fib_levels(ph60 or price, pl60 or price)
    # 成交量MA
    result['vol_ma'] = {'ma5': _vol_ma(vols, 5), 'ma10': _vol_ma(vols, 10),
                       'current': vols[-1],
                       'ratio': round(vols[-1] / _vol_ma(vols, 5), 2) if _vol_ma(vols, 5) else None}
    # 量价背离
    result['divergence'] = _divergence(closes, highs)
    # 综合信号
    signals = []
    if result['ma']['ma20'] and price > result['ma']['ma20']: signals.append('价在MA20上方')
    if result['ma']['ma20'] and price < result['ma']['ma20']: signals.append('价在MA20下方')
    if result['macd']['bar'] and result['macd']['bar'] > 0: signals.append('MACD红柱')
    if result['macd']['bar'] and result['macd']['bar'] < 0: signals.append('MACD绿柱')
    if result['boll']['position'] == 'upper': signals.append('触及BOLL上轨')
    if result['boll']['position'] == 'lower': signals.append('触及BOLL下轨')
    if result['vol_ma']['ratio'] and result['vol_ma']['ratio'] > 1.5: signals.append('明显放量')
    result['summary'] = signals
    return result

# 技术面可分析的指数（顺序即前端下拉顺序）
TECH_INDEXES = [
    ('sh000001', '上证指数'), ('sz399001', '深证成指'), ('sz399006', '创业板指'),
    ('sh000688', '科创50'), ('sh000016', '上证50'), ('sh000300', '沪深300'),
    ('sh000852', '中证1000'),
]
TECH_INDEX_NAME = dict(TECH_INDEXES)
_tech_cache = {}          # code -> (ts, payload)
_TECH_TTL = 300           # 5 分钟；日线指标一天只变一次，5分钟足够


def _tech_daily(code, need=750):
    """指数日线（新浪优先、东财兜底）。
    返回 (dates, opens, closes, highs, lows, vols)，长度一致。"""
    try:
        k = _sina_index_kline(code, 240, datalen=max(need + 20, 800))
        dates = [str(d)[:10] for d in k['dates']]
        if len(dates) >= 60:
            opens = [x[0] for x in k['ohlc']]
            closes = [x[1] for x in k['ohlc']]
            highs = [x[2] for x in k['ohlc']]
            lows = [x[3] for x in k['ohlc']]
            vols = list(k['vol'])
            return dates, opens, closes, highs, lows, vols
    except Exception as e:
        print('[tech] 新浪日线失败 %s: %s' % (code, str(e)[:80]))
    # 兜底：akshare（东财/新浪）
    df = ak.stock_zh_index_daily(symbol=code)
    df = df.tail(need + 20).reset_index(drop=True)
    return ([str(d)[:10] for d in df['date'].tolist()],
            df['open'].astype(float).tolist(), df['close'].astype(float).tolist(),
            df['high'].astype(float).tolist(), df['low'].astype(float).tolist(),
            df['volume'].astype(float).tolist())


@app.route('/api/tech/<code>')
def tech_analysis(code):
    """技术分析：日线，返回K线+指标序列+历史百分位。
    支持 7 个常用指数；结果缓存 5 分钟，避免每次切指数都重算。"""
    if code not in TECH_INDEX_NAME:
        return jsonify({'code': -1, 'msg': 'unknown index',
                        'allowed': [c for c, _ in TECH_INDEXES]})
    hit = _tech_cache.get(code)
    if hit and time.time() - hit[0] < _TECH_TTL:
        return jsonify(hit[1])
    try:
        dates, opens, closes, highs, lows, vols = _tech_daily(code, 750)
        # 近三年窗口用于百分位
        dates, opens, closes, highs, lows, vols = (
            dates[-750:], opens[-750:], closes[-750:], highs[-750:], lows[-750:], vols[-750:])
        n = len(closes)

        # --- 计算MA序列 ---
        def ma_series(arr, w):
            out = []
            for i in range(len(arr)):
                if i < w-1: out.append(None)
                else: out.append(round(sum(arr[i-w+1:i+1])/w, 2))
            return out
        ma5 = ma_series(closes, 5)
        ma10 = ma_series(closes, 10)
        ma20 = ma_series(closes, 20)
        ma60 = ma_series(closes, 60)

        # --- MACD序列 ---
        dif_arr, dea_arr, bar_arr = [], [], []
        e12 = e26 = closes[0]
        for c in closes:
            e12 = c*(2/13)+e12*(11/13)
            e26 = c*(2/27)+e26*(25/27)
            dif_arr.append(e12-e26)
        dea_val = dif_arr[0]
        for d in dif_arr:
            dea_val = d*(2/10)+dea_val*(8/10)
            dea_arr.append(dea_val)
        bar_arr = [2*(dif_arr[i]-dea_arr[i]) for i in range(n)]

        # --- BOLL序列 ---
        boll_up, boll_mid, boll_low = [], [], []
        for i in range(n):
            if i < 19:
                boll_up.append(None); boll_mid.append(None); boll_low.append(None)
            else:
                sl = closes[i-19:i+1]
                m = sum(sl)/20
                std = (sum((x-m)**2 for x in sl)/20)**0.5
                boll_mid.append(round(m,2))
                boll_up.append(round(m+2*std,2))
                boll_low.append(round(m-2*std,2))

        # --- BIAS序列 ---
        bias6 = [round((closes[i]-sum(closes[max(0,i-5):i+1])/min(6,i+1))/(sum(closes[max(0,i-5):i+1])/min(6,i+1))*100, 2) if i>=5 else None for i in range(n)]

        # --- 带宽 ---
        bw = [round((boll_up[i]-boll_low[i])/boll_mid[i]*100, 2) if boll_up[i] else None for i in range(n)]

        # --- 背离检测（价格 vs MACD柱状图）---
        divergence = None
        # 找近60日两个MACD柱峰值
        recent_bar = bar_arr[-60:]
        recent_close = closes[-60:]
        peaks = []
        for i in range(2, len(recent_bar)-2):
            if recent_bar[i] == max(recent_bar[i-2:i+3]):
                peaks.append((i, recent_close[i], recent_bar[i]))
        if len(peaks) >= 2:
            p1, p2 = peaks[-2], peaks[-1]
            if p2[1] > p1[1] and p2[2] < p1[2]:
                divergence = '顶背离'
            elif p2[1] < p1[1] and p2[2] > p1[2]:
                divergence = '底背离'

        # --- 历史百分位（近500日）---
        def percentile(val, arr):
            valid = [x for x in arr if x is not None]
            if not valid or val is None: return None
            return round(sum(1 for x in valid if x <= val)/len(valid)*100, 0)

        # MA排列状态
        price = closes[-1]
        ma_align = '多头排列' if ma5[-1] and ma10[-1] and ma20[-1] and ma5[-1]>ma10[-1]>ma20[-1] else \
                   ('空头排列' if ma5[-1] and ma10[-1] and ma20[-1] and ma5[-1]<ma10[-1]<ma20[-1] else '交叉纠缠')

        # 指标百分位
        indicators = {
            'ma_align': {'value': ma_align, 'score': 20 if ma_align=='多头排列' else (80 if ma_align=='空头排列' else 50)},
            'boll_pos': {'value': '上轨上方' if price>boll_up[-1] else ('下轨下方' if price<boll_low[-1] else '中轨附近'),
                         'percentile': percentile(price, closes)},
            'boll_bw': {'value': bw[-1], 'percentile': percentile(bw[-1], bw)},
            'macd': {'dif': round(dif_arr[-1],2), 'dea': round(dea_arr[-1],2), 'bar': round(bar_arr[-1],2),
                     'percentile': percentile(dea_arr[-1], dea_arr)},
            'bias6': {'value': bias6[-1], 'percentile': percentile(bias6[-1], bias6)},
            'divergence': {'value': divergence or '无'}
        }

        m = min(200, n)
        payload = {
            'code': 0, 'name': TECH_INDEX_NAME[code], 'index_code': code,
            'updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'dates': dates[-m:],
            'ohlc': [[round(opens[i],2), round(closes[i],2), round(lows[i],2), round(highs[i],2)]
                     for i in range(n - m, n)],
            'volumes': vols[-m:],
            'ma5': ma5[-m:], 'ma10': ma10[-m:], 'ma20': ma20[-m:], 'ma60': ma60[-m:],
            'boll_up': boll_up[-m:], 'boll_mid': boll_mid[-m:], 'boll_low': boll_low[-m:],
            'dif': [round(x,2) for x in dif_arr[-m:]],
            'dea': [round(x,2) for x in dea_arr[-m:]],
            'macd_bar': [round(x,2) for x in bar_arr[-m:]],
            'bias6': bias6[-m:],
            'indicators': indicators
        }
        _tech_cache[code] = (time.time(), payload)
        return jsonify(payload)
    except Exception as e:
        traceback.print_exc()
        return jsonify({'code': -2, 'msg': str(e)})

@app.route('/api/tech_indexes')
def tech_indexes():
    """技术面可选指数清单"""
    return jsonify({'code': 0, 'data': [{'code': c, 'name': n} for c, n in TECH_INDEXES]})

@app.route('/api/fear_greed', methods=['GET', 'POST'])
def fear_greed():
    """恐贪指数：GET 读缓存（含韭圈儿原始分档文案），POST 手动覆盖某日值"""
    cache_file = os.path.join(BASE_DIR, 'fg_cache.json')
    if request.method == 'POST':
        data = request.get_json(force=True) or {}
        if data.get('value') is None:
            return jsonify({'code': 1, 'msg': '缺少 value'})
        try:
            val = float(data['value'])
        except Exception:
            return jsonify({'code': 1, 'msg': 'value 非法'})
        if not (0 <= val <= 100):
            return jsonify({'code': 1, 'msg': 'value 需在 0-100'})
        # 手动值必须带上业务日期，否则自动刷新会把它当成"旧日期数据"丢掉
        day = str(data.get('date') or datetime.now().strftime('%Y-%m-%d'))[:10]
        merged = {'value': round(val, 2), 'date': day,
                  'fetch_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                  'desc': str(data.get('desc') or ''), 'manual': True}
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    old = json.load(f)
                if old.get('history_len'):
                    merged['history_len'] = old['history_len']
                # 手动值日期更早则不覆盖更晚的自动值
                if str(old.get('date', '')) > day:
                    return jsonify({'code': 0, 'value': old.get('value'),
                                    'date': old.get('date'), 'skipped': True})
            except Exception:
                pass
        safe_json_save(cache_file, merged)
        return jsonify({'code': 0, 'value': merged['value'], 'date': merged['date']})

    cached = {'value': 9, 'date': ''}
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cached = json.load(f)
        except Exception:
            pass
    return jsonify({
        'code': 0,
        'value': cached.get('value', 9),
        'date': cached.get('date', ''),
        'fetch_time': cached.get('fetch_time', cached.get('date', '')),
        'desc': cached.get('desc', ''),
        'history_len': cached.get('history_len', 0),
        'manual': bool(cached.get('manual'))
    })

@app.route('/api/fear_greed/history')
def fear_greed_history():
    """韭圈儿恐贪指数历史序列，供趋势图/分位使用"""
    hist_file = os.path.join(BASE_DIR, 'fg_history.json')
    days = request.args.get('days', '0')
    try:
        days = int(days)
    except Exception:
        days = 0
    if not os.path.exists(hist_file):
        return jsonify({'code': 0, 'data': []})
    try:
        with open(hist_file, 'r', encoding='utf-8') as f:
            arr = json.load(f)
    except Exception as e:
        return jsonify({'code': 1, 'msg': str(e)})
    if days > 0:
        arr = arr[-days:]
    return jsonify({'code': 0, 'data': arr})

@app.route('/api/etf_share', methods=['GET', 'POST'])
def etf_share():
    """510300沪深300ETF份额：GET读取缓存，POST更新"""
    import json as _json
    cache_file = os.path.join(BASE_DIR, 'etf_share.json')
    if request.method == 'POST':
        data = request.get_json(force=True)
        # 校验口径与 /api/fear_greed 一致：以前 POST {"percentile":99,"date":"1999-01-01"}
        # 直接返回 code:0 并把线上当日数据覆盖掉，没有任何回退保护。
        if not isinstance(data, dict):
            return jsonify({'code': 1, 'msg': '请求体必须是对象'})
        try:
            val = float(data.get('value'))
        except (TypeError, ValueError):
            return jsonify({'code': 1, 'msg': 'value 非法'})
        if not (0 < val < 100000):
            return jsonify({'code': 1, 'msg': 'value 需为 (0, 100000) 内的份额数'})
        pct_raw = data.get('percentile')
        try:
            pct = float(pct_raw)
        except (TypeError, ValueError):
            return jsonify({'code': 1, 'msg': 'percentile 非法'})
        if not (0 <= pct <= 100):
            return jsonify({'code': 1, 'msg': 'percentile 需在 0-100'})
        day = str(data.get('date') or datetime.now().strftime('%Y-%m-%d'))[:10]
        try:
            datetime.strptime(day, '%Y-%m-%d')
        except ValueError:
            return jsonify({'code': 1, 'msg': 'date 需为 YYYY-MM-DD'})
        if day < '2000-01-01':
            return jsonify({'code': 1, 'msg': 'date 明显异常（早于 2000 年）'})
        # 不允许用更早的日期覆盖已有更新的日期（与 fear_greed 同一规则）
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    old = _json.load(f)
                if str(old.get('date') or '') > day:
                    return jsonify({'code': 0, 'skipped': True,
                                    'value': old.get('value'), 'date': old.get('date')})
            except Exception:
                pass
        merged = dict(data)
        merged['value'] = round(val, 4)
        merged['percentile'] = round(pct, 1)
        merged['date'] = day
        safe_json_save(cache_file, merged)
        return jsonify({'code': 0, 'value': merged['value'],
                        'percentile': merged['percentile'], 'date': day})
    cached = {'value': 374.76, 'percentile': 28, 'date': '2026-10-30'}
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cached = _json.load(f)
        except:
            pass
    # 同时加载历史数据
    hist_file = os.path.join(BASE_DIR, 'etf_share_history.json')
    history = []
    if os.path.exists(hist_file):
        try:
            with open(hist_file, 'r', encoding='utf-8') as f:
                history = _json.load(f)
        except:
            pass
    return jsonify({
        'code': 0,
        'value': cached.get('value'),
        'percentile': cached.get('percentile'),
        'date': cached.get('date'),
        'fetch_time': cached.get('fetch_time', cached.get('date')),
        'sample_n': cached.get('sample_n'),
        'source': cached.get('source', ''),
        'history': history
    })

# ==================== 宏观数据自动获取 ====================
_macro_cache = {'data': None, 'ts': 0}

@app.route('/api/hs300_daily')
def hs300_daily():
    """沪深300近3年每日收盘价，用于ETF份额图叠加"""
    try:
        df = ak.stock_zh_index_daily(symbol='sh000300')
        df['date'] = df['date'].astype(str)
        # 取近3年
        cutoff = (datetime.now() - timedelta(days=3*365)).strftime('%Y-%m-%d')
        df = df[df['date'] >= cutoff]
        result = [[row['date'], round(row['close'],2)] for _, row in df.iterrows()]
        return jsonify({'code':0, 'data': result})
    except Exception as e:
        return jsonify({'code':1, 'msg':str(e)})

@app.route('/api/news')
def news():
    """金十数据快讯：自动拉取10页历史，缓存1分钟，支持下滑查看近200条消息"""
    now = time.time()
    if not hasattr(news, '_cache'):
        news._cache = {'items': None, 'ts': 0}
    if news._cache['items'] and now - news._cache['ts'] < 60:
        return jsonify({'code':0, 'data': news._cache['items']})
    try:
        import urllib.request as _urlreq, urllib.parse as _urlparse
        headers = {'x-app-id':'bVBF4FyRTn5NJF5n','x-version':'1.0.0',
                   'Origin':'https://www.jin10.com','Referer':'https://www.jin10.com/'}
        def _fetch_page(max_t=''):
            t = _urlparse.quote(max_t)
            url = f'https://flash-api.jin10.com/get_flash_list?channel=-8200&vip=1&max_time={t}'
            req = _urlreq.Request(url, headers=headers)
            return json.loads(_urlreq.urlopen(req, timeout=10).read()).get('data', [])
        # 解析单条消息
        def _parse_item(item):
            inner = item.get('data', {}) or {}
            content = inner.get('content','') or ''
            title = inner.get('title','') or ''
            if inner.get('exclusive_to', []): return None
            if not content and not title: return None
            return {'title':title,'content':content,'time':item.get('time',''),'important':item.get('important',0)}
        # 首次加载拉10页历史，后续刷新只拉最新一页
        all_raw = []
        if not news._cache['items']:
            max_t = ''
            for _ in range(10):
                p = _fetch_page(max_t)
                if not p: break
                all_raw.extend(p)
                max_t = p[-1]['time']
        else:
            all_raw = _fetch_page()
        # 解析+去重
        parsed = []
        for it in all_raw:
            p = _parse_item(it)
            if p: parsed.append(p)
        # 合并旧缓存
        if news._cache['items']:
            parsed.extend(news._cache['items'])
        seen = set()
        uniq = []
        for n in parsed:
            if n['time'] not in seen:
                seen.add(n['time'])
                uniq.append(n)
        # 按时间倒序，保留最多200条历史
        uniq.sort(key=lambda x:x['time'], reverse=True)
        uniq = uniq[:200]
        news._cache = {'items': uniq, 'ts': now}
        return jsonify({'code':0, 'data': uniq})
    except Exception as e:
        if news._cache['items']:
            return jsonify({'code':0, 'data': news._cache['items']})
        return jsonify({'code':1, 'msg':str(e)})

# ===== 股指期权T型报价 =====
_OPTION_UNDERLYING = {
    'IO': {'name':'沪深300指数','list_func':'option_cffex_hs300_list_sina','spot_func':'option_cffex_hs300_spot_sina'},
    'HO': {'name':'上证50指数','list_func':'option_cffex_sz50_list_sina','spot_func':'option_cffex_sz50_spot_sina'},
    'MO': {'name':'中证1000指数','list_func':'option_cffex_zz1000_list_sina','spot_func':'option_cffex_zz1000_spot_sina'},
}

@app.route('/api/option/underlying')
def option_underlying():
    """返回支持的标的与挂牌月份（按到期日升序，已交割月份自动剔除）"""
    force = request.args.get('force') == '1'
    result = {}
    for code, cfg in _OPTION_UNDERLYING.items():
        try:
            months = _opt_months(code, force=force)
        except Exception as e:
            months = []
            print('option 月份解析失败 %s: %s' % (code, str(e)[:80]))
        result[code] = {
            'name': cfg['name'],
            'contracts': [m['code'] for m in months],   # 兼容旧前端
            'months': months,
            'error': '' if months else '未获取到挂牌月份',
        }
    return jsonify({'code': 0, 'data': result,
                    'updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

# ===== 中金所官方行情快照（T型报价兜底源，官方、无限流） =====
_CFFEX_QUOTE_FILE = {'IO': 'quote_IO.txt', 'HO': 'quote_HO.txt', 'MO': 'quote_MO.txt'}
_cffex_snap_cache = {}

def _cffex_option_snapshot(prefix):
    """解析中金所官方行情快照，返回 [{strike, call{...}, put{...}}, ...]。

    字段：instrument,position,volume,lastprice,updown,bprice,bamount,sprice,samount
    instrument 形如 IO2611-C-3550。无买卖量时 bid/ask 可能为 0，前端会按 0 处理。
    失败抛异常。
    """
    fn = _CFFEX_QUOTE_FILE.get(prefix)
    if not fn:
        raise ValueError('无中金所快照文件映射: %s' % prefix)
    cached = _cffex_snap_cache.get(prefix)
    if cached and time.time() - cached['ts'] < 5:
        return cached['rows']
    txt = http_get('http://www.cffex.com.cn/' + fn, timeout=10, retry=1)
    lines = [l for l in txt.replace('\r', '').split('\n') if l.strip()]
    if not lines:
        raise ValueError('中金所快照为空')
    # 关键：同一行权价在多个到期月都存在，必须按 (月份, 行权价) 分组，
    # 否则不同月份的合约会互相覆盖，报价就串月了
    by_key = {}
    for line in lines:
        p = line.split(',')
        if len(p) < 9:
            continue
        inst = p[0].strip()
        seg = inst.split('-')
        if len(seg) != 3:
            continue          # 跳过表头等非合约行
        month, cp, strike_s = seg
        try:
            strike = float(strike_s)
            last = float(p[3]); updown = float(p[4])
            bid = float(p[5]); ask = float(p[7])
            oi = float(p[1])
        except Exception:
            continue
        side = 'call' if cp.upper().startswith('C') else 'put'
        key = (month, strike)
        node = by_key.setdefault(key, {'strike': strike, 'month': month})
        node[side] = {
            'code': '%s%s%s' % (month.lower(), 'C' if side == 'call' else 'P',
                                int(strike) if strike == int(strike) else strike),
            'bid': bid, 'ask': ask, 'last': last, 'oi': oi, 'chg': updown
        }
    rows = []
    for (month, strike) in sorted(by_key):
        node = by_key[(month, strike)]
        if 'call' not in node or 'put' not in node:
            continue
        rows.append({'strike': strike, 'month': month,
                     'call': node['call'], 'put': node['put']})
    if not rows:
        raise ValueError('中金所快照无成对合约')
    _cffex_snap_cache[prefix] = {'ts': time.time(), 'rows': rows}
    return rows


_UNDERLYING_QQ_CODE = {'IO': 'sh000300', 'HO': 'sh000016', 'MO': 'sh000852'}
_spot_cache = {}      # {'IO': {'price':x,'chg_pct':y,'ts':..,'time':'..'}}
_SPOT_TTL = 3.0       # 秒级缓存，防止前端轮询把行情源打爆

# ===== 期权标的：同月份股指期货 =====
# 股指期权虽然名义标的是指数，但定价与对冲实际锚定同月份期货（含分红与资金成本，
# 存在基差），因此页面上的"标的价"用期货，指数现货只作为基差参考。
_FUT_OF_OPTION = {'IO': 'IF', 'HO': 'IH', 'MO': 'IM'}
_fut_cache = {}
_FUT_TTL = 3.0


def _fut_code_of(prefix, symbol):
    """io2611 -> IF2611；io2611C4000 -> IF2611；解析不出月份返回 ''。"""
    digits = ''.join(ch for ch in (symbol or '')[2:] if ch.isdigit())
    if len(digits) < 4:
        return ''
    return _FUT_OF_OPTION.get(prefix, '') + digits[:4]


# ===== 挂牌月份：自动排序 + 交割后自动剔除 =====
def _nth_weekday(year, month, weekday, n):
    """某月第 n 个星期几（weekday: 0=周一 … 4=周五）。"""
    first = datetime(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (n - 1))


def _option_expiry(month_code):
    """'2611' -> 2026-11-20。CFFEX 股指期权到期日 = 合约月第三个周五。"""
    mm = int(month_code[2:4])
    yy = 2000 + int(month_code[:2])
    return _nth_weekday(yy, mm, 4, 3)


_opt_month_cache = {}
_OPT_MONTH_TTL = 600


def _opt_months(prefix, force=False):
    """挂牌月份列表，按到期日升序，已交割月份自动剔除。

    月份来源双保险：新浪合约列表（akshare）为主，中金所官方快照兜底，取并集后
    按到期日重排 —— 新浪返回的顺序是乱的（如 io2703 排在 io2701 前面），
    且不保证及时剔除交割月份，所以顺序和有效性都不能直接用它。
    交割日当天 15:00 后即从列表消失，后续月份自然整体前移。
    """
    now_ts = time.time()
    hit = _opt_month_cache.get(prefix)
    if not force and hit and now_ts - hit['ts'] < _OPT_MONTH_TTL:
        return hit['months']

    months = set()
    cfg = _OPTION_UNDERLYING.get(prefix)
    if cfg:
        try:
            data = getattr(ak, cfg['list_func'])()
            for c in list(data.values())[0]:
                digits = ''.join(ch for ch in str(c) if ch.isdigit())
                if len(digits) >= 4:
                    months.add(digits[:4])
        except Exception as e:
            print('option 月份主源失败 %s: %s' % (prefix, str(e)[:80]))
    if not months:
        try:
            for row in _cffex_option_snapshot(prefix):
                digits = ''.join(ch for ch in str(row.get('month') or '') if ch.isdigit())
                if len(digits) >= 4:
                    months.add(digits[:4])
        except Exception as e:
            print('option 月份兜底失败 %s: %s' % (prefix, str(e)[:80]))

    now = datetime.now()
    today = now.date()
    items = []
    for mc in months:
        try:
            exp = _option_expiry(mc)
        except Exception:
            continue
        # 到期日 15:00 收盘即交割，之后不再挂牌
        if exp.date() < today or (exp.date() == today and now.hour >= 15):
            continue
        items.append({'month': mc, 'expiry': exp.strftime('%Y-%m-%d'),
                      'days_left': (exp.date() - today).days, '_exp': exp})
    items.sort(key=lambda x: x['_exp'])
    for i, it in enumerate(items):
        it['code'] = prefix.lower() + it['month']
        it['label'] = '本月' if i == 0 else ('下月' if i == 1 else '%d月' % int(it['month'][2:4]))
        del it['_exp']
    if items:
        _opt_month_cache[prefix] = {'ts': now_ts, 'months': items}
    return items



def _fetch_futures_spot(fut_code):
    """新浪内盘期货实时行情。

    hq_str_nf_IF2611 字段：0开 1高 2低 3最新 4成交量 5成交额 6持仓 7收盘
    13均价 14昨结算 15昨持仓 16最新 ... 末尾为日期/时间/名称。
    涨跌幅以昨结算为基准（交易所口径）。
    """
    now = time.time()
    hit = _fut_cache.get(fut_code)
    if hit and now - hit.get('ts', 0) < _FUT_TTL:
        return hit
    raw = http_get('https://hq.sinajs.cn/list=nf_' + fut_code,
                   referer='https://finance.sina.com.cn', encoding='gbk')
    if '="' not in raw:
        raise ValueError('新浪期货行情格式异常')
    body = raw.split('="', 1)[1].split('"')[0]
    p = body.split(',')
    if len(p) < 17 or not p[3]:
        raise ValueError('期货 %s 无行情（可能未上市或已交割）' % fut_code)
    try:
        price = float(p[3])
    except Exception:
        raise ValueError('期货 %s 最新价解析失败' % fut_code)
    if price <= 0:
        raise ValueError('期货 %s 最新价无效' % fut_code)
    pre = 0.0
    try:
        pre = float(p[14])          # 昨结算
    except Exception:
        pre = 0.0
    if pre <= 0 or abs(price - pre) / pre > 0.2:
        pre = 0.0
    chg_pct = ((price - pre) / pre * 100.0) if pre > 0 else None
    try:
        hold = float(p[6])
    except Exception:
        hold = None
    tstr = ''
    for idx in range(len(p) - 1, 0, -1):
        v = p[idx].strip()
        if len(v) == 8 and v.count(':') == 2:      # 15:00:00
            tstr = (p[idx - 1].strip() + ' ' + v) if len(p[idx - 1].strip()) == 10 else v
            break
    data = {'price': round(price, 2), 'chg_pct': (round(chg_pct, 2) if chg_pct is not None else None),
            'pre_settle': (round(pre, 2) if pre else None), 'hold': hold,
            'time': tstr, 'ts': now, 'source': '新浪期货'}
    _fut_cache[fut_code] = data
    return data


def _sina_fut_kline(fut_code, period, datalen=320):
    """新浪期货K线（日线/分钟线），返回 {'dates','ohlc','vol'}，失败抛异常。

    period: 'day' 或 '5'/'15'/'30'/'60'
    注意日K接口必须带 type=_YYYY_MM_DD 参数，否则返回 400。
    """
    if period == 'day':
        url = ('https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var%20t='
               '/InnerFuturesNewService.getDailyKLine?symbol=' + fut_code +
               '&type=_' + datetime.now().strftime('%Y_%m_%d'))
    else:
        url = ('https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var%20t='
               '/InnerFuturesNewService.getFewMinLine?symbol=' + fut_code +
               '&type=' + str(period))
    txt = http_get(url, referer='https://finance.sina.com.cn')
    if '=(' not in txt:
        raise ValueError('新浪期货K线返回格式异常')
    raw = txt[txt.index('=(') + 2:]
    raw = raw[:raw.rindex(')')]
    arr = json.loads(raw)
    if not isinstance(arr, list) or not arr:
        raise ValueError('期货 %s K线为空' % fut_code)
    dates, ohlc, vol = [], [], []
    for it in arr[-int(datalen):] if period != 'day' else arr[-int(datalen):]:
        try:
            o = float(it['o']); c = float(it['c'])
            h = float(it['h']); l = float(it['l'])
        except Exception:
            continue
        dates.append(str(it.get('d', '')))
        ohlc.append([o, c, h, l])
        try:
            vol.append(float(it.get('v') or 0))
        except Exception:
            vol.append(0.0)
    if not dates:
        raise ValueError('期货 %s K线无有效记录' % fut_code)
    return {'dates': dates, 'ohlc': ohlc, 'vol': vol}

def _fetch_underlying_spot(prefix, force=False):
    """取标的现货最新价。成功更新缓存；失败抛异常（调用方用 last-good 兜底）。"""
    qq = _UNDERLYING_QQ_CODE.get(prefix)
    if not qq:
        raise ValueError('不支持的品种前缀: %s' % prefix)
    now = time.time()
    hit = _spot_cache.get(prefix)
    if not force and hit and now - hit.get('ts', 0) < _SPOT_TTL:
        return hit
    raw = http_get('https://qt.gtimg.cn/q=' + qq, referer='https://gu.qq.com/', encoding='gbk')
    if '="' not in raw:
        raise ValueError('腾讯行情返回格式异常')
    parts = raw.split('="', 1)[1].rstrip('"').split('~')
    if len(parts) < 33:
        raise ValueError('腾讯行情字段不足(%d)' % len(parts))
    price = float(parts[3])
    if price <= 0:
        raise ValueError('腾讯行情最新价无效: %s' % parts[3])
    chg_pct = float(parts[32]) if parts[32] not in ('', '-') else 0.0
    tstr = parts[30] if len(parts) > 30 else ''
    # 20261111161409 -> 2026-11-11 16:14:09
    if len(tstr) >= 14 and tstr.isdigit():
        tstr = '%s-%s-%s %s:%s:%s' % (tstr[0:4], tstr[4:6], tstr[6:8], tstr[8:10], tstr[10:12], tstr[12:14])
    data = {'price': round(price, 2), 'chg_pct': round(chg_pct, 2),
            'time': tstr, 'ts': now, 'source': '腾讯行情'}
    _spot_cache[prefix] = data
    return data

@app.route('/api/option/quote')
def option_quote():
    """获取指定合约的T型报价，symbol如 io2611

    现货价与T型报价完全解耦：任一环节失败都不影响另一个，
    只要拿到数据就返回 code:0，用 spot_ok / quote_ok 标记可用性。
    现货价失败时用上一次成功值兜底，并标注 stale。
    """
    symbol = request.args.get('symbol', '').lower()
    if not symbol or len(symbol) < 2:
        return jsonify({'code': 1, 'msg': '参数错误'})
    prefix = symbol[:2].upper()
    if prefix not in _OPTION_UNDERLYING:
        return jsonify({'code': 1, 'msg': '不支持的品种'})

    resp = {'code': 0, 'symbol': symbol, 'data': [], 'quote_ok': False,
            'spot_ok': False, 'stale': False}

    # ---------- 1. 标的实时价：指数现货为主口径，期货只作基差参考 ----------
    # 股指期权的标的物就是指数本身（IO→沪深300、HO→上证50、MO→中证1000），
    # 行权价、内在价值、溢价率、ATM 全都按指数点位判断；期货与指数存在基差，
    # 拿期货价当标的价会让虚实值判断和现价线整体偏移，因此这里以指数为准，
    # 期货价仅用于展示基差（期货 - 指数）。
    fut_code = _fut_code_of(prefix, symbol)
    index_price = None
    fut_price = None

    # 1a. 指数现货（主口径）
    try:
        idx = _fetch_underlying_spot(prefix)
        index_price = idx['price']
        resp.update({
            'underlying_price': idx['price'],
            'underlying_chg_pct': idx['chg_pct'],
            'underlying_kind': 'index',
            'underlying_code': _UNDERLYING_QQ_CODE[prefix],
            'underlying_name': _OPTION_UNDERLYING[prefix]['name'],
            'spot_time': idx.get('time', ''),
            'spot_ok': True,
            'underlying_source': idx.get('source', '腾讯行情'),
        })
    except Exception as e:
        last = _spot_cache.get(prefix)
        if last:
            index_price = last['price']
            resp.update({
                'underlying_price': last['price'],
                'underlying_chg_pct': last['chg_pct'],
                'underlying_kind': 'index',
                'underlying_code': _UNDERLYING_QQ_CODE[prefix],
                'underlying_name': _OPTION_UNDERLYING[prefix]['name'],
                'spot_time': last.get('time', ''),
                'spot_ok': True, 'stale': True,
                'underlying_source': last.get('source', '腾讯行情'),
            })
        resp['spot_index_error'] = '%s: %s' % (type(e).__name__, str(e)[:100])

    # 指数价同时保留在 spot_index_price 里（老前端字段兼容）
    if index_price:
        resp['spot_index_price'] = index_price
        resp['spot_index_chg_pct'] = resp.get('underlying_chg_pct')

    # 1b. 同月份期货：只用于算基差
    if fut_code:
        try:
            fut = _fetch_futures_spot(fut_code)
            fut_price = fut['price']
            resp.update({
                'fut_price': fut['price'],
                'fut_chg_pct': fut['chg_pct'],
                'fut_code': fut_code,
                'fut_hold': fut.get('hold'),
                'fut_ok': True,
                'fut_source': fut.get('source', '新浪期货'),
            })
        except Exception as e:
            last_fut = _fut_cache.get(fut_code)
            if last_fut:
                fut_price = last_fut['price']
                resp.update({
                    'fut_price': last_fut['price'],
                    'fut_chg_pct': last_fut['chg_pct'],
                    'fut_code': fut_code,
                    'fut_ok': True, 'fut_stale': True,
                    'fut_source': last_fut.get('source', '新浪期货'),
                })
            resp['fut_error'] = '%s: %s' % (type(e).__name__, str(e)[:110])
            print('期货价获取失败(%s)：%s' % (fut_code, resp['fut_error']))

    # 基差 = 期货 - 指数
    if index_price and fut_price:
        resp['basis'] = round(fut_price - index_price, 2)

    # 1c. 指数拿不到时才降级为期货，并明确标注，避免让人误以为是指数
    if not resp.get('spot_ok'):
        if fut_price:
            resp.update({
                'underlying_price': fut_price,
                'underlying_chg_pct': resp.get('fut_chg_pct'),
                'underlying_kind': 'futures',
                'underlying_code': fut_code,
                'underlying_name': _OPTION_UNDERLYING[prefix]['name'] + '期货（指数不可用，降级）',
                'spot_time': '',
                'spot_ok': True, 'stale': True,
                'underlying_source': '新浪期货（降级）',
            })
        else:
            resp['spot_error'] = '指数与期货均不可用'

    # ---------- 2. T型报价（新浪优先，中金所官方快照兜底） ----------
    quote_src = ''
    try:
        spot_func = getattr(ak, _OPTION_UNDERLYING[prefix]['spot_func'])
        df = spot_func(symbol=symbol)
        rows = []
        for _, r in df.iterrows():
            rows.append({
                'strike': float(r['行权价']),
                'call': {
                    'code': r['看涨合约-标识'],
                    'bid': float(r['看涨合约-买价']),
                    'ask': float(r['看涨合约-卖价']),
                    'last': float(r['看涨合约-最新价']),
                    'oi': float(r['看涨合约-持仓量']),
                    'chg': float(r['看涨合约-涨跌'])
                },
                'put': {
                    'code': r['看跌合约-标识'],
                    'bid': float(r['看跌合约-买价']),
                    'ask': float(r['看跌合约-卖价']),
                    'last': float(r['看跌合约-最新价']),
                    'oi': float(r['看跌合约-持仓量']),
                    'chg': float(r['看跌合约-涨跌'])
                }
            })
        resp['data'] = rows
        resp['quote_ok'] = True
        quote_src = '新浪期权T型报价'
    except Exception as e1:
        resp['quote_error'] = '%s: %s' % (type(e1).__name__, str(e1)[:120])
        print('新浪T型报价失败(symbol=%s)：%s，改用中金所官方快照' % (symbol, resp['quote_error']))
        # 兜底：中金所官方行情快照（含全部到期月，必须按当前合约月份过滤）
        try:
            all_rows = _cffex_option_snapshot(prefix)
            month_tag = symbol[2:]          # 如 io2611 -> 2611
            picked = [r for r in all_rows
                      if not month_tag or str(r.get('month', '')).lower().endswith(month_tag)]
            if picked:
                resp['data'] = picked
                resp['quote_ok'] = True
                resp['quote_error'] = None
                quote_src = '中金所官方快照（兜底）'
        except Exception as e2:
            resp['quote_error'] = '新浪:%s / 中金所:%s' % (str(e1)[:80], str(e2)[:80])
            print('T型报价两个数据源均失败(symbol=%s)：%s' % (symbol, resp['quote_error']))
    resp['quote_source'] = quote_src

    if not resp['quote_ok'] and not resp['spot_ok']:
        resp['code'] = 1
        resp['msg'] = resp.get('quote_error') or resp.get('spot_error') or '数据源不可用'
    return jsonify(resp)

# ===== 分钟K线数据源（全部绕开东财，避免限流） =====
def _sina_index_kline(index_code, scale, datalen=320):
    """新浪指数K线：index_code 形如 sh000300；scale 支持 5/15/30/60（分钟）与 240（日）。
    返回 {'dates':[], 'ohlc':[[o,c,h,l],...], 'vol':[]}，失败抛异常。"""
    url = ('https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData'
           '?symbol=%s&scale=%d&ma=no&datalen=%d' % (index_code, int(scale), int(datalen)))
    txt = http_get(url, referer='https://finance.sina.com.cn')
    arr = json.loads(txt)
    if not isinstance(arr, list) or not arr:
        raise ValueError('新浪指数K线返回为空')
    dates, ohlc, vol = [], [], []
    for it in arr:
        d = str(it.get('day', ''))
        try:
            o = float(it['open']); c = float(it['close'])
            h = float(it['high']); l = float(it['low'])
        except Exception:
            continue
        dates.append(d)
        ohlc.append([o, c, h, l])
        try:
            vol.append(float(it.get('volume') or 0))
        except Exception:
            vol.append(0.0)
    if not dates:
        raise ValueError('新浪指数K线无有效记录')
    return {'dates': dates, 'ohlc': ohlc, 'vol': vol}

def _sina_option_dayline(opt_code):
    """新浪期权日K（自解析 JSONP，不用 akshare）。

    akshare 的 option_cffex_*_daily_sina 直接 ast.literal_eval 括号内文本，
    遇到「月代码」或空数据（新浪返回 `(null)`）会抛 SyntaxError，对用户毫无意义。
    这里自己解析：拿到 null / 空数组就抛出可读原因。
    opt_code 必须带看涨/看跌标识，形如 io2611c4000。
    """
    code = (opt_code or '').strip().lower()
    if not re.search(r'[cp]\d', code):
        raise ValueError('需要具体合约（含 C/P 与行权价，如 io2611c4000），月代码没有K线')
    now = time.localtime()
    url = ('https://stock.finance.sina.com.cn/futures/api/jsonp.php/var%%20_%s%d_%d_%d'
           '=/FutureOptionAllService.getOptionDayline?symbol=%s'
           % (code, now.tm_year, now.tm_mon, now.tm_mday, code))
    txt = http_get(url, referer='https://stock.finance.sina.com.cn/option/quotes.html',
                   timeout=15, retry=2)
    lo, hi = txt.find('['), txt.rfind(']')
    if lo < 0 or hi <= lo:
        raise ValueError('新浪期权日K无数据（合约可能未成交或已摘牌）')
    rows = json.loads(txt[lo:hi + 1])
    if not rows:
        raise ValueError('新浪期权日K无数据（合约可能未成交或已摘牌）')
    dates, ohlc, vol = [], [], []
    for it in rows:
        try:
            dates.append(str(it['d']))
            ohlc.append([float(it['o']), float(it['c']), float(it['h']), float(it['l'])])
            vol.append(float(it.get('v') or 0))
        except Exception:
            continue
    if not dates:
        raise ValueError('新浪期权日K无有效记录')
    return {'dates': dates, 'ohlc': ohlc, 'vol': vol}


def _em_option_kline(opt_code, klt, limit=320):
    """东财期权K线（尝试性源）。opt_code 形如 io2611C4000，klt=1/5/15/30/60。
    失败抛异常，由调用方降级。"""
    secid = '151.' + opt_code.upper()
    url = ('https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=%s'
           '&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56'
           '&klt=%d&fqt=1&end=20500101&lmt=%d' % (secid, int(klt), int(limit)))
    js = http_get_json(url, referer='https://quote.eastmoney.com/', retry=1, timeout=10)
    node = (js or {}).get('data') or {}
    klines = node.get('klines') or []
    if not klines:
        raise ValueError('东财期权K线为空')
    dates, ohlc, vol = [], [], []
    for line in klines:
        p = line.split(',')
        if len(p) < 6:
            continue
        try:
            # f51时间, f52开, f53收, f54高, f55低, f56成交量
            dates.append(p[0])
            ohlc.append([float(p[1]), float(p[2]), float(p[3]), float(p[4])])
            vol.append(float(p[5]))
        except Exception:
            continue
    if not dates:
        raise ValueError('东财期权K线无有效记录')
    return {'dates': dates, 'ohlc': ohlc, 'vol': vol}

# K线结果短缓存，避免来回切周期反复打数据源
_kline_cache = {}

@app.route('/api/option/kline')
def option_kline():
    """期权K线 + 同周期标的K线对比。

    period: day / 60 / 30 / 15 / 5 / 1
    - 标的指数：日线走新浪日K，分钟走新浪 CN_MarketDataService（稳定、无限流）
    - 期权合约：日线走新浪期权日K，分钟走东财K线（东财限流时返回空并给出原因，
      绝不把日线冒充成分钟线）
    """
    code = (request.args.get('code', '') or '').strip()
    period = (request.args.get('period', 'day') or 'day').strip().lower()
    if not code:
        return jsonify({'code': 1, 'msg': '参数错误'})
    prefix = code[:2].upper()
    if prefix not in _UNDERLYING_QQ_CODE:
        return jsonify({'code': 1, 'msg': '不支持的品种'})

    cache_key = '%s|%s' % (code, period)
    hit = _kline_cache.get(cache_key)
    if hit and time.time() - hit['ts'] < 60:
        return jsonify(hit['payload'])

    underlying_code = _UNDERLYING_QQ_CODE[prefix]
    fut_code = _fut_code_of(prefix, code)
    opt_data = {'dates': [], 'ohlc': [], 'vol': []}
    u_data = {'dates': [], 'ohlc': [], 'vol': []}
    opt_reason = ''
    opt_detail = ''
    u_reason = ''
    u_kind = 'index'
    u_used = underlying_code

    # ---------- 1. 标的K线：指数现货优先（标的就是指数），期货仅降级 ----------
    try:
        if period == 'day':
            u_df = ak.stock_zh_index_daily(symbol=underlying_code).tail(250)
            u_data = {
                'dates': u_df['date'].astype(str).tolist(),
                'ohlc': u_df[['open', 'close', 'high', 'low']].values.tolist(),
                'vol': u_df['volume'].tolist()
            }
        else:
            u_data = _sina_index_kline(underlying_code, int(period), datalen=320)
    except Exception as e:
        u_reason = '指数 %s %s周期K线不可用：%s' % (underlying_code, period, str(e)[:90])
        print('option_kline 指数失败(%s,%s)：%s' % (underlying_code, period, e))
    if not u_data['dates'] and fut_code:
        # 指数拿不到才降级到期货，并明确标注口径
        try:
            u_data = _sina_fut_kline(fut_code, period, datalen=(250 if period == 'day' else 320))
            u_kind = 'futures'
            u_used = fut_code
            u_reason = (u_reason + '｜已降级为期货K线') if u_reason else '已降级为期货K线'
        except Exception as e:
            if not u_reason:
                u_reason = '标的K线不可用：%s' % str(e)[:100]
            print('option_kline 期货降级失败(%s,%s)：%s' % (fut_code, period, e))

    # ---------- 2. 期权K线 ----------
    try:
        if period == 'day':
            opt_data = _sina_option_dayline(code)
        else:
            opt_data = _em_option_kline(code, int(period), limit=320)
    except Exception as e:
        opt_detail = '%s: %s' % (type(e).__name__, str(e)[:160])
        if period == 'day':
            opt_reason = '期权日K不可用 · %s' % str(e)[:90]
        else:
            opt_reason = ('期权%s分钟K线不可用 · 东财不提供中金所股指期权分钟线，'
                          '公开源里暂无可替代的接口 · 标的指数分钟线不受影响，可先切回日K'
                          % period)
        print('option_kline 期权失败(%s,%s)：%s' % (code, period, opt_detail))

    payload = {
        'code': 0,
        'period': period,
        'data': {'option': opt_data, 'underlying': u_data},
        'opt_reason': opt_reason,
        'opt_error_detail': opt_detail,
        'underlying_reason': u_reason,
        'underlying_kind': u_kind,
        'underlying_code': u_used,
        'opt_source': '新浪期权日K' if period == 'day' else '东财期权分钟K',
        'underlying_source': (
            ('新浪期货%sK' % ('日' if period == 'day' else (period + '分钟'))) if u_kind == 'futures'
            else ('新浪指数%sK' % ('日' if period == 'day' else '分钟')))    }
    _kline_cache[cache_key] = {'ts': time.time(), 'payload': payload}
    return jsonify(payload)


# ==================== 指数 + 恐贪组合面板 ====================
_INDEX_PANEL = [
    ('sh000001', '上证指数'),
    ('sz399001', '深证成指'),
    ('sz399006', '创业板指'),
    ('sh000688', '科创50'),
]
_index_sent_cache = {'ts': 0, 'days': 0, 'payload': None}
_INDEX_SENT_TTL = 300      # 5分钟，盘中够用又不会反复打新浪


def _fg_series_map():
    """恐贪历史 -> {date: value}"""
    path = os.path.join(BASE_DIR, 'fg_history.json')
    out = {}
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                for item in json.load(f):
                    try:
                        out[str(item[0])[:10]] = float(item[1])
                    except Exception:
                        continue
        except Exception:
            pass
    return out


@app.route('/api/index_sentiment')
def index_sentiment():
    """四大指数收盘点位 + 恐贪指数，按统一交易日序列对齐。

    前端负责归一化（基期=100）与图例开关；这里只返回原始值，避免口径写死。
    """
    try:
        days = int(request.args.get('days', 0))
    except Exception:
        days = 0
    # days<=0 = 全部可用历史（自动跟随数据源最长区间）；传正数则按条数截取
    days = 0 if days <= 0 else max(20, min(4000, days))
    datalen = 1800 if days <= 0 else min(1800, days + 15)

    hit = _index_sent_cache
    if hit['payload'] and hit['days'] == days and time.time() - hit['ts'] < _INDEX_SENT_TTL:
        return jsonify(hit['payload'])

    raw = {}
    for code, name in _INDEX_PANEL:
        try:
            d = _sina_index_kline(code, 240, datalen=datalen)
        except Exception as e:
            print('index_sentiment 拉取失败 %s：%s' % (code, e))
            continue
        m = {}
        for i, dt in enumerate(d['dates']):
            m[dt[:10]] = d['ohlc'][i][1]          # close
        if m:
            raw[code] = {'name': name, 'map': m}

    if not raw:
        return jsonify({'code': 1, 'msg': '指数数据暂不可用'})

    # 以样本最全的指数日期序列作为主时间轴
    base_code = max(raw.keys(), key=lambda c: len(raw[c]['map']))
    all_dates = sorted(raw[base_code]['map'].keys())
    dates = all_dates[-days:] if days else all_dates

    fg = _fg_series_map()
    # 恐贪历史起点决定了这张图的可用下界（再往前的指数点位没有对比物）
    if dates and fg:
        fg_start = min(fg.keys())
        trimmed = [d for d in dates if d >= fg_start]
        if len(trimmed) >= 20:
            dates = trimmed
    items = []
    for code, name in _INDEX_PANEL:
        if code not in raw:
            continue
        m = raw[code]['map']
        vals, last_valid = [], None
        for dt in dates:
            v = m.get(dt)
            if v is None:
                v = last_valid             # 停牌/缺失沿用前值，保持序列连续
            else:
                last_valid = v
            vals.append(v)
        # 前段缺失（该指数上市较晚）直接跳过该点
        clean = [v for v in vals if v is not None]
        if len(clean) < 5:
            continue
        first = clean[0]
        prev = clean[-2] if len(clean) > 1 else first
        items.append({
            'key': code, 'name': name, 'type': 'index',
            'values': vals,
            'last': round(clean[-1], 2),
            'chg_pct': round((clean[-1] / prev - 1) * 100, 2) if prev else None,
            'norm_base': first,
        })

    # 恐贪指数
    fg_vals, last_fg = [], None
    for dt in dates:
        v = fg.get(dt)
        if v is None:
            v = last_fg
        else:
            last_fg = v
        fg_vals.append(v)
    if any(v is not None for v in fg_vals):
        tail = [v for v in fg_vals if v is not None]
        items.append({
            'key': 'fear_greed', 'name': '恐贪指数', 'type': 'fear',
            'values': fg_vals,
            'last': round(tail[-1], 2),
            'chg_pct': None,
        })

    payload = {'code': 0, 'days': len(dates), 'dates': dates,
               'start': dates[0], 'end': dates[-1], 'items': items,
               'updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    _index_sent_cache.update({'ts': time.time(), 'days': days, 'payload': payload})
    return jsonify(payload)

def _fred_csv(series_id):
    """从FRED获取CSV数据，返回[(date_str, value_str), ...]"""
    import urllib.request, io, csv
    url = f'https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}'
    req = urllib.request.urlopen(url, timeout=15)
    rows = list(csv.reader(io.StringIO(req.read().decode())))
    return [(r[0], r[1]) for r in rows[1:] if len(r) >= 2 and r[1] not in ('.', '')]

@app.route('/api/macro')
def macro():
    """获取宏观指标最新值，缓存5分钟"""
    now = time.time()
    if _macro_cache['data'] and now - _macro_cache['ts'] < 5 * 60:
        return jsonify(_macro_cache['data'])

    result = {'code': 0, 'data': {}}
    try:
        # 1. LPR 1年期（每月20日公布）
        lpr = ak.macro_china_lpr()
        lpr['TRADE_DATE'] = pd.to_datetime(lpr['TRADE_DATE'])
        latest = lpr.sort_values('TRADE_DATE').iloc[-1]
        next_lpr = latest['TRADE_DATE'] + pd.DateOffset(months=1)
        next_lpr = next_lpr.replace(day=20)
        result['data']['lpr'] = {'value': round(float(latest['LPR1Y']), 3),
                                 'date': str(latest['TRADE_DATE'].date()),
                                 'next_date': next_lpr.strftime('%Y-%m-%d')}
    except Exception as e:
        result['data']['lpr'] = {'error': str(e)[:100]}

    try:
        # 2. 中国10Y国债 + 美国10Y
        bond = ak.bond_zh_us_rate()
        bond['日期'] = pd.to_datetime(bond['日期'])
        latest = bond.sort_values('日期').iloc[-1]
        cn10 = latest.get('中国国债收益率10年')
        us10 = latest.get('美国国债收益率10年')
        result['data']['cny10y'] = {'value': round(float(cn10), 3) if pd.notna(cn10) else None,
                                    'date': str(latest['日期'].date())}
        result['data']['us10y'] = {'value': round(float(us10), 3) if pd.notna(us10) else None,
                                   'date': str(latest['日期'].date())}
    except Exception as e:
        result['data']['cny10y'] = {'error': str(e)[:100]}
        result['data']['us10y'] = {'error': str(e)[:100]}

    try:
        # 3. 中国CPI同比（次月9-12日公布）
        cpi = ak.macro_china_cpi()
        cpi['月份'] = pd.to_datetime(cpi['月份'].str.replace('年','-').str.replace('月份','').str.strip())
        cpi = cpi.dropna(subset=['全国-同比增长'])
        latest = cpi.sort_values('月份').iloc[-1]
        next_cpi = latest['月份'] + pd.DateOffset(months=2)
        next_cpi = next_cpi.replace(day=9)
        result['data']['cncpi'] = {'value': round(float(latest['全国-同比增长']), 2),
                                  'date': str(latest['月份'].date()),
                                  'next_date': next_cpi.strftime('%Y-%m-%d')}
    except Exception as e:
        result['data']['cncpi'] = {'error': str(e)[:100]}

    try:
        # 4. PMI
        pmi = ak.macro_china_pmi()
        pmi['日期'] = pd.to_datetime(pmi['月份'].str.replace('年','-').str.replace('月份','').str.strip())
        pmi = pmi.dropna(subset=['制造业-指数'])
        latest = pmi.sort_values('日期').iloc[-1]
        # PMI每月最后一天发布，更新日期=数据月末
        release_date = latest['日期'] + pd.offsets.MonthEnd(0)
        next_pmi_month = latest['日期'] + pd.DateOffset(months=1)
        next_pmi = next_pmi_month + pd.offsets.MonthEnd(0)
        result['data']['pmi'] = {'value': round(float(latest['制造业-指数']), 1),
                                 'date': release_date.strftime('%Y-%m-%d'),
                                 'next_date': next_pmi.strftime('%Y-%m-%d')}
    except Exception as e:
        result['data']['pmi'] = {'error': str(e)[:100]}

    try:
        # 5. 美国联邦基金利率（FRED数据源）
        fred_rows = _fred_csv('FEDFUNDS')
        last_date, last_val = fred_rows[-1]
        # FOMC 2026会议日期: 1/27-28, 3/17-18, 4/28-29, 6/16-17, 7/28-29, 9/15-16, 10/27-28, 12/8-9
        fomc_dates = ['2026-01-28','2026-03-18','2026-04-29','2026-06-17','2026-07-29','2026-09-16','2026-10-28','2026-12-09']
        today_str = datetime.now().strftime('%Y-%m-%d')
        next_fomc = next((d for d in fomc_dates if d > today_str), '2027-01-27')
        result['data']['usr'] = {'value': round(float(last_val), 3),
                                 'date': last_date,
                                 'next_date': next_fomc}
    except Exception as e:
        result['data']['usr'] = {'error': str(e)[:100]}

    try:
        # 6. 美国CPI同比
        uscpi = ak.macro_usa_cpi_yoy()
        uscpi['时间'] = pd.to_datetime(uscpi['时间'])
        uscpi = uscpi.dropna(subset=['现值'])
        uscpi = uscpi.sort_values('时间').reset_index(drop=True)
        latest = uscpi.iloc[-1]
        # 下次公布：数据月+2个月的12日（当月数据次月公布，已过）
        next_release = latest['时间'] + pd.DateOffset(months=2)
        next_date = next_release.replace(day=12).strftime('%Y-%m-%d')
        result['data']['uscpi'] = {'value': round(float(latest['现值']), 2),
                                   'date': str(latest['时间'].date()),
                                   'next_date': next_date}
    except Exception as e:
        result['data']['uscpi'] = {'error': str(e)[:100]}

    try:
        # 7. 美国核心PCE同比（FRED数据源，PCEPILFE计算YoY）
        fred_rows = _fred_csv('PCEPILFE')
        last_date, last_val = fred_rows[-1]
        last_dt = datetime.strptime(last_date, '%Y-%m-%d')
        target = (last_dt.replace(day=1) - timedelta(days=365)).strftime('%Y-%m-%d')
        yoy_val = None
        for r_date, r_val in reversed(fred_rows):
            if r_date <= target:
                yoy_val = (float(last_val) / float(r_val) - 1) * 100
                break
        next_pce = last_dt + pd.DateOffset(months=2)
        next_pce = next_pce + pd.offsets.MonthEnd(0)
        result['data']['uspce'] = {'value': round(yoy_val, 2) if yoy_val else None,
                                   'date': last_date,
                                   'next_date': next_pce.strftime('%Y-%m-%d')}
    except Exception as e:
        result['data']['uspce'] = {'error': str(e)[:100]}

    try:
        # 8. 美国非农新增（FRED数据源，PAYEMS月度变化，单位：万）
        fred_rows = _fred_csv('PAYEMS')
        last_date, last_val = fred_rows[-1]
        prev_date, prev_val = fred_rows[-2]
        change_wan = (float(last_val) - float(prev_val)) / 10  # 千人→万人
        last_dt = datetime.strptime(last_date, '%Y-%m-%d')
        next_nfp = last_dt + pd.DateOffset(months=2)
        first_day = next_nfp.replace(day=1)
        days_until_fri = (4 - first_day.weekday()) % 7
        next_nfp_fri = first_day + pd.Timedelta(days=days_until_fri)
        result['data']['nfp'] = {'value': round(change_wan, 1),
                                 'date': last_date,
                                 'next_date': next_nfp_fri.strftime('%Y-%m-%d')}
    except Exception as e:
        result['data']['nfp'] = {'error': str(e)[:100]}

    result['updated'] = datetime.now().strftime('%Y-%m-%d %H:%M')
    _macro_cache['data'] = result
    _macro_cache['ts'] = now
    return jsonify(result)

# ============ 策略回测v2 ============
import json as _json

def _compute_indicators(df):
    close = df['close'].values
    ma5 = pd.Series(close).rolling(5).mean().values
    ma10 = pd.Series(close).rolling(10).mean().values
    ma20 = pd.Series(close).rolling(20).mean().values
    ma60 = pd.Series(close).rolling(60).mean().values
    std = pd.Series(close).rolling(20).std().values
    upper = ma20 + 2*std
    lower = ma20 - 2*std
    ema12 = pd.Series(close).ewm(span=12, adjust=False).mean().values
    ema26 = pd.Series(close).ewm(span=26, adjust=False).mean().values
    dif = ema12 - ema26
    dea = pd.Series(dif).ewm(span=9, adjust=False).mean().values
    macd_hist = (dif - dea) * 2
    bias6 = (close - ma60) / ma60 * 100
    return {'close':close,'ma5':ma5,'ma10':ma10,'ma20':ma20,'ma60':ma60,
            'boll_up':upper,'boll_mid':ma20,'boll_low':lower,
            'dif':dif,'dea':dea,'macd_hist':macd_hist,'bias6':bias6}

def _score_indicators(ind, i):
    if i < 60: return 50, 50
    c = ind['close'][i]
    ma5,ma10,ma20,ma60 = ind['ma5'][i],ind['ma10'][i],ind['ma20'][i],ind['ma60'][i]
    if ma5>ma10>ma20>ma60: ma_s=10
    elif ma5>ma10>ma20: ma_s=30
    elif ma5<ma10<ma20<ma60: ma_s=90
    elif ma5<ma10<ma20: ma_s=70
    else: ma_s=50
    b = ind['bias6'][i] if not pd.isna(ind['bias6'][i]) else 0
    bias_s = max(0,min(100, 50 - b*3))
    bu,bl = ind['boll_up'][i],ind['boll_low'][i]
    boll_s = max(0,min(100, (c-bl)/(bu-bl)*100)) if (bu-bl)>0 else 50
    dv,dv2 = ind['dif'][i],ind['dea'][i]
    if dv>dv2 and dv>0: macd_s=20
    elif dv>dv2: macd_s=40
    elif dv<dv2 and dv<0: macd_s=80
    else: macd_s=60
    return round((ma_s+bias_s)/2,1), round((boll_s+macd_s)/2,1)

BT_FILE = os.path.join(BASE_DIR, 'bt_snapshots.json')

def _load_bt():
    if os.path.exists(BT_FILE):
        try:
            with open(BT_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if not isinstance(data, dict) or not isinstance(data.get('records'), list):
                return {'records': []}
            return data
        except Exception:
            pass
    return {'records': []}

def _save_bt(data):
    # 走 safe_json_save：它带文件锁 + os.replace 重试，避免 bt_record 与自动记录线程
    # 同时写时 WinError 32 或丢写（以前这里是自带的 tmp+os.replace，无锁无重试）。
    safe_json_save(BT_FILE, data)
    sync_file('bt_snapshots.json')

@app.route('/api/bt_data')
def bt_data():
    """获取/初始化回测所需的每日技术指标数据（近1年，含OHLC）。

    审计教训：旧实现先按「近 1 年」截断，再丢弃前 60 根做指标预热，
    245-60≈183 天，白丢 3 个多月；且文件里已有 >50 条就直接 return，永远冻结。
    现在改成：多取 400 个自然日做预热，只有窗口内的日期才落库；?rebuild=1 强制重建。
    重建时保留已有记录里的 snapshot/s_score/m_score 等字段，只刷新 OHLC 与技术分。
    """
    data = _load_bt()
    rebuild = request.args.get('rebuild') == '1'
    if not rebuild and data.get('records') and len(data['records']) > 50:
        return jsonify({'code':0, 'data': data})
    existing = {}
    for r in data.get('records', []):
        if isinstance(r, dict) and r.get('date'):
            existing[str(r['date'])[:10]] = r
    records_map = dict(existing)
    cutoff = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    warm_start = (datetime.now() - timedelta(days=365 + 400)).strftime('%Y-%m-%d')
    for code, name in [('sh000300','hs300'), ('sh000852','zz1000')]:
        try:
            df = ak.stock_zh_index_daily(symbol=code)
        except Exception as e:
            traceback.print_exc()
            continue
        df['date'] = df['date'].astype(str)
        df = df[df['date'] >= warm_start].reset_index(drop=True)
        if not len(df):
            continue
        ind = _compute_indicators(df)
        for i in range(len(df)):
            if i < 60: continue
            date = df['date'][i]
            if date < cutoff: continue
            rec = records_map.get(date)
            if not rec:
                rec = {'date': date}
            rec[name+'_open'] = round(float(df['open'][i]),2)
            rec[name+'_high'] = round(float(df['high'][i]),2)
            rec[name+'_low'] = round(float(df['low'][i]),2)
            rec[name+'_close'] = round(float(df['close'][i]),2)
            rec[name+'_vol'] = round(float(df['volume'][i]),0)
            rec[name+'_bias6'] = round(float(ind['bias6'][i]), 2) if not pd.isna(ind['bias6'][i]) else 0
            rec[name+'_boll_pos'] = round((ind['close'][i]-ind['boll_low'][i])/(ind['boll_up'][i]-ind['boll_low'][i])*100,1) if (ind['boll_up'][i]-ind['boll_low'][i])>0 else 50
            dv,dv2 = ind['dif'][i],ind['dea'][i]
            if not pd.isna(dv) and not pd.isna(dv2):
                if dv>dv2 and dv>0: macd_s=20
                elif dv>dv2: macd_s=40
                elif dv<dv2 and dv<0: macd_s=80
                else: macd_s=60
            else: macd_s=50
            rec[name+'_macd_score'] = macd_s
            # MA排列分
            ma5,ma10,ma20,ma60 = ind['ma5'][i],ind['ma10'][i],ind['ma20'][i],ind['ma60'][i]
            if not any(pd.isna(x) for x in [ma5,ma10,ma20,ma60]):
                if ma5>ma10>ma20>ma60: ma_s=10
                elif ma5>ma10>ma20: ma_s=30
                elif ma5<ma10<ma20<ma60: ma_s=90
                elif ma5<ma10<ma20: ma_s=70
                else: ma_s=50
            else: ma_s=50
            rec[name+'_ma_score'] = ma_s
            records_map[date] = rec
    records = [records_map[d] for d in sorted(records_map)]
    data = {'records': records, 'updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    _save_bt(data)
    return jsonify({'code':0, 'data': data})

@app.route('/api/bt_record', methods=['POST'])
def bt_record():
    """记录当日指标快照+前端算好的S/M评分"""
    body = request.json or {}
    if not isinstance(body, dict):
        return jsonify({'code': 1, 'msg': '请求体必须是对象'})
    data = _load_bt()
    today = str(body.get('date') or datetime.now().strftime('%Y-%m-%d'))[:10]
    # 日期与评分必须校验：以前 POST {"date":"1900-01-01","s_score":"x"} 会
    # 返回 code:0 并把一条垃圾快照写进 bt_snapshots.json（回测直接吃这堆数据）。
    try:
        datetime.strptime(today, '%Y-%m-%d')
    except ValueError:
        return jsonify({'code': 1, 'msg': 'date 需为 YYYY-MM-DD'})
    if today < '2000-01-01':
        return jsonify({'code': 1, 'msg': 'date 明显异常（早于 2000 年）'})

    def _score(name):
        raw = body.get(name)
        if raw is None:
            return None
        try:
            val = float(raw)
        except (TypeError, ValueError):
            return 'invalid'
        if val != val or not (0 <= val <= 100):
            return 'invalid'
        return round(val, 2)

    s_val = _score('s_score')
    m_val = _score('m_score')
    if s_val == 'invalid' or m_val == 'invalid':
        return jsonify({'code': 1, 'msg': 's_score / m_score 需为 0-100 的数字'})

    def _price(name, lo, hi):
        raw = body.get(name)
        if raw is None:
            return None
        try:
            val = float(raw)
        except (TypeError, ValueError):
            return None
        return round(val, 2) if (val == val and lo < val < hi) else None

    # 读-改-写整段持锁，否则与 _auto_record_worker 并发时会互相覆盖。
    # 合并语义（审计教训）：旧实现无条件 rec['hs300_close'] = _price(...)，
    # 请求里没带价格时 None 会把已有的 close 清空（11-16 的 close 就这么丢的）。
    # 现在只有「本次真的提供了值」才覆盖，缺失字段保留原值，并在响应里回传缺失 id。
    with _save_lock_for(BT_FILE):
        data = _load_bt()
        rec = next((r for r in data['records'] if r['date']==today), None)
        if not rec:
            rec = {'date': today}
            data['records'].append(rec)
        snap = body.get('indicators')
        if isinstance(snap, list) and snap:
            rec['snapshot'] = snap
        if s_val is not None:
            rec['s_score'] = s_val
        if m_val is not None:
            rec['m_score'] = m_val
        for field, lo, hi in (('hs300_close', 0, 100000), ('zz1000_close', 0, 100000)):
            val = _price(field, lo, hi)
            if val is not None:
                rec[field] = val
        rec['updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data['records'].sort(key=lambda x: x['date'])
        _save_bt(data)
    # 回传缺失的指标 id：前端据此提示「哪些指标今天没算出来」，而不是静默丢项
    missing_ids = []
    try:
        cfg_ids = []
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                cfg_ids = [i.get('id') for i in (json.load(f).get('indicators') or []) if i.get('id')]
        got = set()
        if isinstance(rec.get('snapshot'), list):
            got = {i.get('id') for i in rec['snapshot'] if isinstance(i, dict)}
        missing_ids = [i for i in cfg_ids if i not in got]
    except Exception:
        pass
    return jsonify({'code':0, 'date': today, 's':rec.get('s_score'), 'm':rec.get('m_score'),
                    'filled': len(rec.get('snapshot') or []), 'missing_ids': missing_ids})


# ==================== 历史评分快照回填（数据补全） ====================
# 用户反馈「历史数据补全都有数据接口的」：S/M/L 评分并非只能在实时抓取时记录。
# 只要把每项指标的历史日频序列取回来，就能逐日重算指标值与评分：
#   · 中证1000 日K          -> tech_ma / tech_boll / tech_macd / tech_bias（与回测同公式）
#   · 两市成交额历史         -> turn（万亿）/ turn_pct（滚动 1200 日分位）
#   · 沪深两市融资余额       -> margin（亿元）/ margin_pct（滚动 750 日分位，合计口径）
#   · 300ETF 份额历史        -> etf_share_pct（滚动 750 日分位）
#   · 韭圈儿恐贪历史         -> fg
# 宏观（lpr/pmi/…）、ERP、涨跌家数没有可对齐的日频历史序列，快照里 cur=None，
# 评分时按权重跳过（不再用 50 冒充中性值）。
_HIST_BACKTESTABLE = {'tech_ma', 'tech_boll', 'tech_macd', 'tech_bias',
                      'turn_pct', 'margin_pct', 'margin', 'turn', 'fg', 'etf_share_pct'}


def _rolling_pct(values_map, window, min_samples=60):
    """按日期升序计算「当日值在此前 window 个交易日中的分位」（不含当日，避免前视）。"""
    out = {}
    past = []
    for d in sorted(values_map.keys()):
        v = values_map[d]
        if len(past) >= min_samples:
            below = sum(1 for x in past if x < v)
            out[d] = round(below / len(past) * 100, 1)
        past.append(v)
        if len(past) > window:
            past.pop(0)
    return out


def _hist_series_for_backfill():
    """回填需要的历史序列：成交额、沪深融资余额、ETF 份额、恐贪。"""
    out = {'turn': {}, 'margin': {}, 'etf': {}, 'fg': {}}
    try:
        out['turn'] = dict(_turnover_amount_map())
    except Exception:
        traceback.print_exc()
    try:
        df_sh = ak.stock_margin_sse(start_date='20190101',
                                    end_date=datetime.now().strftime('%Y%m%d'))
        df_sh = df_sh.sort_values('信用交易日期')
        sh = {}
        for _, row in df_sh.iterrows():
            raw = str(row['信用交易日期'])
            sh['%s-%s-%s' % (raw[:4], raw[4:6], raw[6:8])] = \
                round(float(row['融资余额']) / 1e8, 2)
        sz = _margin_sz_map(sorted(sh.keys())) if sh else {}
        if sz:
            out['margin'] = {d: round(v + sz[d], 2) for d, v in sh.items() if d in sz}
        else:
            out['margin'] = sh
    except Exception:
        traceback.print_exc()
    try:
        p = os.path.join(BASE_DIR, 'etf_share_history.json')
        if os.path.exists(p):
            with open(p, 'r', encoding='utf-8') as f:
                for d, v in json.load(f):
                    out['etf'][str(d)[:10]] = float(v)
    except Exception:
        traceback.print_exc()
    try:
        p = os.path.join(BASE_DIR, 'fg_history.json')
        if os.path.exists(p):
            with open(p, 'r', encoding='utf-8') as f:
                for it in json.load(f):
                    if it and len(it) >= 2:
                        try:
                            out['fg'][str(it[0])[:10]] = float(it[1])
                        except (TypeError, ValueError):
                            pass
    except Exception:
        traceback.print_exc()
    return out


def _pct_inc(val, arr):
    """看板口径分位：窗口内 <= val 的占比（/api/tech 用 x<=val）"""
    if val is None:
        return None
    vals = [x for x in arr if x is not None]
    if len(vals) < 60:
        return None
    return round(sum(1 for x in vals if x <= val) / len(vals) * 100, 0)


def _trailing_rank_map(values_map, window=750, min_n=60):
    """每个日期用「该日期之前」最近 window 个样本做排名分位。

    与 /api/sentiment 一致：排除当日、近三年（约 750 个交易日）样本，
    低于当前值的占比 * 100。"""
    out = {}
    ds = sorted(values_map)
    vals = [values_map[d] for d in ds]
    for i, d in enumerate(ds):
        win = vals[max(0, i - window):i]
        if len(win) < min_n:
            continue
        v = values_map[d]
        out[d] = sum(1 for x in win if x < v) / len(win) * 100
    return out


def _pos_map(values_map, start=None, end=None):
    """区间位置分位：以 [start,end] 内样本的最小/最大值为 0/100。

    用户要求：回测区间里每条分位曲线只应有一个最低点和一个最高点。
    排名分位（低于当前值的占比）在单边行情会连续出现多个 100/0，
    这里改成「区间内位置」，天然只有一个 0 和一个 100。
    """
    items = [(d, v) for d, v in values_map.items()
             if v and (not start or str(d) >= str(start)) and (not end or str(d) <= str(end))]
    if len(items) < 2:
        return {}
    vals = [v for _, v in items]
    lo, hi = min(vals), max(vals)
    if hi <= lo:
        return {}
    items.sort(key=lambda x: x[0])
    # 并列极值只给一个点：取第一次出现的日期作为唯一最高/最低
    min_d = next(d for d, v in items if v == lo)
    max_d = next(d for d, v in items if v == hi)
    out = {}
    for d, v in items:
        if d == min_d:
            out[d] = 0.0
        elif d == max_d:
            out[d] = 100.0
        else:
            # 非极值点夹在 0.1~99.9，避免并列值四舍五入后也变成 0/100
            p = (v - lo) / (hi - lo) * 100
            out[d] = round(max(0.1, min(99.9, p)), 2)
    return out

def build_snapshot_history(days=750, write=True):
    """用历史接口逐日回算指标快照与 S/M/L 评分，合并写入 bt_snapshots.json。"""
    cfg = {}
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                cfg = _migrate_config(json.load(f))
        except Exception:
            traceback.print_exc()
    inds = (cfg.get('indicators') or [])
    if not inds:
        return {'code': 1, 'msg': 'bt_config 为空，无法回填'}
    hist = _hist_series_for_backfill()
    turn_map, margin_map = hist['turn'], hist['margin']
    etf_map, fg_map = hist['etf'], hist['fg']
    # 区间位置：整段历史里最低=0、最高=100（唯一极值点）
    turn_pct = _pos_map(turn_map)
    margin_pct = _pos_map(margin_map)
    etf_pct = _pos_map(etf_map)

    days = max(60, min(2500, int(days)))
    cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    df = ak.stock_zh_index_daily(symbol='sh000852')
    df['date'] = df['date'].astype(str)
    warm = (datetime.now() - timedelta(days=days + 1600)).strftime('%Y-%m-%d')
    df = df[df['date'] >= warm].reset_index(drop=True)
    if len(df) <= 60:
        return {'code': 1, 'msg': '中证1000 历史数据不足'}

    close = df['close'].astype(float)
    ma5 = close.rolling(5).mean().values
    ma10 = close.rolling(10).mean().values
    ma20 = close.rolling(20).mean().values
    ma60 = close.rolling(60).mean().values
    ma6 = close.rolling(6).mean().values
    std20 = close.rolling(20).std().values
    up = ma20 + 2 * std20
    low = ma20 - 2 * std20
    ema12 = close.ewm(span=12, adjust=False).mean().values
    ema26 = close.ewm(span=26, adjust=False).mean().values
    dif = ema12 - ema26
    dea = pd.Series(dif).ewm(span=9, adjust=False).mean().values
    closes = close.values
    bias_win = []

    margin_dates = sorted(margin_map)
    etf_dates = sorted(etf_map)
    fg_dates = sorted(fg_map)
    mi = ei = fi = 0
    cur_margin = cur_etf = 0.0
    cur_fg = None
    snaps = {}
    for i in range(len(df)):
        d = df['date'][i]
        while mi < len(margin_dates) and margin_dates[mi] <= d:
            cur_margin = margin_map[margin_dates[mi]]
            mi += 1
        while ei < len(etf_dates) and etf_dates[ei] <= d:
            cur_etf = etf_map[etf_dates[ei]]
            ei += 1
        while fi < len(fg_dates) and fg_dates[fi] <= d:
            cur_fg = fg_map[fg_dates[fi]]
            fi += 1
        b6 = 0.0
        if not pd.isna(ma6[i]) and ma6[i]:
            b6 = (closes[i] - ma6[i]) / ma6[i] * 100
        bias_win.append(b6)
        if len(bias_win) > 1200:
            bias_win.pop(0)
        if i < 60 or d < cutoff:
            continue
        c = closes[i]
        # 技术指标（与 /api/bt_backtest 同一套公式）
        # 与看板 /api/tech/sh000852 同口径：MA 排列 20/80/50；BOLL=价格在近 750 日收盘中的分位；
        # MACD=DEA 在近 750 日中的分位；BIAS6 在近 750 日中的分位
        if ma5[i] > ma10[i] > ma20[i]:
            ma_s = 20
        elif ma5[i] < ma10[i] < ma20[i]:
            ma_s = 80
        else:
            ma_s = 50
        boll_pos = _pct_inc(c, closes[max(0, i - 749):i + 1])
        macd_s = _pct_inc(dea[i], dea[max(0, i - 749):i + 1])
        bias_pct = _pct_inc(b6, bias_win[-750:])
        snap = []
        for ind in inds:
            iid = ind.get('id')
            entry = dict(ind)
            entry['cur'] = None
            # 与 /api/bt_backtest 同口径：不可回测指标（宏观/ERP/涨跌家数等）
            # 权重清零，S/M 只由可重建指标决定；否则同一日期快照 s/m 与
            # 回测 short/mid 会对不上。
            if iid not in _HIST_BACKTESTABLE:
                entry['wS'] = 0
                entry['wM'] = 0
                entry['wL'] = 0
            if iid == 'tech_ma':
                entry['cur'] = ma_s
            elif iid == 'tech_boll':
                entry['cur'] = round(boll_pos, 1) if boll_pos is not None else None
            elif iid == 'tech_macd':
                entry['cur'] = macd_s
            elif iid == 'tech_bias':
                entry['cur'] = round(bias_pct, 0) if bias_pct is not None else None
            elif iid == 'turn':
                v = turn_map.get(d, 0) or 0
                entry['cur'] = round(v / 10000.0, 3) if v > 0 else None
            elif iid == 'turn_pct':
                p = turn_pct.get(d)
                entry['cur'] = round(p, 1) if p is not None else 50
            elif iid == 'margin':
                entry['cur'] = round(cur_margin, 0) if cur_margin > 0 else None
            elif iid == 'margin_pct':
                p = margin_pct.get(d)
                entry['cur'] = round(p, 1) if p is not None else 50
            elif iid == 'etf_share_pct':
                p = etf_pct.get(d)
                entry['cur'] = round(p, 1) if p is not None else 50
            elif iid == 'fg':
                entry['cur'] = round(cur_fg, 2) if cur_fg is not None else None
            snap.append(entry)
        snaps[d] = {
            'snapshot': snap,
            's_score': _composite_score(snap, 'S'),
            'm_score': _composite_score(snap, 'M'),
            'l_score': _composite_score(snap, 'L'),
        }

    filled = 0
    if write and snaps:
        with _save_lock_for(BT_FILE):
            data = _load_bt()
            by_date = {str(r.get('date'))[:10]: r for r in data.get('records', [])
                       if isinstance(r, dict) and r.get('date')}
            for d, s in snaps.items():
                rec = by_date.get(d)
                if not rec:
                    rec = {'date': d}
                    by_date[d] = rec
                rec.update(s)
                rec['updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                filled += 1
            data['records'] = [by_date[d] for d in sorted(by_date)]
            _save_bt(data)
    order = sorted(snaps)
    return {'code': 0, 'days': days, 'filled': filled,
            'from': order[0] if order else None,
            'to': order[-1] if order else None,
            'sources': {'turn': len(turn_map), 'margin': len(margin_map),
                        'etf': len(etf_map), 'fg': len(fg_map)}}


@app.route('/api/bt_backfill')
def bt_backfill():
    """历史评分快照回填（数据补全）。?days=750&write=0 可只预览不落盘。"""
    try:
        days = int(request.args.get('days', 750))
    except (TypeError, ValueError):
        days = 750
    write = request.args.get('write', '1') != '0'
    try:
        return jsonify(build_snapshot_history(days=days, write=write))
    except Exception as e:
        traceback.print_exc()
        return jsonify({'code': 1, 'msg': str(e)})


def _bt_backfill_if_needed(force=False):
    """启动时检查近 90 天评分快照覆盖，缺得多就自动回填一次。

    force=True 时无视「近 90 天已覆盖」，直接用扩展后的历史序列重建 1500 天
    （首次深回填 ETF 份额 / 补齐 2022-09 前深市融资余额后需要重算旧快照，
    否则旧快照里 margin/etf_share_pct 是空值或错误口径）。
    """
    try:
        if not force:
            try:
                cal = _trade_date_set() or set()
                start = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
                today_s = datetime.now().strftime('%Y-%m-%d')
                # 只算已发生的交易日：交易日历含未来日期（如 2026-12-31），
                # 不加上界会把「未来」也算成缺失，导致每次启动都重复回填。
                need_dates = sorted(d for d in cal if start <= d <= today_s)
            except Exception:
                need_dates = []
            recs = _load_bt().get('records', [])
            have = {str(r.get('date'))[:10] for r in recs
                    if isinstance(r, dict) and r.get('snapshot')}
            missing = [d for d in need_dates if d not in have]
            if len(missing) <= 10:
                covered = len([d for d in need_dates if d in have])
                print('[历史补全] 近 90 天评分快照已覆盖 %d/%d，无需回填'
                      % (covered, len(need_dates)))
                return
            print('[历史补全] 近 90 天评分快照缺 %d 个交易日，开始用历史接口回填...' % len(missing))
            days = 750
        else:
            print('[历史补全] 历史序列已扩展，重建 5 年评分快照...')
            days = 1900
        r = build_snapshot_history(days=days, write=True)
        print('[历史补全] 完成：%s ~ %s 共 %d 条（%s）'
              % (r.get('from'), r.get('to'), r.get('filled'), r.get('msg') or 'ok'))
    except Exception:
        traceback.print_exc()


def _score_of(ind):
    """与前端 scoreOf() 相同逻辑"""
    cur = ind.get('cur')
    if cur is None: return None
    mn, neu, mx = ind.get('min',0), ind.get('neutral',50), ind.get('max',100)
    d = ind.get('dir','direct')
    if d == 'direct': return max(0, min(100, cur))
    if cur <= mn: s = 0
    elif cur >= mx: s = 100
    elif cur <= neu: s = (cur - mn) / (neu - mn) * 50 if neu > mn else 50
    else: s = 50 + (cur - neu) / (mx - neu) * 50 if mx > neu else 50
    if d == 'high_bull': s = 100 - s
    return max(0, min(100, s))

def _composite_score(snapshot, wk):
    """与前端 composite(wk) 相同逻辑: 加权平均"""
    s, w = 0, 0
    for ind in snapshot:
        score = _score_of(ind)
        if score is None: continue
        weight = ind.get('w'+wk, 0)
        if weight > 0:
            s += score * weight
            w += weight
    return round(s / w, 1) if w > 0 else 50

# 仓位档位 -> 展示文案。必须与 pos_factor {1:0.5, 2:1.0} 一致：
# 档位 1 是半仓，档位 2 已是满仓，以前写「多1/3 / 多2/3」与实际仓位不符。
POS_LABEL = {0: '空仓', 1: '半仓多', 2: '全仓多', 3: '全仓多', -1: '半仓空', -2: '全仓空'}

# 单边手续费（含佣金与滑点，按成交金额比例），开平各收一次。
# 原实现完全不计成本，短周期频繁进出会把结果系统性抬高。
FEE_RATE = 0.0008


def _trade_target(ttype, cur):
    """交易类型 -> 该笔成交后的目标仓位档位（0 空仓 / 1 半仓 / 2 全仓，负数为做空）"""
    if ttype == 'buy1': return 1
    if ttype == 'buy2': return 2
    if ttype == 'buy3': return 3
    if ttype == 'sell1': return max(cur - 1, 0) if cur > 0 else 0
    if ttype == 'sell2': return 0
    if ttype == 'short1': return -1
    if ttype == 'short2': return -2
    if ttype == 'cover1': return min(cur + 1, 0) if cur < 0 else 0
    if ttype == 'cover2': return 0
    return cur


def _consume_lots(lots, direction, units, price):
    """按 FIFO 平掉 direction 方向（1 多 / -1 空）的 units 手（单位=半仓）。

    返回 (加权盈亏百分比之和, 平掉的手数)。手数可能是零碎的一半，所以用加权。
    """
    pnl_sum = 0.0
    closed = 0.0
    remaining = units
    while remaining > 1e-9 and lots:
        lot = lots[0]
        if lot['dir'] != direction:
            lots.pop(0)
            continue
        take = min(lot['qty'], remaining)
        if direction == 1:
            pnl = (price - lot['price']) / lot['price'] * 100
        else:
            pnl = (lot['price'] - price) / lot['price'] * 100
        pnl_sum += pnl * take
        closed += take
        lot['qty'] -= take
        remaining -= take
        if lot['qty'] <= 1e-9:
            lots.pop(0)
    return pnl_sum, closed


def _pair_trades(trades):
    """把成交流水配成一次开→平，返回 (wins, losses, total_win, total_loss)。

    旧实现只认 sell2/cover2 作为平仓，把 sell1/cover1（减半仓）整段丢掉，
    导致胜率与盈亏比系统性失真。这里按 FIFO 手数配对，分批建仓/分批平仓都算。
    """
    cur = 0
    lots = []                # [{'dir': 1/-1, 'price': float, 'qty': float}]
    wins = losses = 0
    total_win = total_loss = 0.0
    for t in trades:
        price = t.get('price') or 0
        if price <= 0:
            continue
        target = _trade_target(t.get('type'), cur)
        delta = target - cur
        if delta == 0:
            continue
        if delta > 0:
            need = float(delta)
            if cur < 0:
                pnl_sum, closed = _consume_lots(lots, -1, min(need, float(-cur)), price)
                if closed > 0:
                    pnl = pnl_sum / closed
                    if pnl > 0: wins += 1; total_win += pnl
                    else: losses += 1; total_loss += abs(pnl)
                    need -= closed
            if need > 1e-9:
                lots.append({'dir': 1, 'price': price, 'qty': need})
        else:
            need = float(-delta)
            if cur > 0:
                pnl_sum, closed = _consume_lots(lots, 1, min(need, float(cur)), price)
                if closed > 0:
                    pnl = pnl_sum / closed
                    if pnl > 0: wins += 1; total_win += pnl
                    else: losses += 1; total_loss += abs(pnl)
                    need -= closed
            if need > 1e-9:
                lots.append({'dir': -1, 'price': price, 'qty': need})
        cur = target
    return wins, losses, total_win, total_loss


def _win_ratio(wins, losses, total_win, total_loss):
    total = wins + losses
    win_rate = round(wins / total * 100, 1) if total > 0 else 0
    profit_loss = round((total_win / wins) / (total_loss / losses), 2) if wins > 0 and losses > 0 else 0
    return win_rate, profit_loss


@app.route('/api/bt_backtest')
def bt_backtest():
    """回测：从历史每日指标+当前权重动态计算S/M，模拟交易"""
    import json as _json
    override_weights = {}
    w_param = request.args.get('weights', '')
    if w_param:
        try: override_weights = _json.loads(w_param)
        except: pass
    p = {
        'long_buy_mid': float(request.args.get('long_buy_mid', 80)),
        'long_buy_s1': float(request.args.get('long_buy_s1', 15)),
        'long_buy_s2': float(request.args.get('long_buy_s2', 13)),
        'long_buy_s3': float(request.args.get('long_buy_s3', 13)),
        'long_close_mid': float(request.args.get('long_close_mid', 45)),
        'long_close_s1': float(request.args.get('long_close_s1', 60)),
        'long_close_s2': float(request.args.get('long_close_s2', 85)),
        'short_open_mid': float(request.args.get('short_open_mid', 80)),
        'short_open_s1': float(request.args.get('short_open_s1', 80)),
        'short_open_s2': float(request.args.get('short_open_s2', 85)),
        'short_close_mid': float(request.args.get('short_close_mid', 66)),
        'short_close_s1': float(request.args.get('short_close_s1', 40)),
        'short_close_s2': float(request.args.get('short_close_s2', 20)),
        'stop_loss': float(request.args.get('stop_loss', 0)),      # 止损%，0=关闭
        'take_profit': float(request.args.get('take_profit', 0)),  # 止盈%，0=关闭
    }
    # 读取当前配置
    cfg_indicators = []
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                cfg_indicators = json.load(f).get('indicators', [])
        except: pass
    if not cfg_indicators:
        cfg_indicators = [
            {'id':'tech_ma','cur':50,'min':0,'neutral':50,'max':100,'dir':'direct','wS':3,'wM':1,'wL':0},
            {'id':'tech_boll','cur':50,'min':0,'neutral':50,'max':100,'dir':'direct','wS':1,'wM':0,'wL':0},
            {'id':'tech_macd','cur':50,'min':0,'neutral':50,'max':100,'dir':'direct','wS':8,'wM':1,'wL':0},
            {'id':'tech_bias','cur':50,'min':0,'neutral':50,'max':100,'dir':'direct','wS':5,'wM':0,'wL':0},
            {'id':'turn_pct','cur':50,'min':0,'neutral':50,'max':100,'dir':'high_bear','wS':0,'wM':9,'wL':9},
            {'id':'margin_pct','cur':50,'min':0,'neutral':50,'max':100,'dir':'high_bear','wS':0,'wM':9,'wL':9},
            {'id':'etf_share_pct','cur':50,'min':0,'neutral':50,'max':100,'dir':'high_bull','wS':0,'wM':5,'wL':5},
        ]
    # 应用前端覆盖权重
    if override_weights:
        for ind in cfg_indicators:
            iid = ind.get('id')
            if iid in override_weights:
                ow = override_weights[iid]
                if 'S' in ow: ind['wS'] = ow['S']
                if 'M' in ow: ind['wM'] = ow['M']
                if 'L' in ow: ind['wL'] = ow['L']

    # 可回测的指标ID（有历史数据的）
    BACKTESTABLE = {'tech_ma','tech_boll','tech_macd','tech_bias','turn_pct','margin_pct',
                    'margin','turn','fg','etf_share_pct'}

    # 解析时间范围
    period = request.args.get('period', '1y')
    days_map = {'10y':3650,'5y':1825,'3y':1095,'2y':730,'1y':365}
    n_days = days_map.get(period, 365)
    start_date = request.args.get('start', '')
    end_date = request.args.get('end', '')
    if not start_date:
        start_date = (datetime.now() - timedelta(days=n_days)).strftime('%Y-%m-%d')

    # === 加载中证1000 K线（技术指标计算 + 交易标的） ===
    df_zz = pd.DataFrame()
    try:
        df_zz = ak.stock_zh_index_daily(symbol='sh000852')
        df_zz['date'] = df_zz['date'].astype(str)
        warmup = (datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=1600)).strftime('%Y-%m-%d')
        df_zz = df_zz[df_zz['date'] >= warmup]
        if end_date: df_zz = df_zz[df_zz['date'] <= end_date]
        df_zz = df_zz.reset_index(drop=True)
    except: pass

    # === 成交额历史（沪+深真实成交额，亿元）：与 /api/sentiment 完全同口径 ===
    vol_map = {}
    try:
        vol_map = dict(_turnover_amount_map())
    except Exception:
        pass

    # === 加载融资余额历史（沪市单市，与/api/sentiment完全一致：stock_margin_sse） ===
    margin_map = {}
    try:
        df_msh = ak.stock_margin_sse(start_date="20200101", end_date=datetime.now().strftime("%Y%m%d"))
        df_msh = df_msh.sort_values('信用交易日期')
        for _, row in df_msh.iterrows():
            raw = str(row['信用交易日期'])
            d = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"  # 20261029 -> 2026-10-29
            margin_map[d] = float(row['融资余额'])  # 元
    except: pass
    # 深市融资余额（东财日频）合并成沪深合计，与看板 /api/sentiment 同口径
    try:
        _sz_need = sorted(margin_map.keys())
        _sz_map = _margin_sz_map(_sz_need) if _sz_need else {}
        if _sz_map:
            margin_map = {d: v + _sz_map[d] for d, v in margin_map.items() if d in _sz_map}
    except Exception:
        traceback.print_exc()

    # 融资余额百分位：滚动近 750 个交易日排名（与看板 sentiment 同口径）
    _range_end = end_date or None
    margin_pct_map = _pos_map(margin_map, start_date, _range_end)

    # === 加载300ETF份额历史（iFinD数据） ===
    etf_share_map = {}
    try:
        etf_file = os.path.join(BASE_DIR, 'etf_share_history.json')
        if os.path.exists(etf_file):
            with open(etf_file, 'r', encoding='utf-8') as f:
                etf_data = json.load(f)
            for d, v in etf_data:
                etf_share_map[d] = float(v)
    except: pass

    # ETF 份额百分位：滚动近 750 个样本排名
    etf_pct_map = _pos_map(etf_share_map, start_date, _range_end)

    # === 加载市场恐贪历史（韭圈儿，2020-08 起） ===
    fg_hist_map = {}
    try:
        fg_file = os.path.join(BASE_DIR, 'fg_history.json')
        if os.path.exists(fg_file):
            with open(fg_file, 'r', encoding='utf-8') as f:
                for item in json.load(f):
                    if not item or len(item) < 2:
                        continue
                    try:
                        fg_hist_map[str(item[0])[:10]] = float(item[1])
                    except (TypeError, ValueError):
                        continue
    except Exception:
        pass
    fg_dates_sorted = sorted(fg_hist_map.keys())

    # 成交额百分位：滚动近 750 个交易日排名（与看板 sentiment 同口径）
    turn_pct_map = _pos_map(vol_map, start_date, _range_end)

    # === O(n)单遍计算每日指标评分 ===
    n = len(df_zz)
    daily_scores = {}
    ohlc_map = {}
    if n > 60:
        closes = df_zz['close'].tolist()
        e12 = closes[0]; e26 = closes[0]; dea_val = 0
        dea_series = []
        bias_window = []
        vol_window = []
        margin_window = []  # 融资余额滚动窗口（按周期）
        margin_dates_sorted = sorted(margin_map.keys())
        margin_idx = 0
        cur_margin_yuan = 0
        etf_dates_sorted = sorted(etf_share_map.keys())
        etf_window = []  # ETF份额累计窗口
        etf_idx = 0
        cur_etf_shares = 0
        cur_etf_date = None
        fg_idx = 0
        cur_fg = None   # 恐贪：按日期前向填充
        # 百分位窗口与/api/sentiment一致：融资500天，成交额1200天
        margin_window_max = 500
        vol_window_max = 1200
        for i in range(n):
            c = closes[i]
            d = df_zz['date'][i]
            e12 = c*(2/13)+e12*(11/13)
            e26 = c*(2/27)+e26*(25/27)
            dif = e12 - e26
            if i == 0: dea_val = dif
            else: dea_val = dif*(2/10)+dea_val*(8/10)
            dea_series.append(dea_val)
            # volume窗口
            v = vol_map.get(d, 0)
            if v > 0:
                vol_window.append(v)
                if len(vol_window) > vol_window_max: vol_window.pop(0)
            # margin窗口（滚动500天，与看板/api/sentiment口径一致）
            while margin_idx < len(margin_dates_sorted) and margin_dates_sorted[margin_idx] <= d:
                mv = margin_map[margin_dates_sorted[margin_idx]]
                if mv > 0:
                    cur_margin_yuan = mv
                    if margin_dates_sorted[margin_idx] >= '2024-10-01':
                        margin_window.append(mv)
                        if len(margin_window) > 500: margin_window.pop(0)
                margin_idx += 1
            # BIAS6 在预热段也要累计，否则 start 之后前 60 个交易日没有分位窗口
            ma6 = sum(closes[i-5:i+1])/6
            bias6 = (c-ma6)/ma6*100 if ma6 else 0
            bias_window.append(bias6)
            if len(bias_window) > 1200: bias_window.pop(0)
            if i < 59: continue
            if start_date and d < start_date: continue
            # 与看板 /api/tech/sh000852 同口径：MA 排列 20/80/50
            ma5 = sum(closes[i-4:i+1])/5
            ma10 = sum(closes[i-9:i+1])/10
            ma20 = sum(closes[i-19:i+1])/20
            if ma5 > ma10 > ma20: ma_s = 20
            elif ma5 < ma10 < ma20: ma_s = 80
            else: ma_s = 50
            # BOLL = 价格在近 750 日收盘中的分位
            boll_pos = _pct_inc(c, closes[max(0, i-749):i+1])
            # MACD = DEA 在近 750 日中的分位
            macd_s = _pct_inc(dea_val, dea_series[-750:])
            # BIAS6 分位（近 750 日，窗口含 start 之前的预热段）
            bias_pct = _pct_inc(bias6, bias_window[-750:])
            # 成交额分位（预计算：与看板同口径）
            turn_pct = turn_pct_map.get(d) if v > 0 else None
            # 融资余额分位（预计算：区间位置）
            margin_pct = margin_pct_map.get(d) if cur_margin_yuan > 0 else None
            # 融资余额绝对值（亿元）
            margin_yi = cur_margin_yuan / 1e8 if cur_margin_yuan > 0 else 0
            # ETF份额分位（预计算：整个数据集排名，前向填充）
            while etf_idx < len(etf_dates_sorted) and etf_dates_sorted[etf_idx] <= d:
                cur_etf_shares = etf_share_map[etf_dates_sorted[etf_idx]]
                cur_etf_date = etf_dates_sorted[etf_idx]
                etf_idx += 1
            etf_share_pct = etf_pct_map.get(cur_etf_date) if cur_etf_shares > 0 else None
            # 恐贪值（前向填充：周末/节假日沿用上一交易日）
            while fg_idx < len(fg_dates_sorted) and fg_dates_sorted[fg_idx] <= d:
                cur_fg = fg_hist_map[fg_dates_sorted[fg_idx]]
                fg_idx += 1

            # 构建当日指标快照
            day_indicators = {}
            for ind in cfg_indicators:
                iid = ind['id']
                entry = dict(ind)
                # 可回测指标用历史计算值
                if iid == 'tech_ma': entry['cur'] = ma_s
                elif iid == 'tech_boll': entry['cur'] = round(boll_pos,1) if boll_pos is not None else None
                elif iid == 'tech_macd': entry['cur'] = macd_s
                elif iid == 'tech_bias': entry['cur'] = round(bias_pct,0) if bias_pct is not None else None
                elif iid == 'turn_pct': entry['cur'] = round(turn_pct,1) if turn_pct is not None else None
                elif iid == 'margin_pct': entry['cur'] = round(margin_pct,1) if margin_pct is not None else None
                elif iid == 'etf_share_pct': entry['cur'] = round(etf_share_pct,1) if etf_share_pct is not None else None
                elif iid == 'fg':
                    entry['cur'] = round(cur_fg, 2) if cur_fg is not None else None
                elif iid == 'turn':
                    # 两市成交额绝对值（万亿），无当日数据时置空避免误判
                    entry['cur'] = round(v / 10000.0, 3) if v > 0 else None
                elif iid == 'margin':
                    # 融资余额绝对值（亿元，high_bear：越高越空）；无数据置空
                    if margin_yi <= 0:
                        entry['cur'] = None
                    else:
                        mn = float(entry.get('min',12000)); nt = float(entry.get('neutral',16000)); mx = float(entry.get('max',20000))
                        if margin_yi <= mn: entry['cur'] = 0
                        elif margin_yi >= mx: entry['cur'] = 100
                        elif margin_yi <= nt: entry['cur'] = round((margin_yi-mn)/(nt-mn)*50,1)
                        else: entry['cur'] = round(50+(margin_yi-nt)/(mx-nt)*50,1)
                # 不可回测指标：在10/30之前权重清零
                if iid not in BACKTESTABLE:
                    entry['wS'] = 0; entry['wM'] = 0; entry['wL'] = 0
                day_indicators[iid] = entry
            snap = list(day_indicators.values())
            s_val = _composite_score(snap, 'S')
            m_val = _composite_score(snap, 'M')
            daily_scores[d] = {'short': s_val, 'mid': m_val,
                'margin_pct': round(margin_pct,1) if margin_pct is not None else None,
                'turn_pct': round(turn_pct,1) if turn_pct is not None else None,
                'etf_pct': round(etf_share_pct,1) if etf_share_pct is not None else None}
            ohlc_map[d] = {
                'open': round(float(df_zz['open'][i]),2),
                'high': round(float(df_zz['high'][i]),2),
                'low': round(float(df_zz['low'][i]),2),
                'close': round(float(df_zz['close'][i]),2),
                'volume': float(df_zz['volume'][i]) if 'volume' in df_zz.columns else 0,
            }

    # 前端传入当日实时composite()值，覆盖最后一个交易日。
    # 仅在「最后一个评分日就是今天」且已收盘时覆盖：盘中今天本来就被剔除，
    # 收盘后看板实时合成值才是当天完整口径（回测历史序列只含可回测指标，与看板卡天然不同）。
    today_s_param = request.args.get('today_s', '')
    today_m_param = request.args.get('today_m', '')
    _now_dt = datetime.now()
    _today_str = _now_dt.strftime('%Y-%m-%d')
    last_d = max(daily_scores.keys()) if daily_scores else None
    # 最后一个评分日的交易时段是否已经走完：早于今天=已收盘；就是今天则要等到 15:30 之后
    _kline_last = str(df_zz['date'].iloc[-1]) if len(df_zz) else ''
    _fresh_d = False
    try:
        _fresh_d = bool(last_d) and datetime.strptime(last_d, '%Y-%m-%d') >= (_now_dt - timedelta(days=10))
    except Exception:
        _fresh_d = False
    _last_completed = (bool(last_d) and last_d == _kline_last and _fresh_d
                       and (last_d < _today_str or (last_d == _today_str and is_after_close(_now_dt))))
    if not (today_s_param and today_m_param and _last_completed):
        last_d = None
    if last_d:
        try:
            daily_scores[last_d]['short'] = float(today_s_param)
            daily_scores[last_d]['mid'] = float(today_m_param)
            # 用看板当前值覆盖原始指标
            try:
                sent_cache = os.path.join(BASE_DIR, 'sent_cache.json')
                if os.path.exists(sent_cache):
                    with open(sent_cache, 'r') as f:
                        sent = json.load(f)
                    if 'margin' in sent and sent['margin'].get('percentile') is not None:
                        daily_scores[last_d]['margin_pct'] = float(sent['margin']['percentile'])
                    if 'turnover' in sent and sent['turnover'].get('percentile') is not None:
                        daily_scores[last_d]['turn_pct'] = float(sent['turnover']['percentile'])
                etf_cache = os.path.join(BASE_DIR, 'etf_share.json')
                if os.path.exists(etf_cache):
                    with open(etf_cache, 'r') as f:
                        etf_c = json.load(f)
                    if etf_c.get('percentile') is not None:
                        daily_scores[last_d]['etf_pct'] = float(etf_c['percentile'])
            except: pass
        except: pass

    # 策略模拟（仓位粘性：只增不减，触发卖出/止盈/止损才清仓）
    try:
        leverage = float(request.args.get('leverage', 1))
    except Exception:
        leverage = 1.0
    leverage = max(1.0, min(20.0, leverage))
    scored_dates = sorted(daily_scores.keys())
    # 交易日盘中：当日 K 线还没走完，回测结果里不显示当天（收盘后 15:30 之后再纳入）
    try:
        _now = datetime.now()
        if is_trading_day(_now) and not is_after_close(_now):
            _today_s = _now.strftime('%Y-%m-%d')
            scored_dates = [d for d in scored_dates if d != _today_s]
    except Exception:
        pass
    # === [移植增量] 前端 auto=1：按当前区间 S/M 分位校准阈值（默认勾选；不勾则严格用页面阈值）===
    auto_q = str(request.args.get('auto', '0')).lower() in ('1', 'true', 'yes')
    _s_range = _m_range = None
    if auto_q and daily_scores:
        _sv = sorted(v['short'] for v in daily_scores.values())
        _mv = sorted(v['mid'] for v in daily_scores.values())
        def _q(arr, perc):
            k = int(round(perc * (len(arr) - 1)))
            return round(arr[max(0, min(len(arr) - 1, k))], 1)
        p['long_buy_s2'], p['long_buy_s1'] = _q(_sv, 0.06), _q(_sv, 0.12)
        p['long_close_s1'], p['long_close_s2'] = _q(_sv, 0.85), _q(_sv, 0.93)
        p['short_open_s1'], p['short_open_s2'] = _q(_sv, 0.90), _q(_sv, 0.95)
        p['short_close_s1'], p['short_close_s2'] = _q(_sv, 0.18), _q(_sv, 0.10)
        p['long_buy_mid'], p['long_close_mid'] = _q(_mv, 0.30), _q(_mv, 0.60)
        p['short_open_mid'], p['short_close_mid'] = _q(_mv, 0.78), _q(_mv, 0.22)
        _s_range = [round(_sv[0], 1), round(_sv[-1], 1)]
        _m_range = [round(_mv[0], 1), round(_mv[-1], 1)]
        print('[backtest] 自动校准阈值 buyM=%.1f closeM=%.1f openM=%.1f closeM2=%.1f'
              % (p['long_buy_mid'], p['long_close_mid'], p['short_open_mid'], p['short_close_mid']))
    pos = 0
    entry_price = 0
    trades = []
    position_map = {}
    warmup = 0
    for d in scored_dates:
        sc = daily_scores[d]
        s, m = sc['short'], sc['mid']
        price = ohlc_map.get(d, {}).get('close', 0)
        if price == 0: continue

        # 前15根K线不操作（指标预热）
        warmup += 1
        if warmup <= 15:
            position_map[d] = 0
            continue

        # 止盈止损检查（持仓中）
        if pos > 0 and entry_price > 0:
            chg = (price - entry_price) / entry_price * 100
            if p['stop_loss'] > 0 and chg <= -p['stop_loss']:
                trades.append({'date':d,'price':price,'type':'sell2','label':'止损平多'}); pos = 0; entry_price = 0
                position_map[d] = pos; continue
            if p['take_profit'] > 0 and chg >= p['take_profit']:
                trades.append({'date':d,'price':price,'type':'sell2','label':'止盈平多'}); pos = 0; entry_price = 0
                position_map[d] = pos; continue
        elif pos < 0 and entry_price > 0:
            chg = (entry_price - price) / entry_price * 100
            if p['stop_loss'] > 0 and chg <= -p['stop_loss']:
                trades.append({'date':d,'price':price,'type':'cover2','label':'止损平空'}); pos = 0; entry_price = 0
                position_map[d] = pos; continue
            if p['take_profit'] > 0 and chg >= p['take_profit']:
                trades.append({'date':d,'price':price,'type':'cover2','label':'止盈平空'}); pos = 0; entry_price = 0
                position_map[d] = pos; continue

        if pos == 0:
            # 两档开多：M<80且S<13全仓，M<80且S<15半仓
            if m < p['long_buy_mid'] and s < p['long_buy_s2']:
                pos = 2; entry_price = price; trades.append({'date':d,'price':price,'type':'buy2','label':'全仓买入'})
            elif m < p['long_buy_mid'] and s < p['long_buy_s1']:
                pos = 1; entry_price = price; trades.append({'date':d,'price':price,'type':'buy1','label':'半仓买入'})
            # 两档开空：M>80且S>85全空，M>80且S>80半仓空
            elif m > p['short_open_mid'] and s > p['short_open_s2']:
                pos = -2; entry_price = price; trades.append({'date':d,'price':price,'type':'short2','label':'全仓空'})
            elif m > p['short_open_mid'] and s > p['short_open_s1']:
                pos = -1; entry_price = price; trades.append({'date':d,'price':price,'type':'short1','label':'半仓空'})
        elif pos > 0:
            # 1. 全平：M>45且S>85，任意多仓全平
            if m > p['long_close_mid'] and s > p['long_close_s2']:
                trades.append({'date':d,'price':price,'type':'sell2','label':'全平多'}); pos = 0; entry_price = 0
                # 平多后同一天开空
                if m > p['short_open_mid'] and s > p['short_open_s2']:
                    pos = -2; entry_price = price; trades.append({'date':d,'price':price,'type':'short2','label':'全仓空'})
                elif m > p['short_open_mid'] and s > p['short_open_s1']:
                    pos = -1; entry_price = price; trades.append({'date':d,'price':price,'type':'short1','label':'半仓空'})
            # 2. 减半：M>45且S>60，仅全仓减半，半仓不动
            elif pos == 2 and m > p['long_close_mid'] and s > p['long_close_s1']:
                pos = 1; trades.append({'date':d,'price':price,'type':'sell1','label':'减半仓'})
            # 3. 加仓：半仓M<80且S<13，加仓到全仓
            elif pos == 1 and m < p['long_buy_mid'] and s < p['long_buy_s2']:
                pos = 2; trades.append({'date':d,'price':price,'type':'buy2','label':'加仓→全仓'})
        elif pos < 0:
            # 1. 全平空：M<66且S<20，任意空仓全平
            if m < p['short_close_mid'] and s < p['short_close_s2']:
                trades.append({'date':d,'price':price,'type':'cover2','label':'全平空'}); pos = 0; entry_price = 0
                # 平空后同一天开多
                if m < p['long_buy_mid'] and s < p['long_buy_s2']:
                    pos = 2; entry_price = price; trades.append({'date':d,'price':price,'type':'buy2','label':'全仓买入'})
                elif m < p['long_buy_mid'] and s < p['long_buy_s1']:
                    pos = 1; entry_price = price; trades.append({'date':d,'price':price,'type':'buy1','label':'半仓买入'})
            # 2. 减半空：M<66且S<40，仅全空减半，半空不动
            elif pos == -2 and m < p['short_close_mid'] and s < p['short_close_s1']:
                pos = -1; trades.append({'date':d,'price':price,'type':'cover1','label':'减半空'})
            # 3. 加仓空：半空M>80且S>85，加仓到全空
            elif pos == -1 and m > p['short_open_mid'] and s > p['short_open_s2']:
                pos = -2; trades.append({'date':d,'price':price,'type':'short2','label':'加仓→全仓空'})
        position_map[d] = pos

    # 收益曲线，两档仓位映射
    # 复利只作用于「已平仓收益」：持仓期间按开仓时的本金计算浮动盈亏，平仓后才把盈亏计入本金
    pos_factor = {0:0, 1:0.5, 2:1.0, 3:1.0, -1:-0.5, -2:-1.0}
    realized = 1.0          # 已实现资金（只含已平仓盈亏）
    entry_base = 1.0        # 当前持仓的计息本金（开仓时的已实现资金）
    open_pnl = 0.0          # 当前持仓的浮动盈亏（相对 entry_base 的绝对金额）
    prev_pf = 0
    equity_curve = []; bench_curve = []; eq_series = [1.0]; bench_rets = []
    bankrupt = False
    for i in range(1, len(scored_dates)):
        d0 = scored_dates[i-1]; d1 = scored_dates[i]
        p0 = ohlc_map.get(d0, {}).get('close', 0)
        p1 = ohlc_map.get(d1, {}).get('close', 0)
        if p0 == 0 or p1 == 0: continue
        b_ret = (p1/p0-1)
        pf = pos_factor.get(position_map.get(d0, 0), 0)
        if pf != prev_pf:
            # 仓位变化（开仓/加仓/减仓/反手/平仓）：先结算旧仓浮盈，再以最新已实现资金作新仓本金
            realized += open_pnl
            open_pnl = 0.0
            entry_base = realized
            prev_pf = pf
        if pf != 0:
            open_pnl += entry_base * (pf * leverage * b_ret)
        equity = realized + open_pnl
        equity_curve.append({'date':d1,'equity':round((equity-1)*100,2)})  # 累计收益率%，从0开始
        eq_series.append(max(equity, 0.0))
        bench_curve.append({'date':d1,'bench':round((p1/p0)*100,2)})
        bench_rets.append(b_ret)
        if equity <= 0:
            # 爆仓：权益归零后不再复利（避免负数本金产生无意义数字）
            bankrupt = True
            equity = 0.0
            equity_curve.append({'date':d1,'equity':-100.0})
            break
    realized += open_pnl
    equity = max(realized, 0.0)

    n_ret = len(bench_rets)
    total_ret = round((equity-1)*100, 1)
    sharpe = max_dd = beta = win_rate = profit_loss = 0
    strat_rets = []
    if n_ret > 1:
        for j in range(1, len(eq_series)):
            prev_eq = eq_series[j-1]
            strat_rets.append((eq_series[j]-prev_eq)/prev_eq if prev_eq > 0 else -1.0)
        if len(strat_rets) > 1:
            avg = sum(strat_rets)/len(strat_rets)
            std = (sum((r-avg)**2 for r in strat_rets)/(len(strat_rets)-1))**0.5
            sharpe = round(avg/std*(252**0.5), 2) if std > 0 else 0
        peak_eq = 1.0; max_dd = 0
        for eqv in eq_series:
            peak_eq = max(peak_eq, eqv)
            dd = (peak_eq-eqv)/peak_eq if peak_eq > 0 else 0
            max_dd = max(max_dd, dd)
        max_dd = round(max_dd*100, 1)
        if len(bench_rets) > 1:
            b_avg = sum(bench_rets)/len(bench_rets)
            s_avg = sum(strat_rets)/len(strat_rets)
            cov = sum((strat_rets[i]-s_avg)*(bench_rets[i]-b_avg) for i in range(len(strat_rets)))/(len(strat_rets)-1)
            var_b = sum((r-b_avg)**2 for r in bench_rets)/(len(bench_rets)-1)
            beta = round(cov/var_b, 2) if var_b > 0 else 0

    # 胜率/盈亏比：配对买卖交易
    wins = 0; losses = 0; total_win = 0; total_loss = 0
    entry_trade = None
    for t in trades:
        if t['type'] in ('buy1','buy2','buy3') and entry_trade is None:
            entry_trade = t
        elif t['type'] in ('short1','short2') and entry_trade is None:
            entry_trade = t
        elif t['type'] in ('sell2','cover2') and entry_trade is not None:
            entry_price = entry_trade['price']; exit_price = t['price']
            if entry_trade['type'].startswith('buy'):
                pnl = (exit_price - entry_price) / entry_price * 100
            else:
                pnl = (entry_price - exit_price) / entry_price * 100
            if pnl > 0:
                wins += 1; total_win += pnl
            else:
                losses += 1; total_loss += abs(pnl)
            entry_trade = None
    total_trades = wins + losses
    win_rate = round(wins/total_trades*100, 1) if total_trades > 0 else 0
    profit_loss = round((total_win/wins)/(total_loss/losses), 2) if wins > 0 and losses > 0 else 0

    # 构建daily数组
    daily = []
    for d in scored_dates:
        o = ohlc_map.get(d, {})
        sc = daily_scores[d]
        daily.append({'date':d,'open':o.get('open',0),'high':o.get('high',0),
                       'low':o.get('low',0),'close':o.get('close',0),
                       'volume':o.get('volume',0),
                       'short':sc['short'],'mid':sc['mid'],
                       'margin_pct':sc.get('margin_pct'),'turn_pct':sc.get('turn_pct'),
                       'etf_pct':sc.get('etf_pct')})

    pos_label = POS_LABEL.get(pos, '空仓')
    latest_d = scored_dates[-1] if scored_dates else None
    latest_s = daily_scores[latest_d]['short'] if latest_d else None
    latest_m = daily_scores[latest_d]['mid'] if latest_d else None

    def _run_on_index(df_idx, key_name):
        """用同样的S/M信号，在指定指数K线上跑策略"""
        ohlc2 = {}
        for i in range(len(df_idx)):
            d = df_idx['date'][i]
            ohlc2[d] = {
                'open': round(float(df_idx['open'][i]),2),
                'high': round(float(df_idx['high'][i]),2),
                'low': round(float(df_idx['low'][i]),2),
                'close': round(float(df_idx['close'][i]),2),
                'volume': float(df_idx['volume'][i]) if 'volume' in df_idx.columns else 0,
            }
        pos2 = 0; entry2 = 0; trades2 = []; pos_map2 = {}
        warmup2 = 0
        for d in scored_dates:
            sc = daily_scores[d]; s, m = sc['short'], sc['mid']
            price = ohlc2.get(d, {}).get('close', 0)
            if price == 0: continue
            # 前15根K线不操作（指标预热）
            warmup2 += 1
            if warmup2 <= 15:
                pos_map2[d] = 0
                continue
            # 止盈止损
            if pos2 > 0 and entry2 > 0:
                chg = (price-entry2)/entry2*100
                if p['stop_loss']>0 and chg <= -p['stop_loss']:
                    trades2.append({'date':d,'price':price,'type':'sell2','label':'止损平多'}); pos2=0; entry2=0; pos_map2[d]=pos2; continue
                if p['take_profit']>0 and chg >= p['take_profit']:
                    trades2.append({'date':d,'price':price,'type':'sell2','label':'止盈平多'}); pos2=0; entry2=0; pos_map2[d]=pos2; continue
            elif pos2 < 0 and entry2 > 0:
                chg = (entry2-price)/entry2*100
                if p['stop_loss']>0 and chg <= -p['stop_loss']:
                    trades2.append({'date':d,'price':price,'type':'cover2','label':'止损平空'}); pos2=0; entry2=0; pos_map2[d]=pos2; continue
                if p['take_profit']>0 and chg >= p['take_profit']:
                    trades2.append({'date':d,'price':price,'type':'cover2','label':'止盈平空'}); pos2=0; entry2=0; pos_map2[d]=pos2; continue
            if pos2 == 0:
                # 两档开多：M<80且S<13全仓，M<80且S<15半仓
                if m < p['long_buy_mid'] and s < p['long_buy_s2']:
                    pos2 = 2; entry2 = price; trades2.append({'date':d,'price':price,'type':'buy2','label':'全仓买入'})
                elif m < p['long_buy_mid'] and s < p['long_buy_s1']:
                    pos2 = 1; entry2 = price; trades2.append({'date':d,'price':price,'type':'buy1','label':'半仓买入'})
                # 两档开空：M>80且S>85全空，M>80且S>80半仓空
                elif m > p['short_open_mid'] and s > p['short_open_s2']:
                    pos2 = -2; entry2 = price; trades2.append({'date':d,'price':price,'type':'short2','label':'全仓空'})
                elif m > p['short_open_mid'] and s > p['short_open_s1']:
                    pos2 = -1; entry2 = price; trades2.append({'date':d,'price':price,'type':'short1','label':'半仓空'})
            elif pos2 > 0:
                # 1. 全平：M>45且S>85，任意多仓全平
                if m > p['long_close_mid'] and s > p['long_close_s2']:
                    trades2.append({'date':d,'price':price,'type':'sell2','label':'全平多'}); pos2=0; entry2=0
                    if m > p['short_open_mid'] and s > p['short_open_s2']:
                        pos2 = -2; entry2 = price; trades2.append({'date':d,'price':price,'type':'short2','label':'全仓空'})
                    elif m > p['short_open_mid'] and s > p['short_open_s1']:
                        pos2 = -1; entry2 = price; trades2.append({'date':d,'price':price,'type':'short1','label':'半仓空'})
                # 2. 减半：M>45且S>60，仅全仓减半，半仓不动
                elif pos2 == 2 and m > p['long_close_mid'] and s > p['long_close_s1']:
                    pos2 = 1; trades2.append({'date':d,'price':price,'type':'sell1','label':'减半仓'})
                # 3. 加仓：半仓M<80且S<13，加仓到全仓
                elif pos2 == 1 and m < p['long_buy_mid'] and s < p['long_buy_s2']:
                    pos2 = 2; trades2.append({'date':d,'price':price,'type':'buy2','label':'加仓→全仓'})
            elif pos2 < 0:
                # 1. 全平空：M<66且S<20，任意空仓全平
                if m < p['short_close_mid'] and s < p['short_close_s2']:
                    trades2.append({'date':d,'price':price,'type':'cover2','label':'全平空'}); pos2=0; entry2=0
                    if m < p['long_buy_mid'] and s < p['long_buy_s2']:
                        pos2 = 2; entry2 = price; trades2.append({'date':d,'price':price,'type':'buy2','label':'全仓买入'})
                    elif m < p['long_buy_mid'] and s < p['long_buy_s1']:
                        pos2 = 1; entry2 = price; trades2.append({'date':d,'price':price,'type':'buy1','label':'半仓买入'})
                # 2. 减半空：M<66且S<40，仅全空减半，半空不动
                elif pos2 == -2 and m < p['short_close_mid'] and s < p['short_close_s1']:
                    pos2 = -1; trades2.append({'date':d,'price':price,'type':'cover1','label':'减半空'})
                # 3. 加仓空：半空M>80且S>85，加仓到全空
                elif pos2 == -1 and m > p['short_open_mid'] and s > p['short_open_s2']:
                    pos2 = -2; trades2.append({'date':d,'price':price,'type':'short2','label':'加仓→全仓空'})
            pos_map2[d] = pos2
        pf_map = {0:0, 1:0.5, 2:1.0, 3:1.0, -1:-0.5, -2:-1.0}
        eq2 = 1.0
        realized2 = 1.0; base2 = 1.0; pnl2 = 0.0; prev_pr = 0
        eq_curve2 = []; s_rets2 = []; b_rets2 = []; eq2_series = [1.0]
        for i in range(1, len(scored_dates)):
            d0 = scored_dates[i-1]; d1 = scored_dates[i]
            p0 = ohlc2.get(d0, {}).get('close', 0); p1 = ohlc2.get(d1, {}).get('close', 0)
            if p0 == 0 or p1 == 0: continue
            br = (p1/p0-1); pr = pf_map.get(pos_map2.get(d0,0), 0)
            if pr != prev_pr:
                realized2 += pnl2
                pnl2 = 0.0
                base2 = realized2
                prev_pr = pr
            if pr != 0:
                pnl2 += base2 * (pr * leverage * br)
            eq2v = realized2 + pnl2
            eq_curve2.append({'date':d1,'equity':round((eq2v-1)*100,2)})  # 累计收益率%，从0开始
            eq2_series.append(max(eq2v, 0.0))
            b_rets2.append(br)
            if eq2v <= 0:
                eq2v = 0.0
                eq_curve2.append({'date':d1,'equity':-100.0})
                break
        realized2 += pnl2
        eq2 = max(realized2, 0.0)
        nr = len(b_rets2); tr2 = round((eq2-1)*100,1)
        sh2 = mdd2 = bt2 = 0
        if nr > 1:
            for j in range(1, len(eq2_series)):
                _pv = eq2_series[j-1]
                s_rets2.append((eq2_series[j]-_pv)/_pv if _pv > 0 else -1.0)
            if len(s_rets2) > 1:
                avg2 = sum(s_rets2)/len(s_rets2)
                std2 = (sum((r-avg2)**2 for r in s_rets2)/(len(s_rets2)-1))**0.5
                sh2 = round(avg2/std2*(252**0.5),2) if std2 > 0 else 0
            pk = 1.0
            for _v in eq2_series:
                pk = max(pk, _v)
                dd = (pk-_v)/pk if pk > 0 else 0; mdd2 = max(mdd2, dd)
            mdd2 = round(mdd2*100,1)
        daily2 = []
        for d in scored_dates:
            o = ohlc2.get(d, {}); sc = daily_scores[d]
            daily2.append({'date':d,'open':o.get('open',0),'high':o.get('high',0),
                           'low':o.get('low',0),'close':o.get('close',0),
                           'volume':o.get('volume',0),
                           'short':sc['short'],'mid':sc['mid'],
                           'margin_pct':sc.get('margin_pct'),'turn_pct':sc.get('turn_pct'),
                           'etf_pct':sc.get('etf_pct')})
        pl2 = POS_LABEL.get(pos2, '空仓')
        # 胜率/盈亏比
        w2 = 0; l2 = 0; tw2 = 0; tl2 = 0; et2 = None
        for t in trades2:
            if t['type'] in ('buy1','buy2','buy3','short1','short2') and et2 is None:
                et2 = t
            elif t['type'] in ('sell2','cover2') and et2 is not None:
                ep = et2['price']; xp = t['price']
                pnl = (xp-ep)/ep*100 if et2['type'].startswith('buy') else (ep-xp)/ep*100
                if pnl > 0: w2 += 1; tw2 += pnl
                else: l2 += 1; tl2 += abs(pnl)
                et2 = None
        wr2 = round(w2/(w2+l2)*100,1) if (w2+l2) > 0 else 0
        plr2 = round((tw2/w2)/(tl2/l2),2) if w2 > 0 and l2 > 0 else 0
        return {'daily':daily2,'trades':trades2,'equity':eq_curve2,
                'current_pos':pos2,'current_pos_label':pl2,
                'today_short':latest_s,'today_mid':latest_m,'today_date':latest_d or '',
                'stats':{'total_ret':tr2,'sharpe':sh2,'max_dd':mdd2,'beta':bt2,
                         'win_rate':wr2,'profit_loss':plr2,'trades_count':len(trades2)}}

    # 加载沪深300K线跑第二组回测
    results = {
        'zz1000': {
            'daily': daily, 'trades': trades, 'equity': equity_curve,
            'current_pos': pos, 'current_pos_label': pos_label,
            'today_short': latest_s, 'today_mid': latest_m, 'today_date': latest_d or '',
            'stats': {'total_ret':total_ret,'sharpe':sharpe,'max_dd':max_dd,'beta':beta,
                      'win_rate':win_rate,'profit_loss':profit_loss,'trades_count':len(trades)}
        }
    }
    try:
        df_hs = ak.stock_zh_index_daily(symbol='sh000300')
        df_hs['date'] = df_hs['date'].astype(str)
        if start_date: df_hs = df_hs[df_hs['date'] >= start_date]
        if end_date: df_hs = df_hs[df_hs['date'] <= end_date]
        df_hs = df_hs.reset_index(drop=True)
        results['hs300'] = _run_on_index(df_hs, 'hs300')
    except:
        pass
    _resp = {'code': 0, 'data': results, 'leverage': leverage, 'bankrupt': bankrupt}
    if auto_q:
        _resp['thresholds'] = {k: round(float(v), 1) for k, v in p.items()}
        _resp['s_range'] = _s_range
        _resp['m_range'] = _m_range
    return jsonify(_resp)
def _score_indicators(ind, i):
    """将技术指标转为0-100评分(0=强多,100=强空)。返回 short_score, mid_score"""
    if i < 60:
        return 50, 50
    c = ind['close'][i]
    # --- MA排列 (短期) ---
    ma5, ma10, ma20, ma60 = ind['ma5'][i], ind['ma10'][i], ind['ma20'][i], ind['ma60'][i]
    # 多头排列(ma5>ma10>ma20>ma60)=0分, 空头排列=100分
    if ma5 > ma10 > ma20 > ma60:
        ma_score = 10
    elif ma5 > ma10 > ma20:
        ma_score = 30
    elif ma5 < ma10 < ma20 < ma60:
        ma_score = 90
    elif ma5 < ma10 < ma20:
        ma_score = 70
    else:
        ma_score = 50
    # --- BIAS6 (短期) ---
    b = ind['bias6'][i]
    # BIAS越高=涨多了=空(高分), 越低=跌多了=多(低分)
    bias_score = max(0, min(100, 50 - b * 3))  # bias=+16.7 -> score=0, bias=-16.7 -> score=100
    # --- BOLL位置 (中期) ---
    bu, bm, bl = ind['boll_up'][i], ind['boll_mid'][i], ind['boll_low'][i]
    if bu == bl:
        boll_score = 50
    else:
        pos = (c - bl) / (bu - bl)  # 0=下轨, 1=上轨
        boll_score = max(0, min(100, pos * 100))  # 上轨=100=空, 下轨=0=多
    # --- MACD (中期) ---
    dif_v = ind['dif'][i]
    dea_v = ind['dea'][i]
    # DIF>DEA=金叉=多(低分), DIF<DEA=死叉=空(高分)
    if dif_v > dea_v and dif_v > 0:
        macd_score = 20
    elif dif_v > dea_v:
        macd_score = 40
    elif dif_v < dea_v and dif_v < 0:
        macd_score = 80
    else:
        macd_score = 60
    # 短期 = MA + BIAS 等权
    short_score = (ma_score + bias_score) / 2
    # 中期 = BOLL + MACD 等权
    mid_score = (boll_score + macd_score) / 2
    return round(short_score, 1), round(mid_score, 1)

@app.route('/api/backtest')
def backtest():
    """策略回测：沪深300/中证1000"""
    code = request.args.get('code', 'sh000300')
    # 策略参数(可从query传入)
    params = {
        'long_add1_mid': float(request.args.get('long_add1_mid', 60)),
        'long_add1_short': float(request.args.get('long_add1_short', 18)),
        'long_add2_short': float(request.args.get('long_add2_short', 15)),
        'long_add3_short': float(request.args.get('long_add3_short', 12)),
        'long_close1_mid': float(request.args.get('long_close1_mid', 40)),
        'long_close1_short': float(request.args.get('long_close1_short', 82)),
        'long_close2_short': float(request.args.get('long_close2_short', 95)),
        'short_open1_mid': float(request.args.get('short_open1_mid', 80)),
        'short_open1_short': float(request.args.get('short_open1_short', 82)),
        'short_open2_short': float(request.args.get('short_open2_short', 95)),
        'short_close1_mid': float(request.args.get('short_close1_mid', 60)),
        'short_close1_short': float(request.args.get('short_close1_short', 20)),
        'short_close2_short': float(request.args.get('short_close2_short', 14)),
        'leverage': float(request.args.get('leverage', 6)),
        'capital': float(request.args.get('capital', 1000000)),
    }
    try:
        df = ak.stock_zh_index_daily(symbol=code)
        df['date'] = df['date'].astype(str)
        # 取近3年
        cutoff = (datetime.now() - timedelta(days=3*365)).strftime('%Y-%m-%d')
        df = df[df['date'] >= cutoff].reset_index(drop=True)
        ind = _compute_indicators(df)
        n = len(df)
        # 计算每日评分
        scores = []
        for i in range(n):
            s, m = _score_indicators(ind, i)
            scores.append({'date': df['date'][i], 'close': round(float(df['close'][i]),2),
                          'short': s, 'mid': m})
        # 模拟交易
        # 记账模型：cash = 已实现权益，position = 带符号持仓手数，
        # 权益 = cash + position * (price - avg_entry)。
        # 目标敞口始终以「当前权益 × leverage × 目标比例」为上限，每笔信号只把
        # 持仓调整到目标值，绝不在已有仓位上叠加。
        # 旧实现把 position 当持仓市值累加、却只扣 buy_val/3 现金（与 leverage 无关），
        # 且分批建仓时每一档都按当时的全额权益重新乘杠杆，敞口能放大到 10 倍权益；
        # 表现为杠杆 6 时单日权益变动 166%、中证1000 达 543%，甚至权益转负。
        capital = params['capital']
        cash = capital            # 已实现权益（初始资金 + 已落袋盈亏 - 手续费）
        position = 0.0            # 带符号持仓，正=多，负=空
        avg_entry = 0.0           # 当前持仓的加权开仓价
        target = 0.0              # 目标敞口比例（-1 ~ +1，再乘 leverage）
        trades = []
        equity_curve = []
        lev = max(1.0, float(params['leverage']))
        p = params

        def _equity(px):
            return cash + position * (px - avg_entry)

        def _rebalance_to(frac, ttype, label):
            """把持仓调整到 权益*lev*frac 的名义敞口，差额部分成交并计手续费。"""
            nonlocal cash, position, avg_entry
            eq = _equity(price)
            if eq <= 0:
                # 爆仓：清仓并停手，不允许权益继续为负
                if abs(position) > 1e-9:
                    realized = position * (price - avg_entry)
                    cash += realized
                    position = 0.0
                    avg_entry = 0.0
                return False
            desired_qty = eq * lev * frac / price
            delta = desired_qty - position
            if abs(delta) * price < 1e-9:
                return False
            if position != 0 and (position > 0) == (delta > 0):
                # 同向加仓：更新加权成本
                avg_entry = (avg_entry * abs(position) + price * abs(delta)) / (abs(position) + abs(delta))
                position += delta
            elif position != 0 and (position > 0) != (delta > 0) and abs(delta) <= abs(position):
                # 同向减仓：平掉 |delta|，已实现盈亏 = -delta * (price - avg_entry)
                # （delta 与持仓反向，取负号才得到被平掉那部分的方向）
                cash -= delta * (price - avg_entry)
                position += delta
                if abs(position) < 1e-9:
                    position = 0.0
                    avg_entry = 0.0
            else:
                # 反手：先按旧成本全部平掉，再以当前价重建到目标敞口
                cash += position * (price - avg_entry)
                position = desired_qty
                avg_entry = price
            cash -= abs(delta) * price * FEE_RATE
            trades.append({'date': date, 'price': price, 'type': ttype, 'label': label})
            return True

        for i in range(60, n):
            s = scores[i]['short']
            m = scores[i]['mid']
            price = scores[i]['close']
            date = scores[i]['date']
            if price <= 0:
                equity_curve.append({'date': date, 'equity': round(_equity(price), 0)})
                continue
            new_target = target
            act = None
            # 多头买入
            if position >= 0:
                if m < p['long_add1_mid'] and s < p['long_add1_short'] and position == 0:
                    new_target, act = 1.0 / 3, ('buy1', '买1/3')
                elif m < p['long_add1_mid'] and s < p['long_add2_short'] and position > 0:
                    new_target, act = 2.0 / 3, ('buy2', '买1/3')
                elif m < p['long_add1_mid'] and s < p['long_add3_short'] and position > 0:
                    new_target, act = 1.0, ('buy3', '全仓')
                elif m > p['long_close1_mid'] and s > p['long_close1_short'] and position > 0:
                    new_target, act = max(target - 1.0 / 3, 0.0), ('sell1', '平1/3多')
                elif s > p['long_close2_short'] and position > 0:
                    new_target, act = 0.0, ('sell2', '全平多')
            # 空头
            if position <= 0 and act is None:
                if m > p['short_open1_mid'] and s > p['short_open1_short'] and position == 0:
                    new_target, act = -1.0 / 3, ('short1', '空1/3')
                elif s > p['short_open2_short'] and position < 0:
                    new_target, act = -1.0 / 2, ('short2', '空1/2')
                elif m < p['short_close1_mid'] and s < p['short_close1_short'] and position < 0:
                    new_target, act = min(target + 1.0 / 2, 0.0), ('cover1', '平1/2空')
                elif s < p['short_close2_short'] and position < 0:
                    new_target, act = 0.0, ('cover2', '全平空')
            if act is not None:
                target = new_target
                _rebalance_to(target, act[0], act[1])
            equity_curve.append({'date': date, 'equity': round(_equity(price), 0)})
        # 统计
        if len(equity_curve) > 1:
            final_eq = equity_curve[-1]['equity']
            total_ret = (final_eq - params['capital']) / params['capital'] * 100
            # 日收益
            eqs = [e['equity'] for e in equity_curve]
            daily_ret = [(eqs[i]-eqs[i-1])/eqs[i-1] for i in range(1,len(eqs))]
            avg_ret = sum(daily_ret)/len(daily_ret)
            std_ret = (sum((r-avg_ret)**2 for r in daily_ret)/len(daily_ret))**0.5
            sharpe = avg_ret/std_ret*(252**0.5) if std_ret>0 else 0
            # 最大回撤
            peak = eqs[0]; max_dd = 0
            for e in eqs:
                if e > peak: peak = e
                dd = (peak-e)/peak
                if dd > max_dd: max_dd = dd
            # 胜率/盈亏比：按手数 FIFO 配对，部分平仓也计入
            # （旧实现把 sell1/cover1 直接丢弃，win_rate 会用「平仓数/事件数」冒充胜率）
            wins2, losses2, tw2, tl2 = _pair_trades(trades)
            win_rate, _pl = _win_ratio(wins2, losses2, tw2, tl2)
        else:
            total_ret = sharpe = max_dd = win_rate = 0
        return jsonify({
            'code': 0,
            'data': {
                'kline': [[s['date'], s['close']] for s in scores],
                'trades': trades,
                'equity': equity_curve,
                'stats': {
                    'total_ret': round(total_ret, 1),
                    'sharpe': round(sharpe, 2),
                    'max_dd': round(max_dd*100, 1),
                    'win_rate': round(win_rate, 1),
                    'trades_count': len(trades),
                },
                'params': params
            }
        })
    except Exception as e:
        import traceback
        return jsonify({'code': 1, 'msg': str(e), 'tb': traceback.format_exc()[:500]})

# ================== 用户配置同步 + 自动记录 ==================
CONFIG_FILE = os.path.join(BASE_DIR, 'bt_config.json')

# 顶层键白名单：以前任意键（例如 {"_audit_probe":{"x":1}}）都会写进配置并
# 原样回显到 saved，配置文件会被慢慢污染。只接受这五个业务字段。
_BT_CONFIG_KEYS = ('indicators', 'thresholds', 'user_thresholds', 'user_weights', 'leverage')

def _bt_config_post(body):
    """bt_config 的写入主体。调用方必须持有 _save_lock_for(CONFIG_FILE)。"""
    if not isinstance(body, dict):
        return jsonify({'code': 1, 'msg': '请求体必须是对象'})
    cur = {}
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                cur = json.load(f) or {}
        except Exception:
            cur = {}
    if not isinstance(cur, dict):
        cur = {}

    ignored = []
    for k, v in body.items():
        if k == 'updated_at':
            continue
        if k not in _BT_CONFIG_KEYS:
            ignored.append(k)
            continue
        if v is None:
            # 显式 null = 删除该键（合并式写入本身删不掉键，旧配置无法清空）
            cur.pop(k, None)
            continue
        if k in ('user_weights', 'user_thresholds', 'thresholds'):
            if isinstance(v, dict) and not v:
                # 提交空对象 = 用户点了「重置」，必须真的清空。
                # 旧代码 dict.update({}) 是 no-op，重置永远不生效。
                cur[k] = {}
            elif isinstance(v, dict) and isinstance(cur.get(k), dict):
                # 按键合并，避免一次只改一个指标就把其他指标抹掉
                merged = dict(cur[k])
                merged.update(v)
                cur[k] = merged
            else:
                cur[k] = v
        else:
            cur[k] = v

    # 写入前跑一遍自愈：浏览器用旧 localStorage 整份 POST 时，会把部分指标权重
    # 覆盖成 0（现场事故：macro 类的 wM 全变 0 → 回测中期分只剩成交额一项）。
    cur = _migrate_config(cur)
    ts = body.get('updated_at') or datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cur['updated_at'] = ts
    try:
        safe_json_save(CONFIG_FILE, cur)
    except Exception as e:
        return jsonify({'code': 1, 'msg': '保存失败：%s' % e})
    sync_file('bt_config.json')
    resp = {'code': 0, 'updated_at': ts,
            'saved': sorted([k for k in cur.keys() if k != 'updated_at'])}
    if ignored:
        resp['ignored'] = sorted(set(ignored))
    return jsonify(resp)

# 看板 20 项规范指标（与 dashboard.html 的 IND 默认值一一对应）。
# 后端只用它补齐缺失项/权重，不覆盖用户已保存的阈值与权重。
# 背景：用户配置是「前端保存时 cur 非空的指标」快照，早期版本没有
# turn_pct / margin_pct / etf_share_pct，导致回测的「中期趋势」没有任何
# 中期权重 → _composite_score 恒回 50（用户看到的「回测中期恒 50」）。
_CANON_INDICATORS = [
    {'id': 'lpr', 'g': 'cn', 'name': '1年期LPR', 'min': 2.5, 'neutral': 3.1, 'max': 3.6, 'dir': 'high_bear', 'bt': False, 'wS': 0, 'wM': 6, 'wL': 5},
    {'id': 'cny10y', 'g': 'cn', 'name': '中国10Y国债收益率(%)', 'min': 1.2, 'neutral': 2.2, 'max': 3.0, 'dir': 'high_bear', 'bt': False, 'wS': 1, 'wM': 6, 'wL': 7},
    {'id': 'cncpi', 'g': 'cn', 'name': '中国CPI同比(%)', 'min': -1.0, 'neutral': 2.0, 'max': 4.0, 'dir': 'high_bear', 'bt': False, 'wS': 0, 'wM': 5, 'wL': 5},
    {'id': 'pmi', 'g': 'cn', 'name': '官方制造业PMI', 'min': 48, 'neutral': 50, 'max': 52, 'dir': 'high_bull', 'bt': False, 'wS': 2, 'wM': 6, 'wL': 7},
    {'id': 'usr', 'g': 'us', 'name': '美国联邦基金利率(%)', 'min': 0.25, 'neutral': 2.5, 'max': 5.5, 'dir': 'high_bear', 'bt': False, 'wS': 1, 'wM': 7, 'wL': 5},
    {'id': 'us10y', 'g': 'us', 'name': '美国10Y收益率(%)', 'min': 1.5, 'neutral': 3.5, 'max': 5.5, 'dir': 'high_bear', 'bt': False, 'wS': 2, 'wM': 6, 'wL': 5},
    {'id': 'uscpi', 'g': 'us', 'name': '美国CPI同比(%)', 'min': 1.0, 'neutral': 2.0, 'max': 4.5, 'dir': 'high_bear', 'bt': False, 'wS': 0, 'wM': 6, 'wL': 5},
    {'id': 'uspce', 'g': 'us', 'name': '美国核心PCE同比(%)', 'min': 1.5, 'neutral': 2.0, 'max': 4.0, 'dir': 'high_bear', 'bt': False, 'wS': 0, 'wM': 5, 'wL': 4},
    {'id': 'turn', 'g': 'sent', 'name': '两市成交额(万亿)', 'min': 0.5, 'neutral': 0.9, 'max': 1.8, 'dir': 'high_bull', 'bt': True, 'wS': 0, 'wM': 0, 'wL': 0},
    {'id': 'margin', 'g': 'sent', 'name': '沪深融资余额总额(亿)', 'min': 15000, 'neutral': 22000, 'max': 30000, 'dir': 'high_bear', 'bt': True, 'wS': 0, 'wM': 0, 'wL': 0},
    {'id': 'fg', 'g': 'sent', 'name': '市场贪恐指数', 'min': 0, 'neutral': 50, 'max': 100, 'dir': 'high_bear', 'bt': True, 'wS': 8, 'wM': 1, 'wL': 0},
    {'id': 'breadth', 'g': 'sent', 'name': '涨跌家数比', 'min': 0.1, 'neutral': 1, 'max': 10, 'dir': 'high_bull', 'bt': False, 'wS': 0, 'wM': 0, 'wL': 0},
    {'id': 'turn_pct', 'g': 'sent', 'name': '成交额历史百分位(%)', 'min': 0, 'neutral': 50, 'max': 100, 'dir': 'high_bear', 'bt': True, 'wS': 3, 'wM': 3, 'wL': 2},
    {'id': 'margin_pct', 'g': 'sent', 'name': '融资余额历史百分位(%)', 'min': 0, 'neutral': 50, 'max': 100, 'dir': 'high_bear', 'bt': True, 'wS': 3, 'wM': 3, 'wL': 2},
    {'id': 'etf_share_pct', 'g': 'sent', 'name': '300ETF份额历史百分位(%)', 'min': 0, 'neutral': 50, 'max': 100, 'dir': 'high_bull', 'bt': True, 'wS': 2, 'wM': 3, 'wL': 3},
    {'id': 'erp', 'g': 'sent', 'name': '股债性价比ERP(%)', 'min': 2.0, 'neutral': 3.5, 'max': 6.0, 'dir': 'high_bull', 'bt': True, 'wS': 2, 'wM': 5, 'wL': 9},
    {'id': 'tech_ma', 'g': 'tech', 'name': '中证1000·MA排列', 'min': 0, 'neutral': 50, 'max': 100, 'dir': 'direct', 'bt': True, 'wS': 5, 'wM': 4, 'wL': 2},
    {'id': 'tech_boll', 'g': 'tech', 'name': '中证1000·BOLL位置', 'min': 0, 'neutral': 50, 'max': 100, 'dir': 'direct', 'bt': True, 'wS': 6, 'wM': 3, 'wL': 1},
    {'id': 'tech_macd', 'g': 'tech', 'name': '中证1000·MACD分位', 'min': 0, 'neutral': 50, 'max': 100, 'dir': 'direct', 'bt': True, 'wS': 4, 'wM': 5, 'wL': 3},
    {'id': 'tech_bias', 'g': 'tech', 'name': '中证1000·BIAS6分位', 'min': 0, 'neutral': 50, 'max': 100, 'dir': 'direct', 'bt': True, 'wS': 3, 'wM': 2, 'wL': 1},
]


def _canon_weight_totals():
    t = {'S': 0, 'M': 0, 'L': 0}
    for c in _CANON_INDICATORS:
        t['S'] += int(c.get('wS') or 0)
        t['M'] += int(c.get('wM') or 0)
        t['L'] += int(c.get('wL') or 0)
    return t


def _migrate_config(cfg, persist=False):
    """配置迁移/自愈（读路径统一经过这里）。

    1. 老配置 `margin` 阈值 12000/16000/20000 → 15000/22000/30000
       （沪+深合计早已常态 2.2 万亿，超过旧 max 后该指标反转恒为 0 分）。
    2. 补齐看板规范 20 项指标（尤其 turn_pct / margin_pct / etf_share_pct）。
    3. 权重自愈：浏览器 localStorage 里的旧权重会整份 POST 回来，把部分指标的
       权重写成 0（现场事故：macro 类 8 项的 wM 全变 0，只剩 turn/etf 有中期权重，
       回测「中期 M」只剩成交额一项 → 2023-11-20 算出 98.3 的假高点）。
       旧版只在「总 M 全为 0」时恢复，部分清零会漏过；现在按「整组权重是否
       低于规范一半」+「单个指标是否被清零」两种规则恢复，且支持 persist 落盘。
    """
    changed = False
    try:
        inds = cfg.get('indicators')
        if not isinstance(inds, list):
            inds = []
        have = {i.get('id') for i in inds if isinstance(i, dict)}
        added = []
        for canon in _CANON_INDICATORS:
            if canon['id'] not in have:
                entry = dict(canon)
                entry['cur'] = None
                inds.append(entry)
                added.append(canon['id'])
        if added:
            print('[config] 迁移补齐指标: %s' % ','.join(added))
            changed = True
        cfg['indicators'] = inds

        ct = _canon_weight_totals()
        pairs = (('S', 'wS'), ('M', 'wM'), ('L', 'wL'))
        for ind in inds:
            if not isinstance(ind, dict):
                continue
            if ind.get('id') == 'margin' and (ind.get('min'), ind.get('max')) == (12000, 20000):
                ind['min'], ind['neutral'], ind['max'] = 15000, 22000, 30000
                changed = True
            canon = next((c for c in _CANON_INDICATORS if c['id'] == ind.get('id')), None)
            if not canon:
                continue
            for k, ck in pairs:
                if ind.get(ck) is None:
                    ind[ck] = canon[ck]
                    changed = True
            if not ind.get('dir'):
                ind['dir'] = canon['dir']
                changed = True
            if all(int(ind.get(ck) or 0) == 0 for _, ck in pairs) \
                    and any(int(canon[ck] or 0) > 0 for _, ck in pairs):
                for _, ck in pairs:
                    ind[ck] = canon[ck]
                print('[config] 迁移修复 indicators: %s 权重被清零，已恢复规范默认' % ind.get('id'))
                changed = True

        tot = {'S': 0, 'M': 0, 'L': 0}
        for ind in inds:
            if isinstance(ind, dict):
                for k, ck in pairs:
                    tot[k] += int(ind.get(ck) or 0)
        for k, ck in pairs:
            if ct[k] > 0 and tot[k] == 0:
                for ind in inds:
                    if not isinstance(ind, dict):
                        continue
                    canon = next((c for c in _CANON_INDICATORS if c['id'] == ind.get('id')), None)
                    if canon and int(canon[ck] or 0) > 0 and int(ind.get(ck) or 0) == 0:
                        ind[ck] = canon[ck]
                        changed = True
                print('[config] 迁移修复 indicators：%s 组权重合计异常偏低（%d/%d），已按规范恢复'
                      % (k, tot[k], ct[k]))

        uw = cfg.get('user_weights')
        if isinstance(uw, dict):
            utot = {'S': 0, 'M': 0, 'L': 0}
            for v in uw.values():
                if isinstance(v, dict):
                    for k in utot:
                        if isinstance(v.get(k), (int, float)):
                            utot[k] += v[k]
            for k, ck in pairs:
                if ct[k] > 0 and utot[k] == 0:
                    for canon in _CANON_INDICATORS:
                        w = uw.get(canon['id'])
                        if not isinstance(w, dict):
                            uw[canon['id']] = {'S': canon['wS'], 'M': canon['wM'], 'L': canon['wL']}
                            changed = True
                        elif int(canon[ck] or 0) > 0 and int(w.get(k) or 0) == 0:
                            w[k] = canon[ck]
                            changed = True
                    print('[config] 迁移修复 user_weights：%s 权重合计异常偏低（%d/%d），已按规范恢复'
                          % (k, utot[k], ct[k]))
            cfg['user_weights'] = uw
    except Exception:
        traceback.print_exc()
    if changed and persist:
        try:
            safe_json_save(CONFIG_FILE, cfg)
            print('[config] 迁移结果已落盘')
        except Exception:
            traceback.print_exc()
    return cfg


@app.route('/api/bt_config', methods=['GET', 'POST'])
def bt_config():
    """保存/读取用户配置：指标阈值、权重、交易策略参数。

    POST 采用「按字段合并」而不是整份覆盖：历史上页面里有两个地方会 POST 这个接口
    （一处提交全量配置、另一处只提交 indicators+thresholds 用于自动记录），
    整份覆盖会把 user_weights / user_thresholds 直接抹掉，表现就是「权重/阈值改了没保存」。
    现在只替换本次提交的字段，其余字段原样保留。
    """
    if request.method == 'POST':
        body = request.json or {}
        # 整段「读-合并-写」必须持锁：只锁写的话，两个并发请求会读到同一份 cur，
        # 各自合并自己的字段，后写的把先写的整段覆盖掉（审计实测 6 个并发 POST 丢 5 份）。
        with _save_lock_for(CONFIG_FILE):
            return _bt_config_post(body)
    else:
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    return jsonify({'code': 0, 'config': _migrate_config(json.load(f), persist=True)})
            except:
                pass
        return jsonify({'code': 0, 'config': {}})


def _auto_record_worker():
    """后台线程：每日23:00后自动记录当日指标快照，开机后自动补跑最近7天缺失交易日"""
    import threading
    last_record_date = None

    def _do_record(target_date):
        """记录指定日期的快照（只记录真实当日，绝不伪造历史）。

        审计教训：旧实现把「当前 config 里今天的 cur」原样写给 target_date，
        启动补跑最近 7 天时会给过去日期写入今天的数值（伪造历史），
        而且 snapshot 只有 14/20 项、缺 turn/margin/fg 等。现在：
        - target_date 不是今天 → 直接跳过（历史缺口宁可空着，也不造假数据）；
        - 指标不全（有效项 < 12）→ 本次不记录，等前端 POST /api/bt_record 补全。
        """
        today = datetime.now().strftime('%Y-%m-%d')
        if target_date != today:
            print(f'[auto-record] 跳过 {target_date}：不伪造历史快照（只记录当日）')
            return
        cfg = {}
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    cfg = json.load(f)
            except:
                pass
        indicators = [i for i in (cfg.get('indicators') or []) if i.get('cur') is not None]
        if len(indicators) < 12:
            print(f'[auto-record] {target_date} 指标不完整（{len(indicators)} 项有值），本次不记录')
            return
        s_val = _composite_score(indicators, 'S')
        m_val = _composite_score(indicators, 'M')
        data = _load_bt()
        rec = next((r for r in data['records'] if r['date']==target_date), None)
        if not rec:
            rec = {'date': target_date}
            data['records'].append(rec)
        rec['snapshot'] = indicators
        rec['s_score'] = s_val
        rec['m_score'] = m_val
        rec['updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data['records'].sort(key=lambda x: x['date'])
        _save_bt(data)
        print(f'[auto-record] {target_date} S={s_val} M={m_val} 指标={len(indicators)} 项')

    # 说明：以前这里会「补跑最近 7 天缺失交易日」，但补跑写入的是「今天的指标值」，
    # 等于给历史日期伪造快照。现在历史缺口由回测端从原始数据重算，这里只负责当日记录。
    try:
        _n = len(_load_bt().get('records', []))
        print('[auto-record] 启动：仅记录当日快照（不伪造历史），已有 %d 条记录' % _n)
    except Exception as e:
        print(f'[auto-record] 初始化检查失败: {e}')

    while True:
        try:
            now = datetime.now()
            # 交易日23:00后都可以触发（支持开机后补跑当天）
            if now.weekday() < 5 and now.hour >= 23:
                today = now.strftime('%Y-%m-%d')
                if today != last_record_date:
                    data = _load_bt()
                    if not any(r['date']==today for r in data['records']):
                        _do_record(today)
                    last_record_date = today
        except Exception as e:
            print(f'[auto-record] error: {e}')
        time.sleep(60)  # 每分钟检查一次


# 启动后台记录线程
import threading
threading.Thread(target=_auto_record_worker, daemon=True).start()


# ===== 510300 份额数据源：上交所官方 ETF 规模（权威、日频、无限流） =====
def _sse_etf_share_on(date_str):
    """查上交所某日 510300 份额。

    date_str: 'YYYY-MM-DD'
    返回 (business_date, share_yi) 或 None（该日无数据/非交易日）。
    数据来源：上海证券交易所 ETF 产品-基金规模，官方日终结算值。
    """
    ymd = date_str.replace('-', '')[:8]
    try:
        df = ak.fund_etf_scale_sse(date=ymd)
    except Exception:
        return None
    if df is None or len(df) == 0 or '基金代码' not in df.columns:
        return None
    hit = df[df['基金代码'].astype(str) == '510300']
    if len(hit) == 0:
        return None
    r = hit.iloc[0]
    try:
        share = float(r['基金份额']) / 1e8   # 份 -> 亿份
    except Exception:
        return None
    if share <= 0:
        return None
    d = str(r.get('统计日期') or date_str)[:10]
    return d, round(share, 4)

def _latest_sse_etf_share(max_back=12):
    """从今天往前找最近一个有数据的交易日。返回 (date, share_yi)，全失败抛异常。"""
    today = datetime.now()
    errs = []
    for i in range(max_back):
        d = (today - timedelta(days=i)).strftime('%Y-%m-%d')
        if not is_trading_day(today - timedelta(days=i)):
            continue
        try:
            got = _sse_etf_share_on(d)
        except Exception as e:
            errs.append('%s:%s' % (d, e))
            continue
        if got:
            return got
    raise RuntimeError('上交所近%d天均无510300份额数据 %s' % (max_back, '; '.join(errs[:2])))

def update_etf_share():
    """更新 510300 份额与百分位。

    主数据源：上交所官方 ETF 规模（ak.fund_etf_scale_sse），日终结算值，可回溯。
    备用数据源：东财 ETF 实时列表（限流频繁，仅在官方源失败时尝试）。
    上交所数据为官方结算值，不再做"连续两次一致才保存"的等待，当天即入库。
    """
    global _etf_fail_streak, _etf_next_retry
    if time.time() < _etf_next_retry:
        return None

    data_date, latest_share, source = None, None, ''

    # ---------- 主源：上交所官方 ----------
    try:
        data_date, latest_share = _latest_sse_etf_share()
        source = '上交所官方ETF规模'
    except Exception as e:
        print('上交所份额源失败，尝试东财备用源：%s' % str(e)[:160])

    # ---------- 备用：东财 ----------
    if data_date is None:
        try:
            df = ak.fund_etf_spot_em()
            if df is not None and len(df) and '代码' in df.columns:
                hit = df[df['代码'].astype(str) == '510300']
                if len(hit):
                    row = hit.iloc[0]
                    dd = pd.to_datetime(row['数据日期'], errors='coerce')
                    if not pd.isna(dd):
                        data_date = dd.strftime('%Y-%m-%d')
                        latest_share = round(float(row['最新份额']) / 1e8, 4)
                        source = '东财ETF实时列表'
        except Exception as e:
            _etf_fail_streak += 1
            _etf_next_retry = time.time() + (10 * 60 if _etf_fail_streak < 3 else 30 * 60)
            print('ETF份额全部数据源失败（保留缓存，%d分钟后重试）：%s'
                  % (int((_etf_next_retry - time.time()) // 60), str(e)[:150]))
            return None

    if data_date is None or latest_share is None:
        print('ETF份额未取到有效数据，保留缓存')
        return None

    # ---------- 落库 ----------
    hist_path = os.path.join(BASE_DIR, 'etf_share_history.json')
    current_path = os.path.join(BASE_DIR, 'etf_share.json')
    old_current = None
    if os.path.exists(current_path):
        try:
            with open(current_path, 'r', encoding='utf-8') as f:
                old_current = json.load(f)
        except Exception:
            old_current = None

    if old_current and data_date < str(old_current.get('date', '')):
        print('ETF数据日期%s早于缓存%s，保留缓存不覆盖' % (data_date, old_current.get('date', '')))
        return old_current
    if old_current and data_date == str(old_current.get('date')) \
            and abs(float(old_current.get('value', 0) or 0) - latest_share) < 1e-6:
        # 份额值没变，但历史序列可能刚被深回填扩充（样本 145 → 700+），
        # 这里用最新历史重算百分位/样本数，否则卡片会一直停在旧的计算结果上。
        try:
            hist = []
            if os.path.exists(hist_path):
                with open(hist_path, 'r', encoding='utf-8') as f:
                    hist = json.load(f)
            cutoff_3y = (datetime.now() - timedelta(days=3 * 365)).strftime('%Y-%m-%d')
            window_vals = [item[1] for item in hist if item[0] >= cutoff_3y]
            if window_vals:
                percentile = _percentile_below(window_vals, latest_share)
                if (len(window_vals) != int(old_current.get('sample_n') or 0)
                        or percentile != old_current.get('percentile')):
                    old_current.update({
                        'percentile': percentile,
                        'sample_n': len(window_vals),
                        'fetch_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
                    safe_json_save(current_path, old_current)
                    print('ETF份额百分位重算：样本%d → %d%%，分位%s%%'
                          % (len(window_vals), len(window_vals), percentile))
        except Exception:
            traceback.print_exc()
        return old_current

    if os.path.exists(hist_path):
        try:
            with open(hist_path, 'r', encoding='utf-8') as f:
                hist = json.load(f)
        except Exception:
            hist = []
    else:
        hist = []
    hist = [item for item in hist if item[0] != data_date]
    hist.append([data_date, latest_share])
    hist.sort(key=lambda x: x[0])

    # 百分位：近三年窗口，低于当前份额的样本占比
    cutoff_3y = (datetime.now() - timedelta(days=3 * 365)).strftime('%Y-%m-%d')
    window_vals = [item[1] for item in hist if item[0] >= cutoff_3y]
    if not window_vals:
        window_vals = [item[1] for item in hist]
    percentile = min(100, max(0, round(
        sum(1 for v in window_vals if v < latest_share) / max(len(window_vals), 1) * 100)))

    fetch_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_data = {
        'value': latest_share,
        'percentile': percentile,
        'date': data_date,
        'fetch_time': fetch_time,
        'sample_n': len(window_vals),
        'source': source
    }
    safe_json_save(current_path, current_data)
    safe_json_save(hist_path, hist)
    _etf_fail_streak = 0
    _etf_next_retry = 0.0
    print('ETF份额更新完成[%s]：%s %s亿份，百分位%s%%（样本%d）'
          % (source, data_date, latest_share, percentile, len(window_vals)))
    return current_data

# ===== 韭圈儿恐贪指数（官方网页同源接口，无需登录/无需浏览器渲染） =====
_JQS_FG_URLS = [
    'https://ai2.jiucaishuo.com/api/fear-greed',
]
_JQS_HEADERS = {
    'Accept': 'application/json, text/plain, */*',
    'Origin': 'https://www.jiucaishuo.com',
    'Referer': 'https://www.jiucaishuo.com/',
}

def fetch_jqs_fear_greed():
    """拉取韭圈儿恐贪指数。

    返回 {'value':float, 'date':'YYYY-MM-DD', 'desc':str, 'history':[[date,value],...]}
    失败抛异常，由调用方保留旧缓存。
    """
    last = None
    for url in _JQS_FG_URLS:
        try:
            js = http_get_json(url, headers=_JQS_HEADERS, timeout=15, retry=2)
            if not isinstance(js, dict):
                raise ValueError('返回非 JSON 对象')
            val = js.get('value')
            if val is None:
                raise ValueError('返回缺少 value 字段')
            val = float(val)
            if not (0 <= val <= 100):
                raise ValueError('恐贪值超出 0-100: %s' % val)
            date = str(js.get('date') or '').strip()[:10]
            if len(date) != 10:
                date = datetime.now().strftime('%Y-%m-%d')
            hist = []
            for it in (js.get('history') or []):
                try:
                    d = str(it.get('date'))[:10]
                    v = float(it.get('value'))
                    if len(d) == 10 and 0 <= v <= 100:
                        hist.append([d, round(v, 2)])
                except Exception:
                    continue
            hist.sort(key=lambda x: x[0])
            return {'value': round(val, 2), 'date': date,
                    'desc': str(js.get('desc') or ''), 'history': hist}
        except Exception as e:
            last = e
            continue
    raise last if last else RuntimeError('韭圈儿恐贪接口不可用')

def update_fear_greed():
    """自动更新韭圈儿恐贪指数；每次成功都同步刷新历史序列。数据源不可用时保留旧值。"""
    fg_path = os.path.join(BASE_DIR, 'fg_cache.json')
    hist_path = os.path.join(BASE_DIR, 'fg_history.json')
    old_data = {'value': 50, 'date': '2020-01-01'}
    if os.path.exists(fg_path):
        try:
            with open(fg_path, 'r', encoding='utf-8') as f:
                old_data = json.load(f)
        except Exception:
            pass
    try:
        got = fetch_jqs_fear_greed()
    except Exception as e:
        print('恐贪拉取失败（保留旧值 %s）：%s: %s'
              % (old_data.get('value'), type(e).__name__, str(e)[:150]))
        return old_data

    value, data_date = got['value'], got['date']
    # 历史序列：接口一次返回全量，直接覆盖为最新（这是"历史自动补全"的来源）
    if got['history']:
        safe_json_save(hist_path, got['history'])
    # 手动值保护：用户在页面上手动改过的同一天数据，不被自动抓取值覆盖
    if old_data.get('manual') and str(old_data.get('date', '')) >= data_date:
        print('恐贪保留手动值 %s@%s（自动值 %s@%s 不覆盖）'
              % (old_data.get('value'), old_data.get('date'), value, data_date))
        return old_data
    # 值或日期没变化就不重写缓存时间，避免"最后一更新"频繁跳动
    if data_date == old_data.get('date') and \
            abs(float(old_data.get('value', -1) or -1) - value) < 0.005:
        return old_data
    fetch_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    new_data = {
        'value': value,
        'date': data_date,
        'fetch_time': fetch_time,
        'desc': got.get('desc', ''),
        'history_len': len(got['history'])
    }
    safe_json_save(fg_path, new_data)
    print('恐贪指数更新：%s %s（%s），获取于 %s，历史 %d 条'
          % (data_date, value, new_data['desc'], fetch_time, len(got['history'])))
    return new_data

def run_close_save():
    """收盘后保存：常规数据当日仅存一次，恐贪每次检查更新，适配数据更新晚"""
    global _etf_save_date
    today = datetime.now().strftime('%Y-%m-%d')
    try:
        # 1. ETF等常规收盘数据，当日仅保存一次
        if _etf_save_date != today:
            print(f'[{datetime.now().strftime("%H:%M:%S")}] 开始保存常规收盘数据...')
            update_etf_share()
            _etf_save_date = today
        # 2. 恐贪指数每次定时都检查，直到拿到当日最新数据，不做一次完成限制
        update_fear_greed()
    except Exception as e:
        traceback.print_exc()
        print(f'收盘保存失败：{e}')

_ETF_DEEP_MARK = os.path.join(BASE_DIR, 'etf_hist_deep.json')


def _etf_deep_done():
    """深回填是否已完成（一次把 3 年日频补齐，之后只做日常 60 天增量）。"""
    try:
        with open(_ETF_DEEP_MARK, 'r', encoding='utf-8') as f:
            d = json.load(f)
        return bool(d.get('done')) and int(d.get('rows') or 0) >= 600
    except Exception:
        return False


def backfill_etf_history(max_days=60, workers=5):
    """用上交所官方接口补齐最近若干交易日缺失的 510300 份额历史。

    上交所接口按"日期 -> 当日全市场 ETF 份额"返回，只能逐日回溯（每条约 2~3s），
    因此这里并发补齐：默认每次启动补最近 60 个交易日中缺失的部分；
    首次（_ETF_DEEP_MARK 未标记）会深回填 1100 个自然日≈750 个交易日，
    让 etf_share_pct 的 750 日滚动窗口在 3 年区间内是满样本。
    返回 (full_list, added_count)。
    """
    hist_path = os.path.join(BASE_DIR, 'etf_share_history.json')
    dmap = {}
    if os.path.exists(hist_path):
        try:
            with open(hist_path, 'r', encoding='utf-8') as f:
                for item in json.load(f):
                    try:
                        dmap[str(item[0])] = float(item[1])
                    except Exception:
                        continue
        except Exception:
            dmap = {}
    before = len(dmap)
    today = datetime.now()
    targets = []
    for i in range(max_days):
        dt = today - timedelta(days=i)
        if not is_trading_day(dt):
            continue
        d = dt.strftime('%Y-%m-%d')
        if d not in dmap:
            targets.append(d)
    if not targets:
        full = sorted([[d, v] for d, v in dmap.items()], key=lambda x: x[0])
        print('ETF历史补全：近%d天无缺口，历史共%d条' % (max_days, len(full)))
        return full, 0

    def _one(d):
        try:
            got = _sse_etf_share_on(d)
        except Exception:
            return None
        time.sleep(0.05)
        return got

    filled = 0
    try:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
            for got in ex.map(_one, targets):
                if got:
                    gd, gv = got
                    if gd not in dmap:
                        dmap[gd] = gv
                        filled += 1
    except Exception:
        traceback.print_exc()
    full = sorted([[d, v] for d, v in dmap.items()], key=lambda x: x[0])
    if filled:
        safe_json_save(hist_path, full)
    if max_days >= 300 and len(dmap) >= 600 and not _etf_deep_done():
        try:
            safe_json_save(_ETF_DEEP_MARK, {'done': True, 'rows': len(dmap),
                                            'updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
        except Exception:
            traceback.print_exc()
    print('ETF历史补全：探测%d个交易日，新增%d条，历史共%d条（原%d条）'
          % (len(targets), filled, len(full), before))
    return full, filled

def backfill_90_days():
    """检查并报告数据卡片的历史覆盖情况。

    能自动补的（有官方历史序列）：ETF份额、恐贪指数 —— 由各自 backfill 完成；
    S/M/L 评分快照 —— 由 _bt_backfill_if_needed()/build_snapshot_history() 用历史接口回算。
    这里只做体检报告，绝不写入 s_score=50 的空占位（会污染回测）。
    """
    try:
        print('=' * 46)
        print('[历史数据体检] %s' % datetime.now().strftime('%Y-%m-%d %H:%M'))
        # 1. 交易日历
        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)
        cal = ak.tool_trade_date_hist_sina()
        cal['trade_date'] = pd.to_datetime(cal['trade_date'], errors='coerce')
        cal = cal.dropna(subset=['trade_date'])
        trade_dates = cal[
            (cal['trade_date'].dt.date >= start_date.date()) &
            (cal['trade_date'].dt.date <= end_date.date())
        ]['trade_date'].dt.strftime('%Y-%m-%d').tolist()

        # 2. 快照覆盖
        snap_path = os.path.join(BASE_DIR, 'bt_snapshots.json')
        records = []
        if os.path.exists(snap_path):
            try:
                with open(snap_path, 'r', encoding='utf-8') as f:
                    records = json.load(f).get('records', [])
            except Exception:
                records = []
        real = [r for r in records if r.get('snapshot')]
        have = {r['date'] for r in real}
        missing = [d for d in trade_dates if d not in have]
        covered = len([d for d in trade_dates if d in have])
        print('  · 评分快照：近90天已覆盖 %d / %d 个交易日，缺 %d 个'
              % (covered, len(trade_dates), len(missing)))
        if missing:
            print('    缺失区间：%s ~ %s（历史接口可回算：启动后自动调用 /api/bt_backfill；'
                  '也可手动 GET /api/bt_backfill?days=750 立即回填，'
                  '数据源=中证1000日K/成交额/融资余额/ETF份额/恐贪）'
                  % (missing[0], missing[-1]))

        # 3. 各历史序列覆盖
        def _span(path, key=None):
            if not os.path.exists(path):
                return '缺失'
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    d = json.load(f)
                arr = d if isinstance(d, list) else d.get(key or '', [])
                if not arr:
                    return '空'
                return '%d条 %s~%s' % (len(arr), str(arr[0][0])[:10], str(arr[-1][0])[:10])
            except Exception:
                return '读取失败'

        print('  · ETF份额历史：%s' % _span(os.path.join(BASE_DIR, 'etf_share_history.json')))
        print('  · 恐贪历史：%s' % _span(os.path.join(BASE_DIR, 'fg_history.json')))
        try:
            _tu = _turn_rows_load()
            _tu_txt = ('%d条 %s~%s' % (len(_tu), min(_tu), max(_tu))) if _tu \
                else '缺失（首次打开页面时后台自动回填近3年）'
        except Exception:
            _tu_txt = '读取失败'
        print('  · 成交额历史（沪市+深市，亿元）：%s' % _tu_txt)
        print('=' * 46)
    except Exception as e:
        traceback.print_exc()
        print('历史体检失败：%s' % e)

# ==================== 市场魔方：全球经济数据 / 全球产业数据 ====================
# 数据源：行情魔方（原市场魔方）https://www.bluecatsite.com
# 接口返回体被加密：base64(iv[16] + ciphertext)，密钥流 = HMAC-SHA256(key, iv || uint32_be(block))
_MF_KEY = b'wMNLNGRdGDHOr0ja99YbYI1fnrmkGWO1vqDzIPBQ1diO'
_MF_BASE = 'https://www.bluecatsite.com/api/v3/star'
_MF_SECTOR_NAMES = {
    'ai-compute': ('🧠', 'AI算力'), 'cpo': ('💡', 'CPO'),
    'semiconductor': ('🔬', '半导体'), 'storage': ('💾', '存储'),
    'data-center': ('🗄️', '数据中心'), 'cloud-computing': ('☁️', '云计算'),
    'commercial-space': ('🚀', '商业航天'), 'satellite': ('🛰️', '卫星'),
    'robotics': ('🤖', '机器人'), 'autonomous-driving': ('🚗', '自动驾驶'),
    'nuclear-power': ('⚛️', '核电'), 'power-grid': ('⚡', '电网'),
    'defense': ('🛡️', '军工'), 'solar': ('☀️', '光伏'),
    'lithium-battery': ('🔌', '锂电池'), 'oil': ('🛢️', '油气'),
    'copper-metals': ('🟠', '铜/有色'), 'gold-miners': ('🥇', '黄金矿业'),
    'bank-finance': ('🏦', '银行金融'), 'biotech': ('💊', '生物医药'),
    'consumer': ('🛒', '消费'), 'rare-earth': ('🧲', '稀土'),
    'telecom-equipment': ('📶', '通信设备'), 'quantum-computing': ('🌀', '量子计算'),
    'blockchain': ('⛓️', '区块链'), 'ai-application': ('✨', 'AI应用'),
}
# ---- 市场魔方：常驻刷新管道（长连接 + 全局限速版）----
# 三个坑，逐个填：
#   1) 慢在握手不在数据：urllib 每次请求都新建 TCP+TLS 连接，实测握手 1.5~4s，
#      采集间隔 1.2s 全被握手吃掉。改成 requests.Session 长连接后同接口降到 0.2~1.0s。
#   2) 上游有硬限流：请求太密直接返回 429，实测可持续速率只有 ~20~25 次/分钟，
#      超过就整片 429——数据一停十几秒，看起来就是「延迟很大」。
#      所以这里做全局令牌桶 + 撞 429 自动放慢，宁慢不挂。
#   3) 部分品种上游本身更新稀疏（原油/黄金/白银/美元指数时间戳落后 3~10 分钟），
#      叠一层新浪外盘行情加速：同一标的、0.2s 返回、实测比上游新好几分钟，谁新用谁。
_MF_ECON_INTERVAL = 20.0    # 经济：VIX/美债上游本身 5 分钟才变一次，20s 足够
_MF_SECT_INTERVAL = 3.0     # 产业：26 板块 3s 一次已经足够「实时」（官网自己 60s 一次）
_MF_CONNECT_TIMEOUT = 3.0
_MF_READ_TIMEOUT = 8.0
_MF_MAX_BACKOFF = 2.0
# 全局令牌桶：所有上游请求共用，撞 429 就整体放慢、连续成功再慢慢提速
# 实测该上游按 IP 限流：可持续只有 0.4~0.5 次/秒，再往上就是成片 429。
# 所以令牌桶的默认节奏直接压在阈值下方——宁可稳定 2 秒一次，也不要忽快忽慢：
# 撞一次 429 要罚 1.5 秒冷却，那个尖峰比「慢一点」难看得多。
_MF_GATE_MIN = 1.2
_MF_GATE_MAX = 3.0      # 上限别放太高：被限流时涨上去容易，落回来慢
_MF_GATE_START = 2.2
# 令牌分配：经济 20s 一次、板块 3s 一次，两路都按 1.0 权重排队（降频后不再互抢到 429）。
_MF_GAP_FACTOR = {'econ': 1.0, 'sect': 1.0}
# 口径：'realtime'（盘中实时） / 'previousTradingDay'（上一交易日收盘），与官网同名同语义
_mf_quote_mode = {'mode': 'realtime'}

_mf_lock = threading.Lock()
_mf_parts = {
    'econ': {'data': None, 'ts': 0.0, 'err': '', 'ok': 0, 'fail': 0, 'lat': 0.0, 'hold': 0},
    'sect': {'data': None, 'ts': 0.0, 'err': '', 'ok': 0, 'fail': 0, 'lat': 0.0, 'hold': 0},
}
_mf_status = {'text': '', 'color': None}
_mf_started = [False]
_mf_err_log = {}
# 加速层（新浪）：{'data': {key: {...}}, 'ts': 抓到时刻, 'err': '', 'lat': 秒}
_mf_fast = {'data': {}, 'ts': 0.0, 'err': '', 'lat': 0.0}
# 全局节流状态：interval=当前放行间隔, next=下个令牌时刻, streak=连续成功数
_mf_gate = {'interval': _MF_GATE_START, 'next': 0.0, 'streak': 0, 'throttled': 0}

try:
    import requests as _rq
    from requests.adapters import HTTPAdapter as _HTTPAdapter
except Exception:   # 没装 requests 也能跑，只是退回 urllib（慢）
    _rq = None
    _HTTPAdapter = None

_mf_tls = threading.local()


class _MFSoftError(Exception):
    """上游偶发空响应/半截包，重试就好，不该按失败长退避。"""


class _MFThrottled(Exception):
    """上游 429：必须整体放慢，不能硬顶。"""

    def __init__(self, retry_after=None):
        self.retry_after = retry_after
        Exception.__init__(self, '429 请求过密')


def _mf_headers():
    """上游要求的伪装头。Visitor-Id 每个连接固定一个（每请求都换会一直被当成新访客）。"""
    return {
        'User-Agent': _UA_DESKTOP,
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate',
        'Referer': 'https://www.bluecatsite.com/',
        'X-Finlens-Client-Source': 'scmfWeb',
        'X-Finlens-Visitor-Id': str(uuid.uuid4()),
    }


def _mf_session():
    """取当前线程独占的长连接会话（线程常驻，所以连接能一直被复用）。"""
    if _rq is None:
        return None
    s = getattr(_mf_tls, 'sess', None)
    if s is None:
        s = _rq.Session()
        s.headers.update(_mf_headers())
        ad = _HTTPAdapter(pool_connections=2, pool_maxsize=4, max_retries=0, pool_block=False)
        s.mount('https://', ad)
        _mf_tls.sess = s
    return s


def _mf_gate_acquire():
    """全局令牌桶：拿不到就等。所有上游请求都从这一个口子过。"""
    while True:
        with _mf_lock:
            now = time.time()
            wait = _mf_gate['next'] - now
            if wait <= 0:
                _mf_gate['next'] = now + _mf_gate['interval']
                return
        time.sleep(min(wait, 0.12))


def _mf_gate_throttled(retry_after=None):
    with _mf_lock:
        _mf_gate['interval'] = min(_MF_GATE_MAX, _mf_gate['interval'] * 1.12)
        _mf_gate['streak'] = 0
        _mf_gate['throttled'] += 1
        _mf_gate['next'] = max(_mf_gate['next'], time.time() + (retry_after or 1.0))


def _mf_gate_ok():
    with _mf_lock:
        _mf_gate['streak'] += 1
        if _mf_gate['streak'] >= 6:       # 连续 6 次没被限，小幅提速（探到阈值下方就停）
            _mf_gate['streak'] = 0
            _mf_gate['interval'] = max(_MF_GATE_MIN, _mf_gate['interval'] * 0.9)


def _mf_decrypt(b64s):
    raw = base64.b64decode(b64s)
    iv, ct = raw[:16], raw[16:]
    out = bytearray()
    for i in range((len(ct) + 31) // 32):
        blk = hmac.new(_MF_KEY, iv + i.to_bytes(4, 'big'), hashlib.sha256).digest()
        seg = ct[i * 32:(i + 1) * 32]
        out.extend(b ^ blk[j] for j, b in enumerate(seg))
    return json.loads(out.decode('utf-8'))


def _mf_fetch(path, page_open=False):
    """拉一个上游接口。走长连接；requests 不可用时退回 urllib。"""
    url = _MF_BASE + path
    s = _mf_session()
    if s is None:
        req = _urlreq.Request(url, headers=_mf_headers())
        try:
            with _urlreq.urlopen(req, timeout=_MF_READ_TIMEOUT) as r:
                body = r.read().decode('utf-8', 'ignore')
                enc = r.headers.get('X-Finlens-Enc')
        except Exception as e:
            if '429' in str(e):
                raise _MFThrottled()
            raise
        if not (body or '').strip():
            raise _MFSoftError('上游返回空响应')
        js = json.loads(body)
    else:
        hd = {'X-Finlens-Page-Open': '1'} if page_open else None
        r = s.get(url, headers=hd, timeout=(_MF_CONNECT_TIMEOUT, _MF_READ_TIMEOUT))
        if r.status_code == 429:
            ra = r.headers.get('Retry-After')
            raise _MFThrottled(float(ra) if (ra or '').isdigit() else None)
        if r.status_code != 200:
            raise _MFSoftError('HTTP %d' % r.status_code)
        enc = r.headers.get('X-Finlens-Enc')
        if not (r.text or '').strip():
            raise _MFSoftError('上游返回空响应')
        js = json.loads(r.text)
    if enc == '1' and isinstance(js.get('d'), str):
        return _mf_decrypt(js['d'])
    return js


def _mf_iso_ts(s):
    """'2026-11-12T12:39:59Z' -> epoch"""
    try:
        from datetime import timezone
        return datetime.strptime(str(s)[:19], '%Y-%m-%dT%H:%M:%S').replace(
            tzinfo=timezone.utc).timestamp()
    except Exception:
        return 0.0


def _mf_hhmmss_ts(s):
    """新浪给的 '20:44:43'（北京时间）-> 今天该时刻的 epoch。

    跨零点修正：凌晨看到 '23:5x' 的时间戳属于昨天，直接拼今天的日期会得到「未来时间」，
    会让 _mf_merge_fast 误判成「比上游新」而覆盖最新价。
    """
    try:
        h, m, sec = [int(x) for x in str(s).split(':')[:3]]
        now = datetime.now()
        ts = datetime(now.year, now.month, now.day, h, m, sec).timestamp()
        if ts - now.timestamp() > 12 * 3600:
            ts -= 86400
        return ts
    except Exception:
        return 0.0


def _mf_ts_hms(ts):
    """epoch -> 本地 hh:mm:ss（上游给的是 UTC，直接显示会差 8 小时）"""
    try:
        return datetime.fromtimestamp(ts).strftime('%H:%M:%S') if ts else ''
    except Exception:
        return ''


def _mf_fmt_price(p):
    try:
        return '%.2f' % float(p)
    except Exception:
        return '--'


def _mf_build_econ(js):
    idx = []
    for it in (js.get('indices') or []):
        ts = _mf_iso_ts(it.get('lastUpdated'))
        idx.append({
            'key': it.get('key'),
            'name': it.get('name'),
            'price': it.get('price'),
            'text': it.get('value') or (('%.2f' % it['price']) if it.get('price') is not None else '--'),
            'pct': round(float(it.get('changePercent') or 0), 2),
            'trend': it.get('trend') or 'flat',
            'time': _mf_ts_hms(ts),
            'ts': ts,
            'last_updated': it.get('lastUpdated') or '',
            'src': 'mofang',
        })
    return idx


def _mf_build_sect(quotes):
    # 恒按 _MF_SECTOR_NAMES 的固定顺序输出 26 项：上游偶发残包时前端只把个别项显示为「--」，
    # 而不是整块「暂无数据」闪一下（审计 §4.6 的「数据消失/闪变」）。
    got = {}
    for it in (quotes.get('s') or []):
        code = it.get('c')
        if code:
            got[code] = it
    sectors = []
    for code, (emoji, name) in _MF_SECTOR_NAMES.items():
        it = got.get(code)
        v = it.get('v') if it else None
        sectors.append({'code': code, 'emoji': emoji, 'name': name,
                        'pct': round(float(v), 2) if v is not None else None,
                        'missing': it is None})
    status = {'color': quotes.get('sectorStatusColor'),
              'text': quotes.get('sectorStatusText') or ''}
    return sectors, status


def _mf_pull(part):
    """拉一次指定数据源，成功后直接替换内存快照。"""
    if part == 'econ':
        # 不带 X-Finlens-Page-Open：实测 2.6s vs 4.5s，快近一倍且数据一致
        data = _mf_build_econ(_mf_fetch('/economy'))
        with _mf_lock:
            _mf_parts['econ'].update({'data': data, 'ts': time.time(), 'err': ''})
    else:
        mode = _mf_quote_mode.get('mode') or 'realtime'
        raw = _mf_fetch('/quotes?page=home&quoteMode=' + mode)
        codes = {it.get('c') for it in (raw.get('s') or []) if it.get('c')}
        if len(codes) < 20:      # 26 个里少于 20 个视为残包，保留上一份完整快照
            raise _MFSoftError('上游残包（%d/26）' % len(codes))
        sectors, status = _mf_build_sect(raw)
        with _mf_lock:
            _mf_parts['sect'].update({'data': sectors, 'ts': time.time(), 'err': ''})
            _mf_status.update(status)
            _mf_status['mode'] = mode


def _mf_worker(part):
    """常驻采集线程：拉一次、记一次延迟、歇一会儿再来。线程常驻 => 连接常驻。"""
    gap0 = _MF_ECON_INTERVAL if part == 'econ' else _MF_SECT_INTERVAL
    while True:
        try:
            gap = _mf_tick(part, gap0)
        except Exception as ex:      # 兜底：绝不让采集线程意外退出（死一条整片数据就停）
            print('[mofang] %s 线程异常（已继续）: %s' % (part, str(ex)[:100]), flush=True)
            gap = 1.0
        time.sleep(max(gap0, gap))


def _mf_tick(part, gap0):
    """跑一轮采集，返回这一轮该歇多久。"""
    _mf_gate_acquire()
    t0 = time.time()
    err = ''
    soft = False
    try:
        _mf_pull(part)
    except _MFThrottled as ex:
        # 被限流：整体放慢，这一轮不算失败（内存里本来就有旧数据）
        _mf_gate_throttled(ex.retry_after)
        with _mf_lock:
            p = _mf_parts[part]
            p['fail'] += 1
            p['err'] = '上游限流，已自动降速'
            p['hold'] = time.time()
            iv = _mf_gate['interval']
        if time.time() - _mf_err_log.get('429', 0) > 120:
            _mf_err_log['429'] = time.time()
            print('[mofang] 上游限流（429），放慢到 %.1fs/次' % iv, flush=True)
        return 0.0
    except _MFSoftError as ex:
        err, soft = str(ex)[:120], True
    except Exception as ex:
        err = str(ex)[:120]
    cost = time.time() - t0
    p = _mf_parts[part]
    with _mf_lock:
        if err:
            p['err'] = err
            p['fail'] += 1
        else:
            p['ok'] += 1
            p['err'] = ''
            p['hold'] = 0
            p['lat'] = round(p['lat'] * 0.7 + cost * 0.3, 3) if p['lat'] else round(cost, 3)
    if err:
        if not soft and time.time() - _mf_err_log.get(part, 0) > 60:
            _mf_err_log[part] = time.time()
            print('[mofang] %s 刷新失败: %s' % (part, err))
        return 0.12 if soft else min(_MF_MAX_BACKOFF, max(0.3, cost))
    _mf_gate_ok()
    # 按权重抢令牌：间隔短的那一路自然拿到更多次机会
    with _mf_lock:
        return _MF_GAP_FACTOR.get(part, 1.0) * _mf_gate['interval']


# ---- 加速层：新浪外盘行情（原油 / 黄金 / 白银 / 美元指数）----
# 魔方上游这 4 项的时间戳实测会落后 3~10 分钟，而新浪同一标的 0.2s 返回、基本实时，
# 所以「谁的时间戳更新就用谁」——不改变展示口径，只是把数字换成更新的那一笔。
# 新浪这个接口实测 3 次/秒无任何限流，所以它能真正做到「每秒刷新」。
_MF_FAST_CODES = {'brent': 'hf_OIL', 'gold': 'hf_XAU', 'silver': 'hf_XAG', 'usd': 'DINIW'}
_MF_FAST_URL = 'https://hq.sinajs.cn/list=' + ','.join(_MF_FAST_CODES.values())
_MF_FAST_GAP = 0.35


def _mf_parse_sina(text):
    out = {}
    rev = {v: k for k, v in _MF_FAST_CODES.items()}
    for line in str(text).split(';'):
        if 'hq_str_' not in line:
            continue
        try:
            code = line.split('hq_str_')[1].split('=')[0]
            body = line.split('"')[1] if '"' in line else ''
        except Exception:
            continue
        key = rev.get(code)
        if not key or not body:
            continue
        f = body.split(',')
        try:
            if code == 'DINIW':
                # 新浪外汇口径：0=时间 1=现价 2=现价 3=昨收 4=? 5=今开 6=最高 7=最低
                # 审计实测：旧代码把 f[5]（今开）当昨收，涨跌幅差 0.03pp（+0.22% vs 官方 +0.19%）。
                price, prev, tm = float(f[1]), float(f[3]), f[0]
            else:
                # 外盘期货口径：0=现价 4=最高 5=最低 6=时间 7=昨收
                price, prev, tm = float(f[0]), float(f[7]), f[6]
        except Exception:
            continue
        if price <= 0 or prev <= 0:
            continue
        out[key] = {'price': price,
                    'pct': round((price - prev) / prev * 100.0, 2),
                    'time': tm,
                    'ts': _mf_hhmmss_ts(tm)}
    return out


def _mf_fast_worker():
    s = _mf_session()
    if s is None:
        return
    backoff = 0.0
    while True:
        t0 = time.time()
        try:
            r = s.get(_MF_FAST_URL, headers={'Referer': 'https://finance.sina.com.cn/'},
                      timeout=(2.5, 5))
            r.encoding = 'gbk'
            data = _mf_parse_sina(r.text)
            if not data:
                raise ValueError('空响应')
            with _mf_lock:
                _mf_fast.update({'data': data, 'ts': time.time(), 'err': '',
                                 'lat': round(time.time() - t0, 3)})
            backoff = 0.0
        except Exception as e:
            backoff = min(5.0, max(1.0, (backoff or 1.0) * 2))
            with _mf_lock:
                _mf_fast['err'] = str(e)[:80]
        time.sleep(backoff if backoff else _MF_FAST_GAP)


def _mf_merge_fast(econ):
    """把更「新」的那一笔换上去：逐项比时间戳，新浪更新才覆盖。"""
    fast = _mf_fast.get('data') or {}
    if not fast or not econ:
        return econ
    out = []
    for it in econ:
        o = fast.get(it.get('key'))
        if o and o.get('ts', 0) > (it.get('ts') or 0) + 0.3:
            it = dict(it)
            it['price'] = o['price']
            it['pct'] = o['pct']
            it['text'] = _mf_fmt_price(o['price'])
            it['time'] = o['time']
            it['ts'] = o['ts']
            it['src'] = 'sina'
        out.append(it)
    return out


@app.route('/api/mofang')
def api_mofang():
    """全球经济数据（6项）+ 全球产业数据（26个板块涨跌）。

    只读内存快照（常驻线程在后台持续拉取），所以接口本身不耗时；
    返回里带 age（数据已存在多少秒），前端据此显示真实延迟。

    口径（审计结论）：官网有「实时 / 上一交易日收盘」两档，且同一时刻两档差 0.7~8pp。
    插件默认 realtime，但必须把当前口径显示出来，并支持 ?mode=realtime|previousTradingDay 切换，
    用户拿官网对比时可以切到同一档。
    加速层（新浪 4 项）默认不覆盖官方值，只有 ?fast=1 才启用（默认展示与官网同源的原值）。
    """
    _mf_ensure_started()
    want_mode = request.args.get('mode') or ''
    if want_mode in ('realtime', 'previousTradingDay') and want_mode != _mf_quote_mode.get('mode'):
        _mf_quote_mode['mode'] = want_mode
        # 切档位后立刻拉一次，别让用户等下一个 3s 周期还看到旧档位的数字
        def _refresh_sect():
            try:
                _mf_pull('sect')
            except Exception as ex:
                print('[mofang] 切档位刷新失败: %s' % str(ex)[:100], flush=True)
        threading.Thread(target=_refresh_sect, daemon=True).start()
    fast = request.args.get('fast') == '1'
    now = time.time()
    econ = _mf_parts['econ']
    sect = _mf_parts['sect']
    e_data = _mf_merge_fast(econ['data'] or []) if fast else (econ['data'] or [])
    s_data = sect['data'] or []
    ts_list = [t for t in (econ['ts'], sect['ts']) if t]
    payload = {
        'code': 0,
        'economy': e_data,
        'sectors': s_data,
        'status': dict(_mf_status),
        'quoteMode': _mf_quote_mode.get('mode') or 'realtime',
        'mode_label': (_mf_status.get('text') or ''),
        'fast': fast,
        'updated': (datetime.fromtimestamp(max(ts_list)).strftime('%Y-%m-%d %H:%M:%S')
                    if ts_list else ''),
        'econ_updated': (datetime.fromtimestamp(econ['ts']).strftime('%H:%M:%S')
                         if econ['ts'] else ''),
        'sect_updated': (datetime.fromtimestamp(sect['ts']).strftime('%H:%M:%S')
                         if sect['ts'] else ''),
        'age': {'economy': (round(now - econ['ts'], 1) if econ['ts'] else None),
                'sectors': (round(now - sect['ts'], 1) if sect['ts'] else None),
                'fast': (round(now - _mf_fast['ts'], 1) if _mf_fast['ts'] else None)},
        'lat': {'economy': econ['lat'], 'sectors': sect['lat'], 'fast': _mf_fast['lat']},
        'rate': {'interval': round(_mf_gate['interval'], 2), 'throttled': _mf_gate['throttled']},
        'pending': not (e_data or s_data),
        'err': econ['err'] or sect['err'] or '',
        'source': '市场魔方 bluecatsite.com' + (' + 新浪外盘（加速）' if fast else ''),
    }
    resp = jsonify(payload)
    resp.headers['Cache-Control'] = 'no-store, max-age=0'
    return resp


def _mf_ensure_started():
    with _mf_lock:
        if _mf_started[0]:
            return
        _mf_started[0] = True
    threading.Thread(target=_mf_worker, args=('econ',), daemon=True).start()
    threading.Thread(target=_mf_worker, args=('sect',), daemon=True).start()
    if _rq is not None:
        threading.Thread(target=_mf_fast_worker, daemon=True).start()
    print('[mofang] 常驻刷新已启动（长连接 + 全局限速 %.1fs/次 · 新浪加速 4 项 %.2fs）'
          % (_MF_GATE_START, _MF_FAST_GAP), flush=True)


def _mf_refresh_async():
    """兼容旧调用：确保常驻刷新已启动。"""
    _mf_ensure_started()


# ==================== 投资日历 ====================
# 金十日历（rili.jin10.com）的公开 CDN(cdn-rili.jin10.com) 已下线，且日历正文改为登录可见，
# 因此以「百度股市通·全球财经日历」为主源（同为宏观数据+事件+重要性分级），
# 金十源保留探测逻辑：一旦恢复会自动优先使用。
_cal_cache = {}
_CAL_TTL = 600
_jin10_dead_until = 0.0


def _cal_jin10_day(day):
    """金十日历单日数据（源已下线时快速失败）。"""
    global _jin10_dead_until
    if time.time() < _jin10_dead_until:
        return None
    try:
        y, m, d = day.split('-')
        txt = http_get('https://cdn-rili.jin10.com/web_data/%s/%s/%s.json' % (y, m, d),
                       referer='https://rili.jin10.com/', timeout=4, retry=1)
        arr = json.loads(txt)
        rows = []
        for it in (arr or []):
            star = it.get('star') or it.get('importance') or 1
            rows.append({'date': day, 'time': str(it.get('time') or '')[:5],
                         'region': it.get('country') or it.get('region') or '',
                         'event': it.get('name') or it.get('event') or '',
                         'actual': str(it.get('actual') or ''),
                         'forecast': str(it.get('consensus') or it.get('forecast') or ''),
                         'prev': str(it.get('previous') or it.get('prev') or ''),
                         'star': int(star) if str(star).isdigit() else 1})
        return rows or None
    except Exception:
        _jin10_dead_until = time.time() + 1800   # 30 分钟内不再尝试
        return None


def _cal_day(day):
    """单个自然日的日历数据，带内存缓存。"""
    hit = _cal_cache.get(day)
    if hit and time.time() - hit[0] < _CAL_TTL:
        return hit[1], hit[2]
    rows = _cal_jin10_day(day)
    src = '金十数据'
    if rows is None:
        rows = []
        src = '百度股市通'
        try:
            df = ak.news_economic_baidu(date=day.replace('-', ''))
            for _, r in df.iterrows():
                def _s(v):
                    if v is None:
                        return ''
                    s = str(v).strip()
                    return '' if s.lower() in ('nan', 'none') else s
                star = r.get('重要性')
                try:
                    star = int(float(star))
                except (TypeError, ValueError):
                    star = 1
                rows.append({
                    'date': str(r.get('日期') or day)[:10],
                    'time': _s(r.get('时间')),
                    'region': _s(r.get('地区')),
                    'event': _s(r.get('事件')),
                    'actual': _s(r.get('公布')),
                    'forecast': _s(r.get('预期')),
                    'prev': _s(r.get('前值')),
                    'star': max(1, min(3, star)),
                })
        except Exception as e:
            print('[calendar] %s 抓取失败: %s' % (day, str(e)[:80]))
    _cal_cache[day] = (time.time(), rows, src)
    return rows, src


@app.route('/api/calendar')
def api_calendar():
    """投资日历：?start=YYYY-MM-DD&end=YYYY-MM-DD（默认今天~+7天，最长 31 天）"""
    today = datetime.now()
    start = request.args.get('start') or today.strftime('%Y-%m-%d')
    end = request.args.get('end') or (today + timedelta(days=7)).strftime('%Y-%m-%d')
    try:
        d0 = datetime.strptime(start, '%Y-%m-%d')
        d1 = datetime.strptime(end, '%Y-%m-%d')
    except Exception:
        return jsonify({'code': -1, 'msg': '日期格式应为 YYYY-MM-DD'})
    if d1 < d0:
        d0, d1 = d1, d0
    if (d1 - d0).days > 31:
        d1 = d0 + timedelta(days=31)
    days = [(d0 + timedelta(days=i)).strftime('%Y-%m-%d') for i in range((d1 - d0).days + 1)]

    results, srcs = [], set()
    with ThreadPoolExecutor(max_workers=5) as ex:
        for rows, src in ex.map(_cal_day, days):
            srcs.add(src)
            results.extend(rows)
    results.sort(key=lambda x: (x['date'], x['time'] or '99:99'))
    return jsonify({
        'code': 0, 'start': days[0], 'end': days[-1],
        'days': len(days), 'count': len(results),
        'source': '、'.join(sorted(srcs)),
        'data': results,
    })


# ==================== K线响应缓存（切换周期提速） ====================
_kline_cache = {}
_KLINE_TTL_DAY = 300      # 日K/周K：5 分钟
_KLINE_TTL_MIN = 60       # 分钟K：1 分钟
_kline_cache_lock = threading.Lock()


def _kline_ttl(period):
    if period in ('day', 'week', '120'):
        return _KLINE_TTL_DAY
    if period == '1':
        # 分时：前端盘中每 30s 重取一次，缓存必须比刷新间隔短，
        # 否则分钟 K 线会一直停在打开页面时的那一分钟（用户感知为「延迟很高」）。
        return 20
    return _KLINE_TTL_MIN


@app.before_request
def _kline_cache_lookup():
    if request.method != 'GET' or not request.path.startswith('/api/kline/'):
        return None
    period = request.args.get('period', 'day')
    key = (request.path, period)
    hit = _kline_cache.get(key)
    if hit and time.time() - hit[0] < _kline_ttl(period):
        return Response(hit[1], mimetype='application/json',
                        headers={'X-Kline-Cache': 'hit'})
    return None


@app.after_request
def _kline_cache_store(resp):
    if (request.method == 'GET' and request.path.startswith('/api/kline/')
            and resp.status_code == 200
            and resp.headers.get('X-Kline-Cache') != 'hit'):   # 命中时不要再存，否则 TTL 永远不过期
        period = request.args.get('period', 'day')
        try:
            body = resp.get_data(as_text=True)
            if body and len(body) > 60:
                with _kline_cache_lock:
                    if len(_kline_cache) > 300:
                        _kline_cache.clear()
                    _kline_cache[(request.path, period)] = (time.time(), body)
                resp.headers['X-Kline-Cache'] = 'miss'
        except Exception:
            pass
    return resp


def background_scheduler():
    """后台线程：启动先取最新值，再补历史，之后每5分钟检查更新；异常不中断。"""
    # 延迟2秒保证Flask先绑定端口
    time.sleep(2)
    # 先拿最新数据（用户打开页面就能看到当日值）
    try:
        print('启动首次检查数据更新...', flush=True)
        update_etf_share()
        update_fear_greed()
    except Exception:
        traceback.print_exc()
    # 成交额长历史：先备好腾讯日线缓存（首次约 30s），否则首次回填/回测里
    # 成交额只有 2022-10 之后的数据，5y 的 turn/turn_pct 会整段缺失。
    try:
        _tx_turn_refresh_async()
        _t0 = time.time()
        while not _tx_turn_load() and time.time() - _t0 < 120:
            time.sleep(2)
    except Exception:
        traceback.print_exc()
    # 再补历史（逐日回溯上交所份额，耗时较长，放后台慢慢跑）
    etf_gained = 0
    try:
        backfill_90_days()
        # ETF 份额：首次深回填 1100 天（约 6~10 分钟），之后每次只补近 60 天
        deep = not _etf_deep_done()
        _full, etf_gained = backfill_etf_history(1100 if deep else 60)
    except Exception:
        traceback.print_exc()
    # 评分快照回填：用历史接口逐日重算 S/M/L，补齐「历史数据补全」。
    # ETF 份额新增较多（首次深回填）时强制重建旧快照，让 etf_share_pct 从 2024 起有值。
    try:
        _bt_backfill_if_needed(force=(etf_gained >= 20))
    except Exception:
        traceback.print_exc()
    # 之后每5分钟检查一次，无需等待收盘，非交易日自然无新数据
    while True:
        time.sleep(5 * 60)
        try:
            update_etf_share()
            update_fear_greed()
        except Exception:
            traceback.print_exc()

if __name__ == '__main__':
    # DSH 插件宿主用环境变量指定端口；独立运行时保持原来的 5099。
    _port = int(os.environ.get('ABOARD_PORT', '5099') or 5099)
    # ABOARD_NO_BACKGROUND=1 时只提供纯数据接口，不起定时抓取线程。
    _background = os.environ.get('ABOARD_NO_BACKGROUND', '').strip() not in ('1', 'true', 'yes')
    print('=' * 50)
    print('  A股市场看板 -> http://127.0.0.1:%d' % _port)
    print('=' * 50, flush=True)
    if _background:
        # 启动后台定时线程，daemon=True随主进程退出
        t = threading.Thread(target=background_scheduler, daemon=True)
        t.start()
        # 市场魔方（全球经济/产业）首次拉取要十几秒，启动时先预热，页面首屏直接命中缓存
        _mf_refresh_async()
    app.run(host='127.0.0.1', port=_port, debug=False, threaded=True)
