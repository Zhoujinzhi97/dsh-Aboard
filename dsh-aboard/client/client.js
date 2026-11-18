/**
 * dsh-aboard — client half.
 *
 * Adds a sidebar entry and a main panel that render the A-share dashboard served by
 * this package's Host half at `/aboard`. The dashboard is a self-contained ECharts
 * application, so the panel frames it same-origin and lets it talk to `/aboard/api/*`
 * on its own; the panel only mounts, watches readiness, and offers a fallback link.
 */

window.__ModuleLoader__.load({
  id: 'dsh-aboard',
  factory(require) {
    const React = require('react')
    const h = React.createElement

    const NS = 'dsh-aboard'
    const PANEL_ID = 'aboard'
    const PANEL_URL = '/aboard'
    const PANEL_ORDER = 2

    const ZH = {
      nav: 'A股看板',
      title: 'A股市场看板',
      timeout: '看板仍在加载，可能正在启动数据后端。',
      openTab: '在新标签页中打开',
      retry: '重试',
      slowHint: '首次启动需要加载数据后端，通常 10～60 秒。',
    }

    const EN = {
      nav: 'A-share board',
      title: 'A-share market dashboard',
      timeout: 'The dashboard is still loading, probably starting its data backend.',
      openTab: 'Open in a new tab',
      retry: 'Retry',
      slowHint: 'The first launch starts the data backend and usually takes 10–60 seconds.',
    }

    const STYLES = [
      '.aboard-panel-host{position:relative;box-sizing:border-box;display:flex;flex-direction:column;height:100%;',
      'min-height:0;padding:calc(28px + var(--dsh-frame-top-clearance,0px)) 0 0;background:transparent;}',
      '.aboard-panel-frame{flex:1 1 auto;width:100%;height:100%;min-height:0;border:0;display:block;',
      'background:#0a0d13;color-scheme:dark;}',
      '.aboard-panel-overlay{position:absolute;left:0;right:0;bottom:0;top:calc(28px + var(--dsh-frame-top-clearance,0px));',
      'display:flex;align-items:center;justify-content:center;flex-direction:column;gap:10px;padding:24px;',
      'text-align:center;background:rgba(10,13,19,0.92);color:#8b96ab;font-size:12px;line-height:1.7;',
      'font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;}',
      '.aboard-panel-overlay strong{color:#e8ecf3;font-size:13px;font-weight:600;}',
      '.aboard-panel-actions{display:flex;gap:8px;flex-wrap:wrap;justify-content:center;margin-top:4px;}',
      '.aboard-panel-actions button,.aboard-panel-actions a{cursor:pointer;font:inherit;font-size:12px;',
      'padding:6px 14px;border-radius:6px;border:1px solid #2c3a4f;background:#1b2230;color:#e8ecf3;',
      'text-decoration:none;display:inline-block;}',
      '.aboard-panel-actions button:hover,.aboard-panel-actions a:hover{border-color:#4e9cf5;color:#fff;}',
    ].join('')

    const ICON_RED = '#f04452'

    function BoardIcon(props) {
      const owner = props || {}
      const size = typeof owner.size === 'number' ? owner.size : 16
      return h(
        'svg',
        {
          viewBox: '0 0 24 24',
          width: size,
          height: size,
          fill: 'none',
          'aria-hidden': true,
          style: { display: 'block', opacity: owner.active ? 1 : 0.72 },
        },
        h('rect', { x: 2.5, y: 13, width: 4, height: 8.5, rx: 1.2, fill: ICON_RED }),
        h('rect', { x: 9.75, y: 8, width: 4, height: 13.5, rx: 1.2, fill: ICON_RED, opacity: 0.72 }),
        h('rect', { x: 17, y: 2.75, width: 4, height: 18.75, rx: 1.2, fill: ICON_RED, opacity: 0.45 }),
      )
    }

    const SLOW_AFTER_MS = 75000

    // ------------------------------------------------------------------
    // iframe 保活：面板切换/收起时 DSH 会卸载 React 面板，若 iframe 跟着被销毁，
    // 每次切回都要重新 GET /aboard、重跑 260KB 内联 JS、重建 13 个 ECharts
    // （实测 145~150 请求 / 570KB / 2.4~3.2s）。这里把 iframe 只创建一次，挂到
    // body 下的常驻容器里；面板渲染时把它搬进挂载点，卸载时先搬回容器再让 React 移除。
    // 同时通过 postMessage 通知看板「已隐藏/已显示」，让它停/启轮询与 resize。
    // ------------------------------------------------------------------
    const KEEP_ID = 'dsh-aboard-keep'
    let keepEl = null
    let frameEl = null

    function postToFrame(type) {
      try {
        if (frameEl && frameEl.contentWindow) {
          frameEl.contentWindow.postMessage({ type }, window.location.origin)
        }
      } catch (error) {
        /* cross-origin or detached: ignore */
      }
    }

    // 轻量调试：把保活生命周期写进 window.__aboardDbg，便于 CDP 断言是否复用同一 iframe
    function dbg(msg, extra) {
      try {
        const list = (window.__aboardDbg = window.__aboardDbg || [])
        list.push(extra === undefined ? msg : `${msg}:${extra}`)
      } catch (error) {
        /* ignore */
      }
    }

    function ensureKeepEl() {
      if (keepEl && keepEl.isConnected) return keepEl
      keepEl = document.createElement('div')
      keepEl.id = KEEP_ID
      // 常驻容器保持 iframe 的可见尺寸，避免 display:none 让内部 ECharts 变为 0x0；
      // z-index 只压过主面板内容（之前用 2147483000 会盖住 DSH 自己的弹层）
      keepEl.style.cssText = [
        'position:fixed;left:0;top:0;width:0;height:0;',
        'visibility:hidden;pointer-events:none;z-index:1000;',
      ].join('')
      document.body.appendChild(keepEl)
      return keepEl
    }

    function ensureFrame() {
      if (!frameEl) {
        frameEl = document.createElement('iframe')
        frameEl.className = 'aboard-panel-frame'
        frameEl.src = PANEL_URL
        frameEl.title = ZH.title
        frameEl.setAttribute('referrerpolicy', 'same-origin')
        dbg('create-frame')
        frameEl.addEventListener('load', () => {
          frameEl.dataset.loaded = '1'
          frameEl.dispatchEvent(new Event('aboard-ready'))
          postToFrame('aboard-show')
          dbg('frame-load')
        })
      }
      const host = ensureKeepEl()
      if (!frameEl.isConnected) host.appendChild(frameEl)
      return frameEl
    }

    // 浮层可见性由「面板是否仍挂载 + 挂载点是否仍有尺寸 + 当前视图是不是聊天」共同决定。
    // 之前的 rAF 每帧 getBoundingClientRect + 只在 cleanup 隐藏有两个致命问题：
    //   1) 每帧强制布局，壳层出现 480ms 长任务，看板打开时整个工作区卡顿；
    //   2) 某些切换路径下 React 面板被保留（cleanup 不执行），浮层就会一直盖住聊天区，
    //      用户表现为「无法返回工作区对话框」。
    // 现在改成 250ms 轮询定位 + 看门狗：只要面板失联、尺寸为 0、或聊天输入框重新可见，
    // 立即隐藏并放开 pointer-events。
    let activeMount = null
    let overlayShown = false
    let overlayTimer = null

    function hasVisibleComposer() {
      try {
        const nodes = document.querySelectorAll('textarea, [contenteditable="true"]')
        for (const node of nodes) {
          if (!node.isConnected) continue
          const r = node.getBoundingClientRect()
          if (r.width > 24 && r.height > 12) return true
        }
        return false
      } catch (error) {
        return false
      }
    }

    function setOverlayShown(shown) {
      if (!keepEl) return
      if (shown === overlayShown) return
      overlayShown = shown
      keepEl.style.visibility = shown ? 'visible' : 'hidden'
      keepEl.style.pointerEvents = shown ? 'auto' : 'none'
      postToFrame(shown ? 'aboard-show' : 'aboard-hide')
    }

    function positionOverlay() {
      if (!keepEl || !activeMount || !activeMount.isConnected) return false
      const r = activeMount.getBoundingClientRect()
      if (!r.width || !r.height) return false
      keepEl.style.left = `${Math.floor(r.left)}px`
      keepEl.style.top = `${Math.floor(r.top)}px`
      keepEl.style.width = `${Math.floor(r.width)}px`
      keepEl.style.height = `${Math.floor(r.height)}px`
      return true
    }

    function tickOverlay() {
      if (!activeMount || !activeMount.isConnected) {
        setOverlayShown(false)
        return
      }
      const placed = positionOverlay()
      // 聊天输入框重新可见说明已经切回工作区，浮层必须让位（否则挡住对话框）
      if (!placed || hasVisibleComposer()) {
        setOverlayShown(false)
        return
      }
      setOverlayShown(true)
    }

    function BoardPanel(props) {
      const t = props && typeof props.t === 'function' ? props.t : (key) => (ZH[key] ?? key)
      const mountRef = React.useRef(null)
      const lastRetryRef = React.useRef(0)
      const [retryTick, setRetryTick] = React.useState(0)
      const [loaded, setLoaded] = React.useState(false)
      const [slow, setSlow] = React.useState(false)

      // 关键：不要搬动 iframe 节点——Chromium 在 iframe 被 reparent 时会重新加载文档
      // （实测搬进常驻容器就会再 GET /aboard）。改为 iframe 永远留在常驻容器里，
      // 由容器跟随面板挂载点的位置/尺寸做「浮层」，切走时只隐藏容器。
      React.useLayoutEffect(() => {
        const mount = mountRef.current
        if (!mount) return undefined
        const frame = ensureFrame()
        ensureKeepEl()

        activeMount = mount
        tickOverlay()
        overlayTimer = window.setInterval(tickOverlay, 250)
        window.addEventListener('resize', tickOverlay)
        window.addEventListener('scroll', tickOverlay, true)

        const onReady = () => {
          setLoaded(true)
          setSlow(false)
        }
        if (frame.dataset.loaded === '1') onReady()
        frame.addEventListener('aboard-ready', onReady)
        dbg('mount:overlay')

        return () => {
          frame.removeEventListener('aboard-ready', onReady)
          if (overlayTimer) {
            window.clearInterval(overlayTimer)
            overlayTimer = null
          }
          window.removeEventListener('resize', tickOverlay)
          window.removeEventListener('scroll', tickOverlay, true)
          activeMount = null
          setOverlayShown(false)
          dbg('cleanup-hide')
        }
      }, [])

      // 重试：保活模式下不再重建节点，直接重载 iframe
      // 注意：不能用「首次运行跳过」的布尔量——React 18 StrictMode 会二次执行 effect，
      // 第二次会被误判成重试从而把 src 重新赋值导致整页重载。这里按 retryTick 变化判断。
      React.useEffect(() => {
        if (retryTick === 0 || retryTick === lastRetryRef.current) return
        lastRetryRef.current = retryTick
        const frame = frameEl
        if (!frame) return
        frame.dataset.loaded = ''
        setLoaded(false)
        try {
          frame.src = PANEL_URL
        } catch (error) {
          /* ignore */
        }
      }, [retryTick])

      React.useEffect(() => {
        // 慢启动提示只对「尚未加载完成」的首次启动有意义
        if (loaded) {
          setSlow(false)
          return undefined
        }
        const timer = window.setTimeout(() => setSlow(true), SLOW_AFTER_MS)
        return () => window.clearTimeout(timer)
      }, [loaded, retryTick])

      return h(
        'div',
        { className: 'aboard-panel-host', ref: mountRef },
        slow
          ? h(
              'div',
              { className: 'aboard-panel-overlay' },
              h('strong', null, t('timeout')),
              h('div', null, t('slowHint')),
              h(
                'div',
                { className: 'aboard-panel-actions' },
                h('button', { type: 'button', onClick: () => setRetryTick((n) => n + 1) }, t('retry')),
                h('a', { href: PANEL_URL, target: '_blank', rel: 'noreferrer' }, t('openTab')),
              ),
            )
          : null,
      )
    }

    function insertStyles() {
      const element = document.createElement('style')
      element.setAttribute('data-dsh-aboard', '')
      element.textContent = STYLES
      document.head.appendChild(element)
      return () => {
        if (element.parentNode) element.parentNode.removeChild(element)
      }
    }

    function apply(ctx) {
      ctx.effect(insertStyles, `${NS}: styles`)

      let tr = (key) => (ZH[key] ?? key)
      const locale = ctx.get('locale')
      if (locale && typeof locale.register === 'function') {
        try {
          const offZh = locale.register(NS, 'zh-CN', ZH)
          const offEn = locale.register(NS, 'en', EN)
          if (typeof offZh === 'function') ctx.effect(offZh, `${NS}: zh dictionary`)
          if (typeof offEn === 'function') ctx.effect(offEn, `${NS}: en dictionary`)
          const bound = typeof locale.bind === 'function' ? locale.bind(NS) : null
          const fallback = String(locale.getLocale ? locale.getLocale() : 'zh').toLowerCase().startsWith('en') ? EN : ZH
          tr = (key) => {
            if (bound) {
              try {
                const value = bound(key)
                if (typeof value === 'string' && value && value !== key) return value
              } catch {
                /* fall back to the static dictionary */
              }
            }
            return fallback[key] ?? key
          }
        } catch (error) {
          console.error(`[${NS}] locale setup failed`, error)
        }
      }

      ctx.slots.inject('main', () =>
        ctx.slots.register(
          {
            name: 'main',
            key: PANEL_ID,
            locale: NS,
            inject: () => ({ t: tr }),
          },
          BoardPanel,
        ),
      )

      ctx.slots.inject('sidebar.panellist', () =>
        ctx.slots.register(
          {
            name: 'sidebar.panellist',
            id: PANEL_ID,
            order: PANEL_ORDER,
            label: () => tr('nav'),
            locale: NS,
          },
          BoardIcon,
        ),
      )
    }

    return {
      inject: ['slots'],
      apply,
    }
  },
})
