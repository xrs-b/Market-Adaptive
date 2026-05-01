<script setup>
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'

const DEFAULT_API_BASE = `${window.location.protocol}//${window.location.hostname}:8008`
const API_BASE = import.meta.env.VITE_API_BASE || DEFAULT_API_BASE
const dark = ref(localStorage.getItem('admin-theme') !== 'light')
const token = ref(localStorage.getItem('admin-token') || '')
const username = ref(localStorage.getItem('admin-user') || '')
const loading = ref(false)
const loginForm = ref({ username: username.value || 'admin', password: 'admin123' })
const loginError = ref('')
const actionMessage = ref('')
const overview = ref(null)
const bots = ref(null)
const positions = ref([])
const orders = ref([])
const logs = ref([])
const timeline = ref({ activity: [], equityPoints: [], ctaEvents: [], riskEvents: [] })
const logFilter = ref('全部')
const moduleFilter = ref('全部模块')
const logKeyword = ref('')
const logQuickFilter = ref('全部')
const positionSort = ref({ key: '未实现盈亏', direction: 'desc' })
const orderSort = ref({ key: '价格', direction: 'desc' })
const logSort = ref({ key: '时间', direction: 'desc' })
const autoRefreshEnabled = ref(true)
const autoRefreshSeconds = ref(15)
const autoRefreshTimer = ref(null)
const lastSuccessfulRefreshAt = ref('')
const refreshFailureCount = ref(0)
const controlHistory = ref([])
const configSections = ref([])
const configSaving = ref(false)
const currentView = ref('overview')
const activeConfigSection = ref('')
const activeConfigGroup = ref('')
const configBaseline = ref({})
const initialEquity = ref(0)
const initialEquityInput = ref('')
const initialEquityMeta = ref({ source: '--', updatedAt: '' })
const initialEquitySaving = ref(false)
const financeMonth = ref(currentMonthKey())
const financeCalendar = ref({ month: currentMonthKey(), monthTotalPnl: 0, dayCount: 0, items: [], initialEquity: 0 })
const ctaWindowHours = ref(24)
const ctaSelectedFamily = ref('')
const ctaSelectedRegime = ref('')
const ctaHeatmapMode = ref('bias')
const ctaHeatmapSortMode = ref('bias_abs')
const ctaFamilyTrendMode = ref('cum_pnl')
const ctaPresetAudit = ref([])
const ctaDashboard = ref({ overview: {}, leaderboards: { all: [], long: [], short: [] }, family_catalog: [], regime_matrix: [], regime_transitions: [], family_score_timeseries: [], decision_audit: { missed_opportunities: [], bad_releases: [] }, suggestions: [], tuningSnapshot: {} })

const viewTabs = [
  { id: 'overview', label: '总览' },
  { id: 'cta-dashboard', label: 'CTA 驾驶舱' },
  { id: 'control', label: '系统控制' },
  { id: 'trading', label: '交易执行' },
  { id: 'logs', label: '日志审计' },
  { id: 'finance', label: '资金日历' },
  { id: 'config', label: '系统配置' },
]

const isAuthed = computed(() => Boolean(token.value))
const themeLabel = computed(() => (dark.value ? '切换白天模式' : '切换黑夜模式'))
const authHeaders = computed(() => ({ Authorization: `Bearer ${token.value}` }))
const latestRefreshTime = computed(() => formatDateTime(overview.value?.['刷新时间']))
const autoRefreshStatusText = computed(() => {
  if (!autoRefreshEnabled.value) return '自动刷新已关闭'
  return `自动刷新 ${autoRefreshSeconds.value}s`
})
const lastSuccessfulRefreshText = computed(() => formatDateTime(lastSuccessfulRefreshAt.value))
const statusStrip = computed(() => {
  if (!overview.value) return []
  return [
    { label: 'CTA', value: overview.value['CTA状态'] },
    { label: '网格', value: overview.value['网格状态'] },
    { label: '风控阻断', value: overview.value['风控阻断'] ? '已阻断' : '正常' },
    {
      label: '主控进程',
      value: overview.value['主进程运行'] ? `运行中${Array.isArray(overview.value['主进程PID']) && overview.value['主进程PID'].length ? ` · PID ${overview.value['主进程PID'].join(', ')}` : ''}` : '未运行',
    },
    { label: '交易对', value: overview.value['交易对'] || '--' },
  ]
})
const metricCards = computed(() => {
  if (!overview.value) return []
  return [
    {
      label: '账户权益',
      value: formatNumber(overview.value['账户权益']),
      sub: `未实现盈亏 ${signedNumber(overview.value['未实现盈亏'])}`,
      tone: metricToneByValue(overview.value['未实现盈亏']),
    },
    {
      label: '保证金率',
      value: percentText(overview.value['保证金率']),
      sub: `总名义价值 ${formatNumber(overview.value['总名义价值'])}`,
      tone: metricToneByRisk(overview.value['保证金率']),
    },
    {
      label: '持仓名义价值',
      value: formatNumber(overview.value['持仓名义价值']),
      sub: `委托名义价值 ${formatNumber(overview.value['委托名义价值'] ?? overview.value['挂单名义价值'])}`,
      tone: 'sky',
    },
    {
      label: '主控进程',
      value: overview.value['主进程运行'] ? '运行中' : '未运行',
      sub: Array.isArray(overview.value['主进程PID']) && overview.value['主进程PID'].length ? `PID ${overview.value['主进程PID'].join(', ')}` : '暂无 PID',
      tone: overview.value['主进程运行'] ? 'emerald' : 'slate',
    },
  ]
})
const botSummaryCards = computed(() => {
  if (!bots.value) return []
  const items = [bots.value['主控'], bots.value['CTA'], bots.value['网格'], bots.value['风控'], bots.value['市场判定']].filter(Boolean)
  return items.map((item) => ({
    名称: item['名称'],
    时间: item['时间'],
    级别: item['级别'],
    摘要: summarizeBotContent(item['内容']),
    原文: item['内容'],
  }))
})
const logCategories = computed(() => ['全部', ...new Set(logs.value.map((item) => item['分类']))])
const logModules = computed(() => ['全部模块', ...new Set(logs.value.map((item) => item['模块']))])
const logQuickFilters = ['全部', '异常优先', '只看风控', '只看 CTA', '只看网格']
const filteredLogs = computed(() =>
  logs.value.filter((item) => {
    const categoryOk = logFilter.value === '全部' || item['分类'] === logFilter.value
    const moduleOk = moduleFilter.value === '全部模块' || item['模块'] === moduleFilter.value
    const keyword = logKeyword.value.trim().toLowerCase()
    const keywordOk =
      !keyword ||
      [item['时间'], item['模块'], item['分类'], item['级别'], item['内容']]
        .join(' ')
        .toLowerCase()
        .includes(keyword)

    let quickOk = true
    if (logQuickFilter.value === '异常优先') {
      quickOk = item['级别'] === '错误' || item['级别'] === '警告' || item['分类'] === '风控'
    } else if (logQuickFilter.value === '只看风控') {
      quickOk = item['分类'] === '风控' || item['模块'] === '风控'
    } else if (logQuickFilter.value === '只看 CTA') {
      quickOk = item['模块'] === 'CTA' || String(item['内容'] || '').toLowerCase().includes('cta')
    } else if (logQuickFilter.value === '只看网格') {
      quickOk = item['模块'] === '网格' || String(item['内容'] || '').toLowerCase().includes('grid')
    }

    return categoryOk && moduleOk && keywordOk && quickOk
  }),
)
const topLogStats = computed(() => {
  const counts = new Map()
  filteredLogs.value.forEach((item) => {
    counts.set(item['分类'], (counts.get(item['分类']) || 0) + 1)
  })
  return [...counts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 4)
})
const recentAlerts = computed(() => {
  return logs.value
    .filter((item) => {
      if (item['级别'] === '错误' || item['级别'] === '警告') return true
      if (item['分类'] === '风控' || item['分类'] === '订单流' || item['分类'] === '盈亏比拦截') return true
      const content = String(item['内容'] || '').toLowerCase()
      return content.includes('blocked=true') || content.includes('flash crash') || content.includes('cta:open_')
    })
    .slice()
    .reverse()
    .slice(0, 12)
})
const alertSummary = computed(() => {
  const summary = { errors: 0, warnings: 0, risk: 0, blocked: 0 }
  recentAlerts.value.forEach((item) => {
    if (item['级别'] === '错误') summary.errors += 1
    if (item['级别'] === '警告') summary.warnings += 1
    if (item['分类'] === '风控') summary.risk += 1
    if (String(item['内容'] || '').includes('blocked=True')) summary.blocked += 1
  })
  return summary
})
const activityBars = computed(() => {
  const list = timeline.value?.activity || []
  const max = Math.max(1, ...list.map((item) => item.count || 0))
  return list.map((item) => ({
    ...item,
    height: `${Math.max(10, Math.round(((item.count || 0) / max) * 100))}%`,
  }))
})
const recentActivityBars = computed(() => (activityBars.value || []).slice(-12))
const equityPoints = computed(() => timeline.value?.equityPoints || [])
const equityChartStats = computed(() => {
  const list = equityPoints.value
  if (!list.length) {
    return {
      latestEquity: '--',
      latestDailyPnl: '--',
      latestMarginRatio: '--',
      latestDrawdown: '--',
      latestPositionNotional: '--',
      latestOpenOrderNotional: '--',
      latestTotalNotional: '--',
      blockedCount: 0,
    }
  }
  const last = list[list.length - 1]
  return {
    latestEquity: formatNumber(last.equity),
    latestDailyPnl: signedNumber(last.daily_pnl),
    latestMarginRatio: `${formatNumber(last.margin_ratio_pct)}%`,
    latestDrawdown: `${formatNumber(last.drawdown_pct)}%`,
    latestPositionNotional: formatNumber(last.position_notional),
    latestOpenOrderNotional: formatNumber(last.open_order_notional),
    latestTotalNotional: formatNumber(last.total_notional),
    blockedCount: list.filter((item) => item.blocked).length,
  }
})
const equityChart = computed(() => buildEquityChart(equityPoints.value))
const ctaTopFamilies = computed(() => ctaDashboard.value?.leaderboards?.all || [])
const ctaLongFamilies = computed(() => ctaDashboard.value?.leaderboards?.long || [])
const ctaShortFamilies = computed(() => ctaDashboard.value?.leaderboards?.short || [])
const ctaRegimeMatrix = computed(() => ctaDashboard.value?.regime_matrix || [])
const ctaRegimeTransitions = computed(() => ctaDashboard.value?.regime_transitions || [])
const ctaFamilyScoreTimeseries = computed(() => ctaDashboard.value?.family_score_timeseries || [])
const ctaFamilyRegimeActions = computed(() => ctaDashboard.value?.family_regime_actions || [])
const ctaMissedRows = computed(() => ctaDashboard.value?.decision_audit?.missed_opportunities || [])
const ctaBadReleaseRows = computed(() => ctaDashboard.value?.decision_audit?.bad_releases || [])
const ctaSuggestions = computed(() => ctaDashboard.value?.suggestions || [])
const ctaTuningSnapshot = computed(() => ctaDashboard.value?.tuningSnapshot || {})
const ctaOverviewCards = computed(() => {
  const overviewData = ctaDashboard.value?.overview || {}
  return [
    { label: 'Family 总数', value: overviewData.family_count ?? 0, hint: '当前纳入统计的 trigger family 数量' },
    { label: 'Long Family', value: overviewData.long_family_count ?? 0, hint: '做多 family 样本' },
    { label: 'Short Family', value: overviewData.short_family_count ?? 0, hint: '做空 family 样本' },
    { label: '最近平仓', value: overviewData.recent_close_count ?? 0, hint: '用于适配学习的 recent close 样本' },
    { label: '最近阻断', value: overviewData.recent_blocked_count ?? 0, hint: '最近 blocked_signal 数量' },
  ]
})
const ctaLeaderboardChart = computed(() => buildBarChart(ctaTopFamilies.value.slice(0, 8), (row) => Number(row.score || 0), (row) => row.trigger_family))
const ctaLongWinrateChart = computed(() => buildBarChart(ctaLongFamilies.value.slice(0, 8), (row) => Number(row.win_rate || 0), (row) => row.trigger_family))
const ctaShortWinrateChart = computed(() => buildBarChart(ctaShortFamilies.value.slice(0, 8), (row) => Number(row.win_rate || 0), (row) => row.trigger_family))
const ctaRegimeHeatRows = computed(() => (ctaRegimeMatrix.value || []).slice(0, 12))
const ctaRegimeFamilies = computed(() => {
  const set = new Set()
  ctaRegimeMatrix.value.forEach((row) => set.add(String(row.trigger_family || '--')))
  return [...set]
})
const ctaRegimeNames = computed(() => {
  const set = new Set()
  ctaRegimeMatrix.value.forEach((row) => set.add(String(row.market_regime || 'unknown')))
  return [...set]
})
const ctaRegimeHeatMatrix = computed(() => {
  const families = ctaRegimeFamilies.value
  const regimes = ctaRegimeNames.value
  const source = ctaRegimeMatrix.value || []

  const modeValue = (bucket) => {
    if (ctaHeatmapMode.value === 'long') return Number(bucket.longPnl || 0)
    if (ctaHeatmapMode.value === 'short') return Number(bucket.shortPnl || 0)
    if (ctaHeatmapMode.value === 'net') return Number(bucket.netPnl || 0)
    return Number(bucket.biasPnl || 0)
  }

  const buckets = families.map((family) => ({
    family,
    cells: regimes.map((regime) => {
      const matched = source.filter((row) => String(row.trigger_family || '--') === family && String(row.market_regime || 'unknown') === regime)
      const longRows = matched.filter((row) => String(row.side || '').toLowerCase() === 'long')
      const shortRows = matched.filter((row) => String(row.side || '').toLowerCase() === 'short')
      const longTrades = longRows.reduce((sum, row) => sum + (Number(row.trade_count) || 0), 0)
      const shortTrades = shortRows.reduce((sum, row) => sum + (Number(row.trade_count) || 0), 0)
      const longPnl = longRows.reduce((sum, row) => sum + (Number(row.total_pnl) || 0), 0)
      const shortPnl = shortRows.reduce((sum, row) => sum + (Number(row.total_pnl) || 0), 0)
      const netPnl = longPnl + shortPnl
      const biasPnl = longPnl - shortPnl
      const longWinBase = longRows.reduce((sum, row) => sum + (Number(row.win_rate) || 0) * (Number(row.trade_count) || 0), 0)
      const shortWinBase = shortRows.reduce((sum, row) => sum + (Number(row.win_rate) || 0) * (Number(row.trade_count) || 0), 0)
      const totalTrades = longTrades + shortTrades
      const totalWinBase = longWinBase + shortWinBase
      const netWinRate = totalTrades ? totalWinBase / totalTrades : 0
      return {
        regime,
        longTrades,
        shortTrades,
        totalTrades,
        longPnl,
        shortPnl,
        netPnl,
        biasPnl,
        netWinRate,
      }
    }),
  }))

  const flattened = buckets.flatMap((row) => row.cells)
  const maxAbsValue = Math.max(1e-9, ...flattened.map((cell) => Math.abs(modeValue(cell))))
  const rows = buckets
    .map((row) => {
      const familyBiasAbs = Math.max(...row.cells.map((cell) => Math.abs(cell.biasPnl)), 0)
      const familyNet = row.cells.reduce((sum, cell) => sum + Number(cell.netPnl || 0), 0)
      const familyLong = row.cells.reduce((sum, cell) => sum + Number(cell.longPnl || 0), 0)
      const familyShort = row.cells.reduce((sum, cell) => sum + Number(cell.shortPnl || 0), 0)
      return {
        family: row.family,
        familyBiasAbs,
        familyNet,
        familyLong,
        familyShort,
        cells: row.cells.map((cell) => {
          const value = modeValue(cell)
          const intensity = Math.min(1, Math.abs(value) / maxAbsValue)
          return {
            ...cell,
            value,
            intensity,
            tone: value > 0 ? 'emerald' : value < 0 ? 'rose' : 'slate',
          }
        }),
      }
    })
    .sort((a, b) => {
      if (ctaHeatmapSortMode.value === 'net') return Math.abs(b.familyNet) - Math.abs(a.familyNet)
      if (ctaHeatmapSortMode.value === 'long') return Math.abs(b.familyLong) - Math.abs(a.familyLong)
      if (ctaHeatmapSortMode.value === 'short') return Math.abs(b.familyShort) - Math.abs(a.familyShort)
      return b.familyBiasAbs - a.familyBiasAbs
    })

  return { families, regimes, rows, maxAbsValue }
})
const ctaFamilyRegimeBands = computed(() => {
  const rows = ctaActiveFamilySeries.value || []
  if (!rows.length) return []
  const total = Math.max(rows.length - 1, 1)
  const width = 520
  const padding = 18
  const bands = []
  let start = 0
  let current = String(rows[0]?.market_regime || 'unknown')
  const pushBand = (endIndex, regime) => {
    const x1 = padding + (start * (width - padding * 2)) / total
    const x2 = padding + (endIndex * (width - padding * 2)) / total
    bands.push({ regime, x: x1, width: Math.max(6, x2 - x1) })
  }
  rows.forEach((item, index) => {
    const regime = String(item?.market_regime || 'unknown')
    if (regime !== current) {
      pushBand(Math.max(index - 1, start), current)
      start = index
      current = regime
    }
  })
  pushBand(rows.length - 1, current)
  return bands
})
const ctaFamilyOptions = computed(() => {
  const catalog = Array.isArray(ctaDashboard.value?.family_catalog) ? ctaDashboard.value.family_catalog : []
  const options = []
  const seen = new Set()
  catalog.forEach((item) => {
    const family = String(item || '').trim()
    if (!family || seen.has(family)) return
    seen.add(family)
    options.push(family)
  })
  ;(ctaTopFamilies.value || []).forEach((row) => {
    const family = String(row.trigger_family || '').trim()
    if (!family || seen.has(family)) return
    seen.add(family)
    options.push(family)
  })
  ;(ctaFamilyScoreTimeseries.value || []).forEach((point) => {
    const family = String(point?.family || '').trim()
    if (!family || seen.has(family)) return
    seen.add(family)
    options.push(family)
  })
  return options
})
const ctaActiveFamily = computed(() => {
  if (ctaSelectedFamily.value && ctaFamilyOptions.value.includes(ctaSelectedFamily.value)) return ctaSelectedFamily.value
  return ctaFamilyOptions.value[0] || ''
})
const ctaActiveFamilySeries = computed(() => {
  const family = ctaActiveFamily.value
  if (!family) return []
  return (ctaFamilyScoreTimeseries.value || []).filter((item) => String(item?.family || '') === family)
})
const ctaActiveFamilyMeta = computed(() => {
  const family = ctaActiveFamily.value
  const boardRow = (ctaTopFamilies.value || []).find((item) => item.trigger_family === family)
  const series = ctaActiveFamilySeries.value
  const latestPoint = series[series.length - 1] || null
  const latestCumPnl = latestPoint ? Number(latestPoint.cum_pnl || 0) : 0
  const latestTradePnl = latestPoint ? Number(latestPoint.trade_pnl || 0) : 0
  const latestRollingWr = latestPoint ? Number(latestPoint.rolling_wr || 0) : 0
  return { family, boardRow, latestCumPnl, latestTradePnl, latestRollingWr, points: series.length }
})
const ctaFamilyTrendChart = computed(() => {
  const family = ctaActiveFamily.value
  const rows = ctaActiveFamilySeries.value || []
  const selector = (item) => {
    if (ctaFamilyTrendMode.value === 'trade_pnl') return Number(item.trade_pnl || 0)
    if (ctaFamilyTrendMode.value === 'rolling_wr') return Number(item.rolling_wr || 0)
    return Number(item.cum_pnl || 0)
  }
  const color = ctaFamilyTrendMode.value === 'rolling_wr' ? '#8b5cf6' : ctaFamilyTrendMode.value === 'trade_pnl' ? '#f97316' : '#0ea5e9'
  const metricLabel = ctaFamilyTrendMode.value === 'rolling_wr' ? 'WR 滚动' : ctaFamilyTrendMode.value === 'trade_pnl' ? '单笔 PnL' : '累计 PnL'
  if (!family) return { family: '', color, metricLabel, chart: buildMiniTrendChart([], () => 0) }
  return { family, color, metricLabel, chart: buildMiniTrendChart(rows, selector) }
})
const controlCards = computed(() => {
  if (!overview.value) return []
  return [
    {
      label: '主控进程',
      value: overview.value['主进程运行'] ? '运行中' : '未运行',
      hint: Array.isArray(overview.value['主进程PID']) && overview.value['主进程PID'].length ? `PID ${overview.value['主进程PID'].join(', ')}` : '暂无进程号',
      tone: overview.value['主进程运行'] ? 'emerald' : 'slate',
    },
    {
      label: 'CTA 状态',
      value: overview.value['CTA状态'] || '--',
      hint: `交易对 ${overview.value['交易对'] || '--'}`,
      tone: String(overview.value['CTA状态'] || '').includes('拦截') ? 'amber' : 'sky',
    },
    {
      label: '网格状态',
      value: overview.value['网格状态'] || '--',
      hint: `总名义价值 ${formatNumber(overview.value['总名义价值'])}`,
      tone: String(overview.value['网格状态'] || '').includes('未激活') ? 'slate' : 'sky',
    },
    {
      label: '风控阻断',
      value: overview.value['风控阻断'] ? '已阻断' : '正常',
      hint: `保证金率 ${percentText(overview.value['保证金率'])}`,
      tone: overview.value['风控阻断'] ? 'rose' : 'emerald',
    },
  ]
})
const controlHints = computed(() => {
  if (!overview.value) return []
  const hints = []
  const marginRatio = Number(overview.value['保证金率'])
  const totalNotional = Number(overview.value['总名义价值'])
  const blocked = Boolean(overview.value['风控阻断'])
  if (blocked) {
    hints.push({ title: '风控当前阻断新开仓', level: 'danger', detail: '建议优先检查风险心跳、保证金率和订单侧曝险。' })
  }
  if (Number.isFinite(marginRatio) && marginRatio >= 0.35) {
    hints.push({ title: '保证金率偏高', level: marginRatio >= 0.6 ? 'danger' : 'warn', detail: `当前保证金率 ${percentText(marginRatio)}。` })
  }
  if (Number.isFinite(totalNotional) && totalNotional > 0) {
    hints.push({ title: '系统存在实际曝险', level: 'info', detail: `当前总名义价值 ${formatNumber(totalNotional)}。` })
  }
  if (!hints.length) {
    hints.push({ title: '当前控制面稳定', level: 'ok', detail: '未发现明显异常，可继续观察。' })
  }
  return hints
})
const controlWatchRows = computed(() => {
  if (!overview.value) return []
  return [
    { label: '账户权益', value: formatNumber(overview.value['账户权益']), tone: metricToneByValue(overview.value['未实现盈亏']) },
    { label: '未实现盈亏', value: signedNumber(overview.value['未实现盈亏']), tone: metricToneByValue(overview.value['未实现盈亏']) },
    { label: '保证金率', value: percentText(overview.value['保证金率']), tone: metricToneByRisk(overview.value['保证金率']) },
    { label: '持仓名义价值', value: formatNumber(overview.value['持仓名义价值']), tone: 'sky' },
    { label: '委托名义价值', value: formatNumber(overview.value['委托名义价值'] ?? overview.value['挂单名义价值']), tone: 'sky' },
    { label: '总名义价值', value: formatNumber(overview.value['总名义价值']), tone: 'sky' },
    { label: '风控阻断', value: overview.value['风控阻断'] ? '已阻断' : '正常', tone: overview.value['风控阻断'] ? 'rose' : 'emerald' },
    { label: '主控 PID', value: Array.isArray(overview.value['主进程PID']) && overview.value['主进程PID'].length ? overview.value['主进程PID'].join(', ') : '--', tone: overview.value['主进程运行'] ? 'emerald' : 'slate' },
  ]
})
const workerHealthRows = computed(() => {
  if (!bots.value) return []
  return [bots.value['主控'], bots.value['CTA'], bots.value['网格'], bots.value['风控'], bots.value['市场判定']]
    .filter(Boolean)
    .map((item) => ({
      name: item['名称'],
      time: item['时间'],
      level: item['级别'],
      summary: summarizeBotContent(item['内容']),
      freshness: relativeTimeText(item['时间']),
    }))
})
const controlPulseRows = computed(() => [
  { label: '轮询状态', value: autoRefreshStatusText.value, hint: autoRefreshEnabled.value ? '控制页保持后台拉取' : '当前为手动刷新模式' },
  { label: '最近成功刷新', value: lastSuccessfulRefreshText.value, hint: refreshFailureCount.value ? `已连续失败 ${refreshFailureCount.value} 次` : '最近轮询正常' },
  { label: '重点告警数', value: `${recentAlerts.value.length}`, hint: `错误 ${alertSummary.value.errors} / 警告 ${alertSummary.value.warnings} / 风控 ${alertSummary.value.risk}` },
  { label: '最近控制动作', value: actionMessage.value || '暂无动作', hint: '启动 / 停止 / 重启 / 刷新都会写入历史' },
])
const positionSummaryCards = computed(() => {
  const list = positions.value || []
  const totalNotional = list.reduce((sum, item) => sum + (Number(item['名义价值']) || 0), 0)
  const totalPnl = list.reduce((sum, item) => sum + (Number(item['未实现盈亏']) || 0), 0)
  const longCount = list.filter((item) => String(item['方向'] || '').toLowerCase().includes('long')).length
  const shortCount = list.filter((item) => String(item['方向'] || '').toLowerCase().includes('short')).length
  return [
    { label: '持仓笔数', value: `${list.length}`, hint: `多头 ${longCount} / 空头 ${shortCount}`, tone: 'sky' },
    { label: '持仓总名义价值', value: formatNumber(totalNotional), hint: '当前全部持仓合计', tone: 'sky' },
    { label: '未实现盈亏汇总', value: signedNumber(totalPnl), hint: '按当前标记价格汇总', tone: metricToneByValue(totalPnl) },
  ]
})
const orderSummaryCards = computed(() => {
  const list = orders.value || []
  const totalAmount = list.reduce((sum, item) => sum + (Number(item['数量']) || 0), 0)
  const reduceOnlyCount = list.filter((item) => Boolean(item['仅减仓'])).length
  const typeCounts = list.reduce((acc, item) => {
    const key = item['类型'] || '未知'
    acc[key] = (acc[key] || 0) + 1
    return acc
  }, {})
  const topType = Object.entries(typeCounts).sort((a, b) => b[1] - a[1])[0]?.[0] || '--'
  return [
    { label: '委托笔数', value: `${list.length}`, hint: `仅减仓 ${reduceOnlyCount} 笔`, tone: 'sky' },
    { label: '委托总数量', value: formatNumber(totalAmount, 4), hint: '按订单数量字段汇总', tone: 'sky' },
    { label: '主类型', value: topType, hint: '当前最常见委托类型', tone: 'slate' },
  ]
})
const logAuditSummaryCards = computed(() => {
  const list = logTableRows.value || []
  const errors = list.filter((item) => item['级别'] === '错误').length
  const warnings = list.filter((item) => item['级别'] === '警告').length
  const risk = list.filter((item) => item['分类'] === '风控').length
  const blocked = list.filter((item) => String(item['内容'] || '').includes('blocked=True')).length
  return [
    { label: '当前结果数', value: `${list.length}`, hint: '筛选后日志总条数', tone: 'sky' },
    { label: '错误', value: `${errors}`, hint: '当前筛选范围内错误', tone: errors ? 'rose' : 'slate' },
    { label: '警告', value: `${warnings}`, hint: '当前筛选范围内警告', tone: warnings ? 'amber' : 'slate' },
    { label: '风控 / 阻断', value: `${risk} / ${blocked}`, hint: '风控分类与 blocked=True 命中', tone: risk || blocked ? 'amber' : 'slate' },
  ]
})
const logFocusRows = computed(() => {
  const list = logTableRows.value || []
  const latestError = list.find((item) => item['级别'] === '错误')
  const latestWarning = list.find((item) => item['级别'] === '警告')
  const latestRisk = list.find((item) => item['分类'] === '风控')
  return [
    {
      label: '最新错误',
      title: latestError ? `${latestError['时间']} · ${latestError['模块']}` : '--',
      detail: latestError ? latestError['内容'] : '当前筛选范围内无错误',
      tone: latestError ? 'rose' : 'slate',
    },
    {
      label: '最新警告',
      title: latestWarning ? `${latestWarning['时间']} · ${latestWarning['模块']}` : '--',
      detail: latestWarning ? latestWarning['内容'] : '当前筛选范围内无警告',
      tone: latestWarning ? 'amber' : 'slate',
    },
    {
      label: '最新风控事件',
      title: latestRisk ? `${latestRisk['时间']} · ${latestRisk['模块']}` : '--',
      detail: latestRisk ? latestRisk['内容'] : '当前筛选范围内无风控事件',
      tone: latestRisk ? 'orange' : 'slate',
    },
  ]
})
const tradingFocusRows = computed(() => {
  const topPosition = [...(positions.value || [])].sort((a, b) => Math.abs(Number(b['名义价值']) || 0) - Math.abs(Number(a['名义价值']) || 0))[0]
  const topPnl = [...(positions.value || [])].sort((a, b) => Math.abs(Number(b['未实现盈亏']) || 0) - Math.abs(Number(a['未实现盈亏']) || 0))[0]
  const topOrder = [...(orders.value || [])].sort((a, b) => Math.abs(Number(b['数量']) || 0) - Math.abs(Number(a['数量']) || 0))[0]
  return [
    {
      label: '最大持仓曝险',
      title: topPosition ? `${topPosition['交易对']} · ${topPosition['方向']}` : '--',
      detail: topPosition ? `名义价值 ${formatNumber(topPosition['名义价值'])}` : '当前无持仓',
    },
    {
      label: '最大盈亏波动',
      title: topPnl ? `${topPnl['交易对']} · ${signedNumber(topPnl['未实现盈亏'])}` : '--',
      detail: topPnl ? `方向 ${topPnl['方向']} / 名义价值 ${formatNumber(topPnl['名义价值'])}` : '当前无持仓',
    },
    {
      label: '最大挂单',
      title: topOrder ? `${topOrder['交易对']} · ${topOrder['类型']}` : '--',
      detail: topOrder ? `数量 ${formatNumber(topOrder['数量'], 4)} / 价格 ${formatNumber(topOrder['价格'])}` : '当前无委托',
    },
  ]
})
const ctaEventRows = computed(() => (timeline.value?.ctaEvents || []).slice().reverse())
const riskEventRows = computed(() => (timeline.value?.riskEvents || []).slice().reverse())
const sortedPositions = computed(() => sortRows(positions.value, positionSort.value))
const sortedOrders = computed(() => sortRows(orders.value, orderSort.value))
const logTableRows = computed(() => sortRows(filteredLogs.value, logSort.value))
const activeConfigSectionData = computed(() => {
  if (!configSections.value.length) return null
  return configSections.value.find((item) => item.section === activeConfigSection.value) || configSections.value[0]
})
const activeConfigGroups = computed(() => groupedFields(activeConfigSectionData.value?.fields || []))
const activeConfigGroupData = computed(() => {
  if (!activeConfigGroups.value.length) return null
  return activeConfigGroups.value.find((item) => item.name === activeConfigGroup.value) || activeConfigGroups.value[0]
})
const configStats = computed(() => {
  const section = activeConfigSectionData.value
  const groups = activeConfigGroups.value
  const fields = section?.fields || []
  return {
    sectionCount: configSections.value.length,
    groupCount: groups.length,
    fieldCount: fields.length,
    highImpactCount: fields.filter((item) => item.highImpact).length,
  }
})
const financeMonthLabel = computed(() => financeMonth.value.replace('-', ' 年 ') + ' 月')
const financeMonthSummary = computed(() => ({
  totalPnl: signedNumber(financeCalendar.value?.monthTotalPnl, 2),
  totalTone: metricToneByValue(financeCalendar.value?.monthTotalPnl),
  initialEquity: formatNumber(financeCalendar.value?.initialEquity ?? initialEquity.value),
  dayCount: financeCalendar.value?.dayCount || 0,
}))
const financeBaselineStats = computed(() => {
  const baseline = Number(initialEquity.value || 0)
  const equity = Number(overview.value?.['账户权益'] || 0)
  const totalPnl = Number.isFinite(baseline) && Number.isFinite(equity) ? equity - baseline : 0
  const totalPnlRate = baseline > 0 ? (totalPnl / baseline) * 100 : 0
  return {
    totalPnl,
    totalPnlText: signedNumber(totalPnl, 2),
    totalPnlRate,
    totalPnlRateText: `${totalPnlRate >= 0 ? '+' : ''}${formatNumber(totalPnlRate, 2)}%`,
  }
})
const financeCalendarCells = computed(() => {
  const month = financeMonth.value || currentMonthKey()
  const [yearText, monthText] = month.split('-')
  const year = Number(yearText)
  const monthIndex = Number(monthText) - 1
  const firstDay = new Date(year, monthIndex, 1)
  const lastDate = new Date(year, monthIndex + 1, 0).getDate()
  const firstWeekday = (firstDay.getDay() + 6) % 7
  const itemMap = new Map((financeCalendar.value?.items || []).map((item) => [item.date, item]))
  const cells = []
  for (let i = 0; i < firstWeekday; i += 1) cells.push({ empty: true, key: `empty-${i}` })
  for (let day = 1; day <= lastDate; day += 1) {
    const date = `${month}-${String(day).padStart(2, '0')}`
    cells.push({ empty: false, key: date, date, day, item: itemMap.get(date) || null })
  }
  while (cells.length % 7 !== 0) cells.push({ empty: true, key: `tail-${cells.length}` })
  return cells
})

function groupedFields(fields) {
  const groups = new Map()
  ;(fields || []).forEach((field) => {
    const key = field.group || '默认分组'
    if (!groups.has(key)) groups.set(key, [])
    groups.get(key).push(field)
  })
  return [...groups.entries()].map(([name, items]) => ({ name, items }))
}

function setTheme(value) {
  dark.value = value
  localStorage.setItem('admin-theme', value ? 'dark' : 'light')
  document.documentElement.classList.toggle('dark', value)
}

function formatDateTime(value) {
  if (!value) return '--'
  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(String(value))) return String(value)
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return String(value)
  const pad = (num) => String(num).padStart(2, '0')
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`
}

function currentMonthKey() {
  const now = new Date()
  return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`
}

function shiftMonth(month, delta) {
  const [yearText, monthText] = String(month || currentMonthKey()).split('-')
  const date = new Date(Number(yearText), Number(monthText) - 1 + Number(delta || 0), 1)
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`
}

function formatShortTime(value) {
  const full = formatDateTime(value)
  return full === '--' ? full : full.slice(11, 16)
}

function parseDateTime(value) {
  const full = formatDateTime(value)
  if (full === '--') return null
  const normalized = full.replace(' ', 'T')
  const date = new Date(normalized)
  return Number.isNaN(date.getTime()) ? null : date
}

function relativeTimeText(value) {
  const date = parseDateTime(value)
  if (!date) return '--'
  const diffMs = Date.now() - date.getTime()
  const diffMinutes = Math.max(0, Math.floor(diffMs / 60000))
  if (diffMinutes < 1) return '刚刚'
  if (diffMinutes < 60) return `${diffMinutes} 分钟前`
  const diffHours = Math.floor(diffMinutes / 60)
  if (diffHours < 24) return `${diffHours} 小时前`
  const diffDays = Math.floor(diffHours / 24)
  return `${diffDays} 天前`
}

function formatChartTime(value) {
  const full = formatDateTime(value)
  return full === '--' ? full : full.slice(11, 19)
}

function formatNumber(value, digits = 2) {
  if (value === null || value === undefined || value === '') return '--'
  const num = Number(value)
  return Number.isFinite(num) ? num.toFixed(digits) : String(value)
}

function signedNumber(value, digits = 2) {
  const num = Number(value)
  if (!Number.isFinite(num)) return '--'
  return `${num >= 0 ? '+' : ''}${num.toFixed(digits)}`
}

function percentText(value) {
  const num = Number(value)
  return Number.isFinite(num) ? `${(num * 100).toFixed(2)}%` : '--'
}

function metricToneByValue(value) {
  const num = Number(value)
  if (!Number.isFinite(num)) return 'slate'
  if (num > 0) return 'emerald'
  if (num < 0) return 'rose'
  return 'slate'
}

function metricToneByRisk(value) {
  const num = Number(value)
  if (!Number.isFinite(num)) return 'slate'
  if (num >= 0.6) return 'rose'
  if (num >= 0.35) return 'amber'
  return 'emerald'
}

function cardToneClass(tone) {
  return {
    emerald: 'border-emerald-200/80 bg-emerald-50/80 dark:border-emerald-900/60 dark:bg-emerald-950/30',
    rose: 'border-rose-200/80 bg-rose-50/80 dark:border-rose-900/60 dark:bg-rose-950/30',
    amber: 'border-amber-200/80 bg-amber-50/80 dark:border-amber-900/60 dark:bg-amber-950/30',
    sky: 'border-sky-200/80 bg-sky-50/80 dark:border-sky-900/60 dark:bg-sky-950/30',
    slate: 'border-slate-200 bg-white dark:border-slate-800 dark:bg-slate-900',
  }[tone || 'slate']
}

function levelClass(level) {
  if (level === '错误') return 'bg-rose-500/15 text-rose-500 ring-1 ring-rose-500/20'
  if (level === '警告') return 'bg-amber-500/15 text-amber-500 ring-1 ring-amber-500/20'
  return 'bg-sky-500/15 text-sky-500 ring-1 ring-sky-500/20'
}

function categoryClass(category) {
  if (category === '开仓') return 'bg-emerald-500/15 text-emerald-500'
  if (category === '风控') return 'bg-rose-500/15 text-rose-500'
  if (category === '订单流') return 'bg-orange-500/15 text-orange-500'
  if (category === '盈亏比拦截') return 'bg-yellow-500/15 text-yellow-600 dark:text-yellow-300'
  if (category === '信号') return 'bg-violet-500/15 text-violet-500'
  return 'bg-slate-700 text-slate-200'
}

function statusPillClass(text) {
  const value = String(text || '')
  if (value.includes('已开仓') || value.includes('运行中') || value.includes('多头待命') || value.includes('正常')) return 'bg-emerald-500/15 text-emerald-500'
  if (value.includes('拦截') || value.includes('阻断')) return 'bg-amber-500/15 text-amber-500'
  if (value.includes('未激活') || value.includes('未运行')) return 'bg-slate-500/15 text-slate-500'
  return 'bg-sky-500/15 text-sky-500'
}

function pnlClass(value) {
  const num = Number(value)
  if (!Number.isFinite(num)) return 'text-slate-500'
  if (num > 0) return 'text-emerald-600 dark:text-emerald-400'
  if (num < 0) return 'text-rose-600 dark:text-rose-400'
  return 'text-slate-500'
}

function calendarDayShellClass(item) {
  if (!item) return 'border-slate-500/70 bg-stone-100 dark:border-slate-800 dark:bg-slate-900'
  if (item.kind === 'live') return 'border-sky-500 bg-sky-200/95 dark:border-sky-700 dark:bg-sky-950/35'
  const num = Number(item.dailyPnl)
  if (num > 0) return 'border-emerald-500 bg-emerald-200/95 dark:border-emerald-700 dark:bg-emerald-950/35'
  if (num < 0) return 'border-rose-500 bg-rose-200/95 dark:border-rose-700 dark:bg-rose-950/35'
  return 'border-slate-500/70 bg-stone-100 dark:border-slate-800 dark:bg-slate-900'
}

function summarizeBotContent(line) {
  const text = String(line || '')
  if (!text || text === '--') return '--'
  if (text.includes('cta:bullish_ready')) return 'CTA 已进入多头待命'
  if (text.includes('cta:open_long')) return 'CTA 已执行多头开仓'
  if (text.includes('cta:open_short')) return 'CTA 已执行空头开仓'
  if (text.includes('cta:order_flow_blocked')) return 'CTA 被订单流拦截'
  if (text.includes('cta:reward_risk_blocked')) return 'CTA 被盈亏比拦截'
  if (text.includes('skip:inactive')) return '当前未激活'
  if (text.includes('blocked=False')) return '当前未触发风控阻断'
  if (text.includes('blocked=True')) return '当前已触发风控阻断'
  if (text.includes('status=trend')) return '市场当前为趋势状态'
  if (text.includes('status=range')) return '市场当前为震荡状态'
  return text.length > 54 ? `${text.slice(0, 54)}...` : text
}

function summarizeCtaEvent(label) {
  const text = String(label || '')
  if (text.includes('open_long')) return '开多'
  if (text.includes('open_short')) return '开空'
  if (text.includes('bullish_ready')) return '多头待命'
  if (text.includes('bearish_ready')) return '空头待命'
  if (text.includes('order_flow_blocked')) return '订单流拦截'
  if (text.includes('reward_risk_blocked')) return '盈亏比拦截'
  if (text.includes('skip:inactive')) return '未激活'
  return text || '--'
}

function boolText(value) {
  return value ? '是' : '否'
}

function compareValues(a, b) {
  const aNum = Number(a)
  const bNum = Number(b)
  if (Number.isFinite(aNum) && Number.isFinite(bNum)) return aNum - bNum
  return String(a ?? '').localeCompare(String(b ?? ''), 'zh-CN')
}

function sortRows(rows, sortState) {
  const list = [...(rows || [])]
  const { key, direction } = sortState || {}
  if (!key) return list
  list.sort((left, right) => {
    const result = compareValues(left?.[key], right?.[key])
    return direction === 'asc' ? result : -result
  })
  return list
}

function toggleSort(target, key) {
  const state = target?.value ?? target
  if (!state) return
  if (state.key === key) {
    state.direction = state.direction === 'asc' ? 'desc' : 'asc'
    return
  }
  state.key = key
  state.direction = 'desc'
}

function sortIndicator(target, key) {
  const state = target?.value ?? target
  if (!state || state.key !== key) return '↕'
  return state.direction === 'asc' ? '↑' : '↓'
}

function positionSideClass(side) {
  const text = String(side || '').toLowerCase()
  if (text.includes('long')) return 'bg-emerald-500/15 text-emerald-500'
  if (text.includes('short')) return 'bg-rose-500/15 text-rose-500'
  return 'bg-slate-500/15 text-slate-500'
}

function orderTypeClass(type) {
  const text = String(type || '').toLowerCase()
  if (text.includes('limit')) return 'bg-sky-500/15 text-sky-500'
  if (text.includes('market')) return 'bg-violet-500/15 text-violet-500'
  if (text.includes('stop')) return 'bg-amber-500/15 text-amber-500'
  return 'bg-slate-500/15 text-slate-500'
}

function hintLevelClass(level) {
  if (level === 'danger') return 'border-rose-200 bg-rose-50 text-rose-700 dark:border-rose-900/60 dark:bg-rose-950/30 dark:text-rose-300'
  if (level === 'warn') return 'border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-900/60 dark:bg-amber-950/30 dark:text-amber-300'
  if (level === 'info') return 'border-sky-200 bg-sky-50 text-sky-700 dark:border-sky-900/60 dark:bg-sky-950/30 dark:text-sky-300'
  return 'border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-900/60 dark:bg-emerald-950/30 dark:text-emerald-300'
}

function alertRowClass(item) {
  if (item['级别'] === '错误') return 'border-rose-200 bg-rose-50/80 dark:border-rose-900/60 dark:bg-rose-950/25'
  if (item['级别'] === '警告') return 'border-amber-200 bg-amber-50/80 dark:border-amber-900/60 dark:bg-amber-950/25'
  if (item['分类'] === '风控') return 'border-orange-200 bg-orange-50/80 dark:border-orange-900/60 dark:bg-orange-950/25'
  return 'border-slate-200 bg-slate-50/80 dark:border-slate-800 dark:bg-slate-900/60'
}

function alertBadgeClass(item) {
  if (item['级别'] === '错误') return 'bg-rose-500/15 text-rose-600 dark:text-rose-300'
  if (item['级别'] === '警告') return 'bg-amber-500/15 text-amber-600 dark:text-amber-300'
  if (item['分类'] === '风控') return 'bg-orange-500/15 text-orange-600 dark:text-orange-300'
  return 'bg-slate-500/15 text-slate-600 dark:text-slate-300'
}

function viewTabClass(id) {
  return currentView.value === id
    ? 'border border-slate-200 bg-slate-100 text-slate-900 shadow-sm dark:border-slate-700 dark:bg-slate-800 dark:text-slate-100'
    : 'border border-transparent bg-transparent text-slate-500 hover:border-slate-200 hover:bg-slate-50 hover:text-slate-900 dark:text-slate-400 dark:hover:border-slate-700 dark:hover:bg-slate-900/60 dark:hover:text-slate-100'
}

function configSectionTabClass(id) {
  return activeConfigSection.value === id
    ? 'bg-slate-900 text-white shadow-sm dark:bg-slate-100 dark:text-slate-900'
    : 'bg-slate-100 text-slate-600 hover:bg-slate-200 dark:bg-slate-800 dark:text-slate-300 dark:hover:bg-slate-700'
}

function configGroupTabClass(name) {
  return activeConfigGroup.value === name
    ? 'border-sky-500 bg-sky-50 text-sky-700 dark:border-sky-500/70 dark:bg-sky-950/30 dark:text-sky-300'
    : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300 hover:bg-slate-50 dark:border-slate-800 dark:bg-slate-900 dark:text-slate-300 dark:hover:border-slate-700 dark:hover:bg-slate-800'
}

function ensureConfigSelection() {
  if (!configSections.value.length) {
    activeConfigSection.value = ''
    activeConfigGroup.value = ''
    return
  }
  const sectionExists = configSections.value.some((item) => item.section === activeConfigSection.value)
  if (!sectionExists) {
    activeConfigSection.value = configSections.value[0].section
  }
  const section = configSections.value.find((item) => item.section === activeConfigSection.value) || configSections.value[0]
  const groups = groupedFields(section?.fields || [])
  const groupExists = groups.some((item) => item.name === activeConfigGroup.value)
  if (!groupExists) {
    activeConfigGroup.value = groups[0]?.name || ''
  }
}

function selectConfigSection(sectionId) {
  activeConfigSection.value = sectionId
  const section = configSections.value.find((item) => item.section === sectionId)
  const groups = groupedFields(section?.fields || [])
  activeConfigGroup.value = groups[0]?.name || ''
}

function selectConfigGroup(groupName) {
  activeConfigGroup.value = groupName
}

function recordControlAction(type, result, detail = '') {
  controlHistory.value.unshift({
    time: formatDateTime(new Date().toISOString()),
    type,
    result,
    detail,
  })
  controlHistory.value = controlHistory.value.slice(0, 20)
}

function controlResultClass(result) {
  if (result === '成功') return 'bg-emerald-500/15 text-emerald-600 dark:text-emerald-300'
  if (result === '失败') return 'bg-rose-500/15 text-rose-600 dark:text-rose-300'
  return 'bg-slate-500/15 text-slate-600 dark:text-slate-300'
}

function freshnessClass(text) {
  if (text === '刚刚' || String(text).includes('分钟前')) return 'text-emerald-600 dark:text-emerald-400'
  if (String(text).includes('小时前')) return 'text-amber-600 dark:text-amber-400'
  if (String(text).includes('天前')) return 'text-rose-600 dark:text-rose-400'
  return 'text-slate-500 dark:text-slate-400'
}

function buildMiniTrendChart(points, selector) {
  const width = 520
  const height = 180
  const padding = 18
  const valid = (points || []).filter((item) => Number.isFinite(Number(selector(item))))
  if (!valid.length) {
    return { width, height, path: '', area: '', points: [], labels: [] }
  }
  const values = valid.map((item) => Number(selector(item)))
  const min = Math.min(...values)
  const max = Math.max(...values)
  const range = max - min || 1
  const chartPoints = valid.map((item, index) => ({
    raw: item,
    value: Number(selector(item)),
    x: padding + (index * (width - padding * 2)) / Math.max(valid.length - 1, 1),
    y: height - padding - ((Number(selector(item)) - min) / range) * (height - padding * 2),
  }))
  const path = buildLinePath(valid, selector, width, height, padding)
  const area = buildAreaPath(valid, selector, width, height, padding)
  const labels = valid.filter((_, index) => index % Math.max(1, Math.ceil(valid.length / 6)) === 0 || index === valid.length - 1)
  return { width, height, path, area, points: chartPoints, labels }
}

function buildBarChart(rows, selector, labelSelector) {
  const width = 520
  const height = 220
  const padding = 24
  const list = (rows || []).map((row) => ({ label: String(labelSelector(row) || '--'), value: Number(selector(row) || 0) }))
  const maxAbs = Math.max(0.0001, ...list.map((item) => Math.abs(item.value)))
  const innerWidth = width - padding * 2
  const barGap = 10
  const barWidth = list.length ? Math.max(18, (innerWidth - barGap * Math.max(list.length - 1, 0)) / Math.max(list.length, 1)) : 0
  const baseline = height - padding
  const bars = list.map((item, index) => {
    const x = padding + index * (barWidth + barGap)
    const h = Math.max(4, Math.abs(item.value) / maxAbs * (height - padding * 2 - 24))
    const y = item.value >= 0 ? baseline - h : baseline
    return { ...item, x, y, width: barWidth, height: h }
  })
  return { width, height, padding, baseline, bars }
}

function buildLinePath(points, selector, width = 760, height = 220, padding = 22) {
  if (!points.length) return ''
  const values = points.map((item) => Number(selector(item))).filter((value) => Number.isFinite(value))
  if (!values.length) return ''
  const min = Math.min(...values)
  const max = Math.max(...values)
  const range = max - min || 1
  return points
    .map((item, index) => {
      const value = Number(selector(item))
      const x = padding + (index * (width - padding * 2)) / Math.max(points.length - 1, 1)
      const y = height - padding - ((value - min) / range) * (height - padding * 2)
      return `${index === 0 ? 'M' : 'L'} ${x.toFixed(2)} ${y.toFixed(2)}`
    })
    .join(' ')
}

function buildAreaPath(points, selector, width = 760, height = 220, padding = 22) {
  const line = buildLinePath(points, selector, width, height, padding)
  if (!line || !points.length) return ''
  const lastX = padding + ((points.length - 1) * (width - padding * 2)) / Math.max(points.length - 1, 1)
  const baseY = height - padding
  return `${line} L ${lastX.toFixed(2)} ${baseY} L ${padding} ${baseY} Z`
}

function buildEquityChart(points) {
  const width = 760
  const height = 220
  const padding = 22
  const valid = (points || []).filter((item) => Number.isFinite(Number(item.equity)))
  if (!valid.length) {
    return {
      width,
      height,
      labels: [],
      equityPath: '',
      equityArea: '',
      dailyPnlPath: '',
      marginPath: '',
      positionNotionalPath: '',
      openOrderNotionalPath: '',
      totalNotionalPath: '',
      blockedSignalPath: '',
      blockedDots: [],
    }
  }
  const labels = valid.filter((_, index) => index % Math.max(1, Math.ceil(valid.length / 6)) === 0 || index === valid.length - 1)
  const equityPath = buildLinePath(valid, (item) => item.equity, width, height, padding)
  const equityArea = buildAreaPath(valid, (item) => item.equity, width, height, padding)
  const dailyPnlPath = buildLinePath(valid, (item) => item.daily_pnl, width, height, padding)
  const marginPath = buildLinePath(valid, (item) => item.margin_ratio_pct, width, height, padding)
  const positionNotionalPath = buildLinePath(valid, (item) => item.position_notional, width, height, padding)
  const openOrderNotionalPath = buildLinePath(valid, (item) => item.open_order_notional, width, height, padding)
  const totalNotionalPath = buildLinePath(valid, (item) => item.total_notional, width, height, padding)
  const blockedSignalPath = buildLinePath(valid, (item) => item.blocked_value, width, height, padding)
  const blockedDots = valid
    .map((item, index) => {
      if (!item.blocked) return null
      const x = padding + (index * (width - padding * 2)) / Math.max(valid.length - 1, 1)
      return { x, time: item.time }
    })
    .filter(Boolean)
  return {
    width,
    height,
    labels,
    equityPath,
    equityArea,
    dailyPnlPath,
    marginPath,
    positionNotionalPath,
    openOrderNotionalPath,
    totalNotionalPath,
    blockedSignalPath,
    blockedDots,
  }
}

async function api(path, options = {}) {
  const response = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
  })
  if (!response.ok) {
    const text = await response.text()
    const error = new Error(text || `HTTP ${response.status}`)
    error.status = response.status
    throw error
  }
  return response.json()
}

async function login() {
  loginError.value = ''
  loading.value = true
  try {
    const result = await api('/api/auth/login', {
      method: 'POST',
      body: JSON.stringify(loginForm.value),
    })
    token.value = result.token
    username.value = result.username
    localStorage.setItem('admin-token', result.token)
    localStorage.setItem('admin-user', result.username)
    await refreshAll()
    await loadConfigSections()
    startAutoRefresh()
  } catch (error) {
    loginError.value = '登录失败，请检查账号密码'
  } finally {
    loading.value = false
  }
}

function logout(options = {}) {
  const { expired = false } = options
  stopAutoRefresh()
  token.value = ''
  username.value = ''
  overview.value = null
  bots.value = null
  positions.value = []
  orders.value = []
  logs.value = []
  timeline.value = { activity: [], equityPoints: [], ctaEvents: [], riskEvents: [] }
  ctaSelectedFamily.value = ''
  ctaSelectedRegime.value = ''
  ctaHeatmapMode.value = 'bias'
  ctaHeatmapSortMode.value = 'bias_abs'
  ctaFamilyTrendMode.value = 'cum_pnl'
  ctaPresetAudit.value = []
  ctaDashboard.value = { overview: {}, leaderboards: { all: [], long: [], short: [] }, family_catalog: [], regime_matrix: [], decision_audit: { missed_opportunities: [], bad_releases: [] } }
  lastSuccessfulRefreshAt.value = ''
  refreshFailureCount.value = 0
  configSections.value = []
  configBaseline.value = {}
  initialEquity.value = 0
  initialEquityInput.value = ''
  initialEquityMeta.value = { source: '--', updatedAt: '' }
  financeMonth.value = currentMonthKey()
  financeCalendar.value = { month: currentMonthKey(), monthTotalPnl: 0, dayCount: 0, items: [], initialEquity: 0 }
  activeConfigSection.value = ''
  activeConfigGroup.value = ''
  localStorage.removeItem('admin-token')
  localStorage.removeItem('admin-user')
  if (expired) {
    loginError.value = '登录已失效，请重新登录'
    actionMessage.value = '后端已重启或登录态过期，请重新登录。'
  }
}

async function refreshAll(options = {}) {
  if (!token.value) return
  const { silent = false } = options
  loading.value = true
  if (!silent) actionMessage.value = ''
  try {
    const [overviewRes, botsRes, positionsRes, ordersRes, logsRes, timelineRes, initialEquityRes, financeCalendarRes, ctaDashboardRes] = await Promise.all([
      api('/api/dashboard/overview', { headers: authHeaders.value }),
      api('/api/bots/status', { headers: authHeaders.value }),
      api('/api/account/positions', { headers: authHeaders.value }),
      api('/api/account/orders', { headers: authHeaders.value }),
      api('/api/logs/recent?limit=120', { headers: authHeaders.value }),
      api('/api/dashboard/timeline?limit=240', { headers: authHeaders.value }),
      api('/api/account/initial-equity', { headers: authHeaders.value }),
      api(`/api/account/daily-calendar?month=${financeMonth.value}`, { headers: authHeaders.value }),
      api(`/api/dashboard/cta?hours=${ctaWindowHours.value}`, { headers: authHeaders.value }),
    ])
    overview.value = overviewRes
    bots.value = botsRes
    positions.value = positionsRes.items || []
    orders.value = ordersRes.items || []
    logs.value = logsRes.items || []
    timeline.value = timelineRes || { activity: [], equityPoints: [], ctaEvents: [], riskEvents: [] }
    initialEquity.value = Number(initialEquityRes?.initialEquity || 0)
    initialEquityMeta.value = { source: initialEquityRes?.source || '--', updatedAt: initialEquityRes?.updatedAt || '' }
    if (!initialEquityInput.value || !silent) {
      initialEquityInput.value = initialEquity.value ? String(initialEquity.value) : ''
    }
    financeCalendar.value = financeCalendarRes || financeCalendar.value
    ctaDashboard.value = ctaDashboardRes || ctaDashboard.value
    lastSuccessfulRefreshAt.value = new Date().toISOString()
    refreshFailureCount.value = 0
    if (silent) {
      actionMessage.value = '轮询成功'
      recordControlAction('自动刷新', '成功', '后台数据已更新')
    } else {
      recordControlAction('手动刷新', '成功', '后台数据已更新')
    }
  } catch (error) {
    if (error?.status === 401) {
      logout({ expired: true })
      return
    }
    refreshFailureCount.value += 1
    actionMessage.value = silent ? `轮询失败（${refreshFailureCount.value}）` : '刷新失败，请检查后台接口是否正常运行。'
    recordControlAction(silent ? '自动刷新' : '手动刷新', '失败', error?.message || '接口调用失败')
  } finally {
    loading.value = false
  }
}

function stopAutoRefresh() {
  if (autoRefreshTimer.value) {
    clearInterval(autoRefreshTimer.value)
    autoRefreshTimer.value = null
  }
}

function startAutoRefresh() {
  stopAutoRefresh()
  if (!autoRefreshEnabled.value || !token.value) return
  autoRefreshTimer.value = setInterval(() => {
    if (!loading.value) refreshAll({ silent: true })
  }, autoRefreshSeconds.value * 1000)
}

function toggleAutoRefresh() {
  autoRefreshEnabled.value = !autoRefreshEnabled.value
  if (autoRefreshEnabled.value) {
    startAutoRefresh()
    recordControlAction('轮询开关', '成功', `已开启（${autoRefreshSeconds.value}s）`)
  } else {
    stopAutoRefresh()
    recordControlAction('轮询开关', '成功', '已关闭')
  }
}

async function sendSystemAction(path, confirmText, successText) {
  if (!confirm(confirmText)) {
    recordControlAction('系统控制', '取消', confirmText)
    return
  }
  try {
    const result = await api(path, {
      method: 'POST',
      headers: authHeaders.value,
      body: JSON.stringify({ confirm: true }),
    })
    actionMessage.value = result.message || successText
    recordControlAction(path.replace('/api/system/', '').toUpperCase(), '成功', result.message || successText)
    await refreshAll()
  } catch (error) {
    actionMessage.value = '系统控制操作失败。'
    recordControlAction(path.replace('/api/system/', '').toUpperCase(), '失败', error?.message || '系统控制操作失败')
  }
}

async function loadPresetAudit() {
  if (!token.value) return
  try {
    const result = await api('/api/cta/preset-audit', { headers: authHeaders.value })
    ctaPresetAudit.value = result.items || []
  } catch (error) {
    if (error?.status === 401) {
      logout({ expired: true })
    }
  }
}

async function loadConfigSections() {
  if (!token.value) return
  try {
    const result = await api('/api/config/schema', { headers: authHeaders.value })
    configSections.value = result.sections || []
    rebuildConfigBaseline()
    ensureConfigSelection()
  } catch (error) {
    if (error?.status === 401) {
      logout({ expired: true })
      return
    }
    actionMessage.value = '系统配置读取失败。'
  }
}

async function loadInitialEquity() {
  if (!token.value) return
  try {
    const result = await api('/api/account/initial-equity', { headers: authHeaders.value })
    initialEquity.value = Number(result.initialEquity || 0)
    initialEquityInput.value = initialEquity.value ? String(initialEquity.value) : ''
    initialEquityMeta.value = {
      source: result.source || '--',
      updatedAt: result.updatedAt || '',
    }
  } catch (error) {
    if (error?.status === 401) {
      logout({ expired: true })
    }
  }
}

async function loadFinanceCalendar() {
  if (!token.value) return
  try {
    const result = await api(`/api/account/daily-calendar?month=${financeMonth.value}`, { headers: authHeaders.value })
    financeCalendar.value = result || { month: financeMonth.value, monthTotalPnl: 0, dayCount: 0, items: [], initialEquity: initialEquity.value }
    if (!initialEquityInput.value && result?.initialEquity) {
      initialEquity.value = Number(result.initialEquity || 0)
      initialEquityInput.value = String(result.initialEquity)
    }
  } catch (error) {
    if (error?.status === 401) {
      logout({ expired: true })
    }
  }
}

async function changeFinanceMonth(delta) {
  financeMonth.value = shiftMonth(financeMonth.value, delta)
  await loadFinanceCalendar()
}

async function saveInitialEquity() {
  if (!token.value || initialEquitySaving.value) return
  const value = Number(initialEquityInput.value)
  if (!Number.isFinite(value) || value <= 0) {
    actionMessage.value = '初始资金必须是大于 0 的数字。'
    return
  }
  initialEquitySaving.value = true
  try {
    const result = await api('/api/account/initial-equity', {
      method: 'POST',
      headers: authHeaders.value,
      body: JSON.stringify({ initialEquity: value }),
    })
    initialEquity.value = Number(result.initialEquity || value)
    initialEquityInput.value = String(initialEquity.value)
    actionMessage.value = result.message || '初始资金已保存。'
    configSections.value = result.sections || configSections.value
    rebuildConfigBaseline()
    ensureConfigSelection()
    await loadInitialEquity()
    await loadFinanceCalendar()
    await refreshAll({ silent: true })
  } catch (error) {
    if (error?.status === 401) {
      logout({ expired: true })
      return
    }
    actionMessage.value = '初始资金保存失败。'
  } finally {
    initialEquitySaving.value = false
  }
}

function normalizeConfigValue(field) {
  if (field.value === null || field.value === undefined || field.value === '') {
    return field.value
  }
  if (field.type === 'number') {
    const num = Number(field.value)
    return Number.isFinite(num) ? num : field.value
  }
  if (field.type === 'boolean') {
    return Boolean(field.value)
  }
  return field.value
}

function rebuildConfigBaseline() {
  const next = {}
  configSections.value.forEach((section) => {
    ;(section.fields || []).forEach((field) => {
      next[field.path] = normalizeConfigValue(field)
    })
  })
  configBaseline.value = next
}

function buildPresetDiff(values) {
  return Object.entries(values || {}).map(([path, nextValue]) => ({
    path,
    before: configBaseline.value[path],
    after: nextValue,
  }))
}

async function applyCtaPreset(values, successMessage, meta = {}) {
  if (!token.value) return
  const diffRows = buildPresetDiff(values)
  const diffText = diffRows.map((row) => `${row.path}: ${JSON.stringify(row.before)} -> ${JSON.stringify(row.after)}`).join('\n')
  const confirmed = confirm(`确认应用 CTA preset？\n\n${diffText || '（未检测到 diff）'}`)
  if (!confirmed) {
    recordControlAction('CTA 驾驶舱调参', '取消', successMessage)
    return
  }
  try {
    const result = await api('/api/config/save', {
      method: 'POST',
      headers: authHeaders.value,
      body: JSON.stringify({ values }),
    })
    configSections.value = result.sections || configSections.value
    rebuildConfigBaseline()
    ensureConfigSelection()
    actionMessage.value = result.message || successMessage
    recordControlAction('CTA 驾驶舱调参', '成功', `${successMessage}\n${diffText}`)
    const auditItem = {
      time: new Date().toISOString(),
      successMessage,
      meta,
      diffRows,
      review24hAt: new Date(Date.now() + 24 * 3600 * 1000).toISOString(),
      review72hAt: new Date(Date.now() + 72 * 3600 * 1000).toISOString(),
    }
    await api('/api/cta/preset-audit', {
      method: 'POST',
      headers: authHeaders.value,
      body: JSON.stringify(auditItem),
    })
    await loadPresetAudit()
    await refreshAll({ silent: true })
  } catch (error) {
    if (error?.status === 401) {
      logout({ expired: true })
      return
    }
    actionMessage.value = 'CTA 驾驶舱调参失败。'
    recordControlAction('CTA 驾驶舱调参', '失败', error?.message || '接口调用失败')
  }
}

async function rollbackCtaTuning() {
  if (!token.value) return
  try {
    const result = await api('/api/config/rollback', {
      method: 'POST',
      headers: authHeaders.value,
      body: JSON.stringify({ snapshotKey: 'cta_tuning_snapshot::latest' }),
    })
    configSections.value = result.sections || configSections.value
    rebuildConfigBaseline()
    ensureConfigSelection()
    actionMessage.value = result.message || 'CTA 参数已回滚'
    recordControlAction('CTA 驾驶舱回滚', '成功', result.message || 'CTA 参数已回滚')
    await refreshAll({ silent: true })
  } catch (error) {
    if (error?.status === 401) {
      logout({ expired: true })
      return
    }
    actionMessage.value = 'CTA 驾驶舱回滚失败。'
    recordControlAction('CTA 驾驶舱回滚', '失败', error?.message || '接口调用失败')
  }
}

async function saveConfigSections() {
  if (!token.value || configSaving.value) return
  configSaving.value = true
  try {
    const values = {}
    configSections.value.forEach((section) => {
      ;(section.fields || []).forEach((field) => {
        const normalized = normalizeConfigValue(field)
        const baseline = configBaseline.value[field.path]
        if (JSON.stringify(normalized) !== JSON.stringify(baseline)) {
          values[field.path] = normalized
        }
      })
    })
    if (!Object.keys(values).length) {
      actionMessage.value = '没有检测到配置变更，无需保存。'
      return
    }
    const result = await api('/api/config/save', {
      method: 'POST',
      headers: authHeaders.value,
      body: JSON.stringify({ values }),
    })
    configSections.value = result.sections || configSections.value
    rebuildConfigBaseline()
    ensureConfigSelection()
    actionMessage.value = result.message || '系统配置已保存。'
    if (Object.prototype.hasOwnProperty.call(values, 'runtime.account_initial_equity')) {
      await loadInitialEquity()
      await loadFinanceCalendar()
      await refreshAll({ silent: true })
    }
  } catch (error) {
    if (error?.status === 401) {
      logout({ expired: true })
      return
    }
    actionMessage.value = '系统配置保存失败。'
  } finally {
    configSaving.value = false
  }
}

async function startSystem() {
  await sendSystemAction('/api/system/start', '确认启动主控进程？', '已发出启动指令。')
}

async function stopSystem() {
  await sendSystemAction('/api/system/stop', '确认停止主控进程？', '已发出停止指令。')
}

async function restartSystem() {
  await sendSystemAction('/api/system/restart', '确认重启主控进程？', '已发出重启指令。')
}

onMounted(async () => {
  setTheme(dark.value)
  if (token.value) {
    await refreshAll()
    await loadConfigSections()
    await loadPresetAudit()
    startAutoRefresh()
  }
})

onBeforeUnmount(() => {
  stopAutoRefresh()
})
</script>

<template>
  <div class="min-h-screen bg-slate-100 text-slate-900 dark:bg-[#0b1120] dark:text-slate-100">
    <header class="border-b border-slate-200 bg-white/95 backdrop-blur dark:border-slate-800 dark:bg-slate-950/90">
      <div class="flex w-full items-center justify-between gap-4 px-6 py-2.5">
        <div class="flex min-w-0 items-center gap-4">
          <div class="hidden h-8 w-8 items-center justify-center rounded-md bg-slate-900 text-[11px] font-semibold text-white dark:flex">MA</div>
          <div class="min-w-0">
            <div class="text-[11px] font-semibold tracking-[0.16em] text-slate-400">MARKET-ADAPTIVE 交易后台</div>
            <div class="mt-0.5 flex flex-wrap items-center gap-3">
              <h1 class="text-base font-semibold tracking-tight">交易系统管理台</h1>
              <span class="hidden rounded-full bg-slate-100 px-2.5 py-1 text-[11px] font-medium text-slate-500 xl:inline-flex dark:bg-slate-800 dark:text-slate-300">专业管理后台布局</span>
            </div>
          </div>
        </div>
        <div class="flex items-center gap-2.5">
          <div class="hidden items-center gap-2 xl:flex">
            <span class="result-badge">{{ autoRefreshStatusText }}</span>
            <span class="result-badge">{{ latestRefreshTime }}</span>
          </div>
          <button class="rounded-md border border-slate-300 px-3.5 py-2 text-[13px] font-medium transition hover:bg-slate-100 dark:border-slate-700 dark:hover:bg-slate-800" @click="setTheme(!dark)">{{ themeLabel }}</button>
          <button v-if="isAuthed" class="rounded-md border border-slate-300 px-3.5 py-2 text-[13px] font-medium transition hover:bg-slate-100 dark:border-slate-700 dark:hover:bg-slate-800" @click="logout">退出登录</button>
        </div>
      </div>
    </header>

    <main class="w-full px-0 py-0">
      <section v-if="!isAuthed" class="mx-auto max-w-md rounded-lg border border-slate-200 bg-white p-7 shadow-sm dark:border-slate-800 dark:bg-slate-900">
        <h2 class="text-xl font-semibold">管理员登录</h2>
        <p class="mt-1.5 text-[12px] text-slate-500 dark:text-slate-400">先做内网管理够用了，后面再补更严谨的账号体系。</p>
        <div class="mt-6 space-y-4">
          <div>
            <label class="mb-2 block text-[13px] font-medium">账号</label>
            <input v-model="loginForm.username" class="w-full rounded-2xl border border-slate-300 bg-white px-4 py-3 outline-none focus:border-slate-500 dark:border-slate-700 dark:bg-slate-950" />
          </div>
          <div>
            <label class="mb-2 block text-[13px] font-medium">密码</label>
            <input v-model="loginForm.password" type="password" class="w-full rounded-2xl border border-slate-300 bg-white px-4 py-3 outline-none focus:border-slate-500 dark:border-slate-700 dark:bg-slate-950" />
          </div>
          <button class="w-full rounded-lg bg-slate-900 px-4 py-3 text-[13px] font-medium text-white transition hover:bg-slate-700 dark:bg-slate-100 dark:text-slate-900 dark:hover:bg-slate-300" :disabled="loading" @click="login">{{ loading ? '登录中...' : '登录管理台' }}</button>
          <p v-if="loginError" class="text-sm text-rose-500">{{ loginError }}</p>
        </div>
      </section>

      <template v-else>
        <div class="grid min-h-[calc(100vh-61px)] grid-cols-1 xl:grid-cols-[248px,minmax(0,1fr)]">
          <aside class="hidden xl:block">
            <div class="sticky top-0 h-[calc(100vh-57px)] rounded-none border-r border-slate-200 bg-white px-4 py-5 shadow-none dark:border-slate-800 dark:bg-[#0f172a]">
              <div class="flex items-center gap-3 px-2 pb-5">
                <div class="flex h-9 w-9 items-center justify-center rounded-md bg-slate-900 text-[11px] font-semibold text-white dark:bg-sky-500">MA</div>
                <div>
                  <div class="text-[12.5px] font-semibold dark:text-slate-100">交易后台</div>
                  <div class="text-[11px] text-slate-400">Market-Adaptive</div>
                </div>
              </div>
              <div class="px-2 text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-400">主视图</div>
              <nav class="mt-3 space-y-1.5">
                <button v-for="item in viewTabs" :key="item.id" :class="['sidebar-nav-button', viewTabClass(item.id)]" @click="currentView = item.id">{{ item.label }}</button>
              </nav>
              <div class="mt-6 rounded-lg border border-slate-200 bg-slate-50 p-3 dark:border-slate-800 dark:bg-slate-900/40">
                <div class="text-[11px] text-slate-400">状态摘要</div>
                <div class="mt-2 space-y-2">
                  <div class="flex items-center justify-between text-[12px]"><span class="text-slate-400">轮询</span><span class="font-medium dark:text-slate-100">{{ autoRefreshStatusText }}</span></div>
                  <div class="flex items-center justify-between text-[12px]"><span class="text-slate-400">最近成功</span><span class="font-medium dark:text-slate-100">{{ lastSuccessfulRefreshText }}</span></div>
                  <div class="flex items-center justify-between text-[12px]"><span class="text-slate-400">失败次数</span><span class="font-medium dark:text-slate-100">{{ refreshFailureCount }}</span></div>
                </div>
              </div>
            </div>
          </aside>

          <div class="min-w-0 px-6 py-5">
        <section v-if="currentView === 'overview'" id="overview" class="elevated-panel p-3.5">
          <div class="mb-3 flex flex-wrap items-center justify-between gap-3 border-b border-slate-200 pb-3 dark:border-slate-800">
            <div class="flex flex-wrap gap-2 xl:hidden">
              <button v-for="item in viewTabs" :key="`top-${item.id}`" :class="['rounded-full px-3 py-1.5 text-xs font-medium transition', viewTabClass(item.id)]" @click="currentView = item.id">{{ item.label }}</button>
            </div>
            <div class="flex flex-wrap items-center gap-2">
              <button class="rounded-md border border-slate-300 px-3.5 py-2 text-[13px] font-medium transition hover:bg-slate-100 dark:border-slate-700 dark:hover:bg-slate-800" :disabled="loading" @click="refreshAll()">刷新数据</button>
              <button class="rounded-md border border-slate-300 px-3.5 py-2 text-[13px] font-medium transition hover:bg-slate-100 dark:border-slate-700 dark:hover:bg-slate-800" @click="toggleAutoRefresh">{{ autoRefreshEnabled ? '关闭轮询' : '开启轮询' }}</button>
            </div>
          </div>
          <div class="grid gap-3 xl:grid-cols-[1.3fr,0.7fr]">
            <div class="page-hero">
              <div class="flex items-start justify-between gap-4">
                <div class="min-w-0">
                  <div class="text-[11px] font-medium uppercase tracking-[0.18em] text-slate-400">Dashboard</div>
                  <div class="mt-2 text-2xl font-semibold tracking-tight">控制台首页</div>
                  <div class="mt-1 text-[12px] text-slate-500 dark:text-slate-400">欢迎回来，{{ username }}。统一查看账户、策略、风险、日志与系统控制。</div>
                </div>
                <div class="hidden rounded-lg border border-slate-200 bg-white px-3 py-2 text-right dark:border-slate-800 dark:bg-slate-900 xl:block">
                  <div class="text-[11px] text-slate-400">最新刷新</div>
                  <div class="mt-1 text-[12.5px] font-medium">{{ latestRefreshTime }}</div>
                </div>
              </div>
              <div class="mt-4 grid gap-2 sm:grid-cols-2 xl:grid-cols-5">
                <div v-for="item in statusStrip" :key="item.label" class="metric-tile flex min-h-[110px] flex-col justify-between px-3 py-3.5">
                  <div class="text-[20px] tracking-tight text-slate-400">{{ item.label }}</div>
                  <div :class="['mt-3 text-[17px] font-semibold leading-6', statusPillClass(item.value).includes('emerald') ? 'text-emerald-600 dark:text-emerald-400' : statusPillClass(item.value).includes('amber') ? 'text-amber-600 dark:text-amber-400' : statusPillClass(item.value).includes('slate') ? 'text-slate-500 dark:text-slate-400' : 'text-sky-600 dark:text-sky-400']">{{ item.value }}</div>
                </div>
              </div>
            </div>
            <div class="grid gap-2.5 md:grid-cols-2 xl:grid-cols-1">
              <div class="grid gap-2 sm:grid-cols-2 xl:grid-cols-1">
                <div class="soft-panel p-3">
                  <div class="text-[11px] text-slate-400">轮询状态</div>
                  <div class="mt-1 text-[13px] font-medium">{{ autoRefreshStatusText }}</div>
                </div>
                <div class="soft-panel p-3">
                  <div class="text-[11px] text-slate-400">最近动作</div>
                  <div class="mt-1 truncate text-[13px] font-medium">{{ actionMessage || '暂无控制动作' }}</div>
                </div>
              </div>
              <div class="soft-panel p-3">
                <div class="mb-2 flex items-center justify-between gap-2">
                  <div class="text-[11px] text-slate-400">主控操作</div>
                  <div class="text-[10px] uppercase tracking-[0.14em] text-slate-300 dark:text-slate-500">Actions</div>
                </div>
                <div class="grid grid-cols-3 gap-2">
                  <button class="rounded-lg bg-emerald-600 px-2.5 py-2 text-[12.5px] font-medium text-white transition hover:bg-emerald-700" @click="startSystem">启动</button>
                  <button class="rounded-lg bg-amber-500 px-2.5 py-2 text-[12.5px] font-medium text-white transition hover:bg-amber-600" @click="restartSystem">重启</button>
                  <button class="rounded-lg bg-rose-600 px-2.5 py-2 text-[12.5px] font-medium text-white transition hover:bg-rose-700" @click="stopSystem">停止</button>
                </div>
              </div>
            </div>
          </div>

          <p v-if="actionMessage" class="mt-3 text-[13px]" :class="actionMessage.includes('失败') ? 'text-rose-500' : 'text-emerald-600 dark:text-emerald-400'">{{ actionMessage }}</p>
        </section>

        <section v-if="currentView === 'overview'" class="mt-4 grid gap-2.5 lg:grid-cols-2 xl:grid-cols-4">
          <div v-for="item in metricCards" :key="item.label" :class="['rounded-lg border p-3.5 shadow-sm', cardToneClass(item.tone)]">
            <div class="text-[12px] text-slate-500 dark:text-slate-400">{{ item.label }}</div>
            <div class="mt-1 text-lg font-semibold tracking-tight">{{ item.value }}</div>
            <div class="mt-1.5 text-[12px] text-slate-500 dark:text-slate-400">{{ item.sub }}</div>
          </div>
        </section>

        <section v-if="currentView === 'overview'" id="trend" class="mt-5 grid gap-4 xl:grid-cols-[1.25fr,0.75fr]">
          <div class="elevated-panel p-3.5">
            <div class="flex items-center justify-between gap-4">
              <div>
                <h3 class="panel-title !mt-0">资金 / 风控走势</h3>
                <p class="mt-1 text-[13px] text-slate-500 dark:text-slate-400">基于风险心跳抽出的真实权益、今日盈亏与保证金率轨迹。</p>
              </div>
              <div class="text-[11px] text-slate-400">最近 {{ equityPoints.length }} 个风险采样点</div>
            </div>

            <div class="mt-4 grid gap-3 md:grid-cols-4 xl:grid-cols-8">
              <div class="metric-tile p-3.5">
                <div class="text-[11px] text-slate-400">最新权益</div>
                <div class="mt-1 text-lg font-semibold tracking-tight">{{ equityChartStats.latestEquity }}</div>
              </div>
              <div class="metric-tile p-3.5">
                <div class="text-[11px] text-slate-400">今日盈亏</div>
                <div :class="['mt-1 text-lg font-semibold tracking-tight', pnlClass(equityPoints[equityPoints.length - 1]?.daily_pnl)]">{{ equityChartStats.latestDailyPnl }}</div>
              </div>
              <div class="metric-tile p-3.5">
                <div class="text-[11px] text-slate-400">保证金率</div>
                <div class="mt-1 text-lg font-semibold tracking-tight">{{ equityChartStats.latestMarginRatio }}</div>
              </div>
              <div class="metric-tile p-3.5">
                <div class="text-[11px] text-slate-400">持仓名义价值</div>
                <div class="mt-1 text-lg font-semibold tracking-tight">{{ equityChartStats.latestPositionNotional }}</div>
              </div>
              <div class="metric-tile p-3.5">
                <div class="text-[11px] text-slate-400">委托名义价值</div>
                <div class="mt-1.5 text-xl font-semibold tracking-tight">{{ equityChartStats.latestOpenOrderNotional }}</div>
              </div>
              <div class="metric-tile p-3.5">
                <div class="text-[11px] text-slate-400">总名义价值</div>
                <div class="mt-1.5 text-xl font-semibold tracking-tight">{{ equityChartStats.latestTotalNotional }}</div>
              </div>
              <div class="metric-tile p-3.5">
                <div class="text-[11px] text-slate-400">当前回撤</div>
                <div class="mt-1.5 text-xl font-semibold tracking-tight">{{ equityChartStats.latestDrawdown }}</div>
              </div>
              <div class="metric-tile p-3.5">
                <div class="text-[11px] text-slate-400">阻断次数</div>
                <div class="mt-1.5 text-xl font-semibold tracking-tight">{{ equityChartStats.blockedCount }}</div>
              </div>
            </div>

            <div v-if="equityPoints.length" class="mt-4 grid gap-4 xl:grid-cols-2">
              <div class="overflow-hidden rounded-2xl border border-slate-200 bg-slate-50 p-4 dark:border-slate-800 dark:bg-slate-950/60">
                <div class="mb-3 flex items-center justify-between gap-3">
                  <div class="text-[12.5px] font-medium">资金 / 风控主图</div>
                  <div class="text-[11px] text-slate-400">权益 / 今日盈亏 / 保证金率</div>
                </div>
                <svg :viewBox="`0 0 ${equityChart.width} ${equityChart.height}`" class="h-64 w-full">
                  <defs>
                    <linearGradient id="equity-fill" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stop-color="#38bdf8" stop-opacity="0.28" />
                      <stop offset="100%" stop-color="#38bdf8" stop-opacity="0.03" />
                    </linearGradient>
                  </defs>
                  <path :d="equityChart.equityArea" fill="url(#equity-fill)" />
                  <path :d="equityChart.equityPath" fill="none" stroke="#0ea5e9" stroke-width="3" stroke-linecap="round" />
                  <path :d="equityChart.dailyPnlPath" fill="none" stroke="#10b981" stroke-width="2" stroke-dasharray="5 5" stroke-linecap="round" />
                  <path :d="equityChart.marginPath" fill="none" stroke="#f59e0b" stroke-width="2" stroke-dasharray="2 6" stroke-linecap="round" />
                  <line v-for="item in equityChart.labels" :key="`label-line-${item.time}`" :x1="22 + ((equityPoints.findIndex((point) => point.time === item.time) * (equityChart.width - 44)) / Math.max(equityPoints.length - 1, 1))" y1="18" :x2="22 + ((equityPoints.findIndex((point) => point.time === item.time) * (equityChart.width - 44)) / Math.max(equityPoints.length - 1, 1))" :y2="equityChart.height - 22" stroke="rgba(148,163,184,0.16)" stroke-width="1" />
                  <circle v-for="item in equityChart.blockedDots" :key="`blocked-${item.time}-${item.x}`" :cx="item.x" cy="24" r="4" fill="#ef4444" />
                </svg>
                <div class="mt-3 flex flex-wrap items-center justify-between gap-3 text-xs text-slate-400">
                  <div class="flex flex-wrap items-center gap-4">
                    <span class="inline-flex items-center gap-2"><span class="h-2.5 w-2.5 rounded-full bg-sky-500"></span>权益</span>
                    <span class="inline-flex items-center gap-2"><span class="h-2.5 w-2.5 rounded-full bg-emerald-500"></span>今日盈亏</span>
                    <span class="inline-flex items-center gap-2"><span class="h-2.5 w-2.5 rounded-full bg-amber-500"></span>保证金率</span>
                    <span class="inline-flex items-center gap-2"><span class="h-2.5 w-2.5 rounded-full bg-rose-500"></span>阻断点</span>
                  </div>
                  <div class="flex flex-wrap gap-4">
                    <span v-for="item in equityChart.labels" :key="`label-${item.time}`">{{ formatChartTime(item.time) }}</span>
                  </div>
                </div>
              </div>

              <div class="overflow-hidden rounded-2xl border border-slate-200 bg-slate-50 p-4 dark:border-slate-800 dark:bg-slate-950/60">
                <div class="mb-3 flex items-center justify-between gap-3">
                  <div class="text-[12.5px] font-medium">名义价值 / 阻断走势</div>
                  <div class="text-[11px] text-slate-400">持仓 / 委托 / 总名义价值 / 阻断信号</div>
                </div>
                <svg :viewBox="`0 0 ${equityChart.width} ${equityChart.height}`" class="h-64 w-full">
                  <path :d="equityChart.positionNotionalPath" fill="none" stroke="#8b5cf6" stroke-width="2.5" stroke-linecap="round" />
                  <path :d="equityChart.openOrderNotionalPath" fill="none" stroke="#06b6d4" stroke-width="2.5" stroke-dasharray="6 4" stroke-linecap="round" />
                  <path :d="equityChart.totalNotionalPath" fill="none" stroke="#f97316" stroke-width="2.5" stroke-linecap="round" />
                  <path :d="equityChart.blockedSignalPath" fill="none" stroke="#ef4444" stroke-width="2" stroke-dasharray="3 6" stroke-linecap="round" />
                  <line v-for="item in equityChart.labels" :key="`notional-label-line-${item.time}`" :x1="22 + ((equityPoints.findIndex((point) => point.time === item.time) * (equityChart.width - 44)) / Math.max(equityPoints.length - 1, 1))" y1="18" :x2="22 + ((equityPoints.findIndex((point) => point.time === item.time) * (equityChart.width - 44)) / Math.max(equityPoints.length - 1, 1))" :y2="equityChart.height - 22" stroke="rgba(148,163,184,0.16)" stroke-width="1" />
                  <circle v-for="item in equityChart.blockedDots" :key="`notional-blocked-${item.time}-${item.x}`" :cx="item.x" cy="24" r="4" fill="#ef4444" />
                </svg>
                <div class="mt-3 flex flex-wrap items-center justify-between gap-3 text-xs text-slate-400">
                  <div class="flex flex-wrap items-center gap-4">
                    <span class="inline-flex items-center gap-2"><span class="h-2.5 w-2.5 rounded-full bg-violet-500"></span>持仓名义价值</span>
                    <span class="inline-flex items-center gap-2"><span class="h-2.5 w-2.5 rounded-full bg-cyan-500"></span>委托名义价值</span>
                    <span class="inline-flex items-center gap-2"><span class="h-2.5 w-2.5 rounded-full bg-orange-500"></span>总名义价值</span>
                    <span class="inline-flex items-center gap-2"><span class="h-2.5 w-2.5 rounded-full bg-rose-500"></span>阻断信号</span>
                  </div>
                  <div class="flex flex-wrap gap-4">
                    <span v-for="item in equityChart.labels" :key="`notional-label-${item.time}`">{{ formatChartTime(item.time) }}</span>
                  </div>
                </div>
              </div>
            </div>
            <div v-else class="mt-6 rounded-2xl border border-dashed border-slate-300 p-8 text-sm text-slate-500 dark:border-slate-700 dark:text-slate-400">暂无风险采样数据，等 risk heartbeat 再积累一会儿就会出图。</div>
          </div>

          <div class="space-y-6">
            <div class="elevated-panel p-3.5">
              <h3 class="panel-title !mt-0">风险摘要面板</h3>
              <p class="mt-1 text-[13px] text-slate-500 dark:text-slate-400">从日志审计里抽最值得盯的异常、风控与关键动作。</p>

              <div class="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                <div class="metric-tile p-3.5">
                  <div class="text-[11px] text-slate-400">错误</div>
                  <div class="mt-1 text-xl font-semibold text-rose-600 dark:text-rose-300">{{ alertSummary.errors }}</div>
                </div>
                <div class="metric-tile p-3.5">
                  <div class="text-[11px] text-slate-400">警告</div>
                  <div class="mt-1 text-xl font-semibold text-amber-600 dark:text-amber-300">{{ alertSummary.warnings }}</div>
                </div>
                <div class="metric-tile p-3.5">
                  <div class="text-[11px] text-slate-400">风控事件</div>
                  <div class="mt-1 text-xl font-semibold text-orange-600 dark:text-orange-300">{{ alertSummary.risk }}</div>
                </div>
                <div class="metric-tile p-3.5">
                  <div class="text-[11px] text-slate-400">阻断命中</div>
                  <div class="mt-1 text-xl font-semibold text-rose-600 dark:text-rose-300">{{ alertSummary.blocked }}</div>
                </div>
              </div>

              <div class="mt-4 space-y-2.5">
                <div v-for="(item, index) in recentAlerts.slice(0, 6)" :key="`${item['时间']}-${index}`" :class="['rounded-lg border p-3.5', alertRowClass(item)]">
                  <div class="flex items-center justify-between gap-3">
                    <div class="flex flex-wrap items-center gap-2">
                      <span :class="['rounded-full px-2.5 py-1 text-xs font-medium', alertBadgeClass(item)]">{{ item['级别'] }}</span>
                      <span :class="['rounded-full px-2.5 py-1 text-xs font-medium', categoryClass(item['分类'])]">{{ item['分类'] }}</span>
                      <span class="text-[11px] text-slate-400">{{ item['模块'] }}</span>
                    </div>
                    <div class="text-[11px] text-slate-400">{{ item['时间'] }}</div>
                  </div>
                  <div class="mt-2 text-sm break-all text-slate-700 dark:text-slate-200">{{ item['内容'] }}</div>
                </div>
                <div v-if="!recentAlerts.length" class="rounded-lg border border-dashed border-slate-300 p-4 text-[13px] text-slate-500 dark:border-slate-700 dark:text-slate-400">暂无需要优先关注的事件</div>
              </div>
            </div>

            <div class="elevated-panel p-3.5">
              <div class="flex items-center justify-between gap-4">
                <div>
                  <h3 class="panel-title !mt-0">运行活跃度</h3>
                  <p class="mt-1 text-[13px] text-slate-500 dark:text-slate-400">保留系统活跃柱状概览，辅助判断主循环是否稳定。</p>
                </div>
                <div class="text-[11px] text-slate-400">{{ activityBars.length }} 个时间片</div>
              </div>
              <div class="mt-4 rounded-lg border border-slate-200 bg-slate-50 px-3 py-4 dark:border-slate-800 dark:bg-slate-900/40">
                <div class="flex h-36 items-end gap-2">
                  <div v-for="(item, index) in recentActivityBars" :key="`${item.time}-${index}`" class="flex h-full min-w-0 flex-1 flex-col justify-end">
                    <div class="group relative flex h-full items-end">
                      <div class="w-full rounded-t-lg bg-gradient-to-t from-sky-500 to-cyan-400 transition-opacity group-hover:opacity-80" :style="{ height: item.height }"></div>
                      <div class="pointer-events-none absolute bottom-full left-1/2 z-10 mb-2 hidden -translate-x-1/2 whitespace-nowrap rounded-lg bg-slate-900 px-2 py-1 text-[11px] text-white shadow-lg group-hover:block">
                        {{ formatDateTime(item.time) }} / {{ item.count }} 条
                      </div>
                    </div>
                    <div class="mt-2 text-center text-[10px] text-slate-400">{{ formatShortTime(item.time) }}</div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </section>

        <section v-if="currentView === 'cta-dashboard'" id="cta-dashboard" class="mt-4 space-y-3">
          <div class="elevated-panel p-4">
            <div class="panel-header">
              <div>
                <div class="panel-kicker">CTA Adaptive Cockpit</div>
                <h3 class="panel-title">CTA 驾驶舱</h3>
                <p class="panel-desc">集中看 family 排行、多空分布、regime 表现和错杀/放错审计。</p>
              </div>
              <div class="flex items-center gap-2 flex-wrap justify-end">
                <button v-for="hours in [24, 72, 168]" :key="`cta-window-${hours}`" :class="['filter-pill', ctaWindowHours === hours ? 'filter-pill-sky-active' : 'filter-pill-muted']" @click="ctaWindowHours = hours; refreshAll()">{{ hours === 168 ? '7d' : `${hours}h` }}</button>
                <span class="result-badge">{{ ctaDashboard?.['交易对'] || overview?.['交易对'] || '--' }}</span>
                <span class="result-badge">{{ ctaDashboard?.marketRegime || 'unknown' }}</span>
              </div>
            </div>
            <div class="mt-3 grid gap-3 md:grid-cols-2 xl:grid-cols-5">
              <div v-for="item in ctaOverviewCards" :key="item.label" class="metric-tile p-3.5">
                <div class="text-[11px] text-slate-400">{{ item.label }}</div>
                <div class="mt-2 text-[20px] font-semibold tracking-tight">{{ item.value }}</div>
                <div class="mt-1 text-[11px] text-slate-500 dark:text-slate-400">{{ item.hint }}</div>
              </div>
            </div>
          </div>

          <div class="grid gap-3 xl:grid-cols-3">
            <div class="elevated-panel overflow-hidden">
              <div class="toolbar-strip"><div><h4 class="subpanel-title">Top Family Score 图</h4><p class="subpanel-desc">观察当前 strongest family 的综合 score。</p></div></div>
              <div class="p-3">
                <svg :viewBox="`0 0 ${ctaLeaderboardChart.width} ${ctaLeaderboardChart.height}`" class="h-56 w-full">
                  <line :x1="ctaLeaderboardChart.padding" :x2="ctaLeaderboardChart.width - ctaLeaderboardChart.padding" :y1="ctaLeaderboardChart.baseline" :y2="ctaLeaderboardChart.baseline" stroke="#cbd5e1" stroke-width="1" />
                  <g v-for="bar in ctaLeaderboardChart.bars" :key="`score-${bar.label}`">
                    <rect :x="bar.x" :y="bar.y" :width="bar.width" :height="bar.height" rx="6" fill="#0ea5e9" fill-opacity="0.85" />
                    <text :x="bar.x + bar.width / 2" :y="ctaLeaderboardChart.height - 6" text-anchor="middle" font-size="10" fill="#64748b">{{ bar.label.slice(0, 10) }}</text>
                  </g>
                </svg>
              </div>
            </div>
            <div class="elevated-panel overflow-hidden">
              <div class="toolbar-strip"><div><h4 class="subpanel-title">Family 总榜</h4><p class="subpanel-desc">当前综合 score 最高的 trigger family。</p></div></div>
              <div class="table-wrap m-3 overflow-auto max-h-[360px]">
                <table class="data-table min-w-full divide-y divide-slate-200 text-sm dark:divide-slate-800">
                  <thead class="sticky top-0 bg-slate-50 dark:bg-slate-950/95"><tr><th class="text-left font-medium text-slate-500">Family</th><th class="text-left font-medium text-slate-500">方向</th><th class="text-right font-medium text-slate-500">Score</th><th class="text-right font-medium text-slate-500">WR</th></tr></thead>
                  <tbody class="divide-y divide-slate-100 dark:divide-slate-800">
                    <tr v-for="row in ctaTopFamilies.slice(0, 10)" :key="`${row.trigger_family}-${row.side}`"><td class="font-medium">{{ row.trigger_family }}</td><td>{{ row.side }}</td><td class="text-right tabular-nums">{{ formatNumber(row.score, 4) }}</td><td class="text-right tabular-nums">{{ percentText(row.win_rate) }}</td></tr>
                    <tr v-if="!ctaTopFamilies.length"><td colspan="4" class="py-6 text-center text-[13px] text-slate-500 dark:text-slate-400">暂无 family 数据</td></tr>
                  </tbody>
                </table>
              </div>
            </div>

            <div class="elevated-panel overflow-hidden">
              <div class="toolbar-strip"><div><h4 class="subpanel-title">Long Family</h4><p class="subpanel-desc">做多侧表现最好的 family。</p></div></div>
              <div class="px-3 pt-3">
                <svg :viewBox="`0 0 ${ctaLongWinrateChart.width} ${ctaLongWinrateChart.height}`" class="h-40 w-full">
                  <line :x1="ctaLongWinrateChart.padding" :x2="ctaLongWinrateChart.width - ctaLongWinrateChart.padding" :y1="ctaLongWinrateChart.baseline" :y2="ctaLongWinrateChart.baseline" stroke="#cbd5e1" stroke-width="1" />
                  <g v-for="bar in ctaLongWinrateChart.bars" :key="`long-${bar.label}`">
                    <rect :x="bar.x" :y="bar.y" :width="bar.width" :height="bar.height" rx="5" fill="#10b981" fill-opacity="0.85" />
                  </g>
                </svg>
              </div>
              <div class="table-wrap m-3 overflow-auto max-h-[360px]">
                <table class="data-table min-w-full divide-y divide-slate-200 text-sm dark:divide-slate-800">
                  <thead class="sticky top-0 bg-slate-50 dark:bg-slate-950/95"><tr><th class="text-left font-medium text-slate-500">Family</th><th class="text-right font-medium text-slate-500">PnL</th><th class="text-right font-medium text-slate-500">WR</th></tr></thead>
                  <tbody class="divide-y divide-slate-100 dark:divide-slate-800">
                    <tr v-for="row in ctaLongFamilies.slice(0, 8)" :key="`${row.trigger_family}-${row.side}`"><td class="font-medium">{{ row.trigger_family }}</td><td class="text-right tabular-nums">{{ signedNumber(row.total_pnl) }}</td><td class="text-right tabular-nums">{{ percentText(row.win_rate) }}</td></tr>
                    <tr v-if="!ctaLongFamilies.length"><td colspan="3" class="py-6 text-center text-[13px] text-slate-500 dark:text-slate-400">暂无 long family 数据</td></tr>
                  </tbody>
                </table>
              </div>
            </div>

            <div class="elevated-panel overflow-hidden">
              <div class="toolbar-strip"><div><h4 class="subpanel-title">Short Family</h4><p class="subpanel-desc">做空侧表现最好的 family。</p></div></div>
              <div class="px-3 pt-3">
                <svg :viewBox="`0 0 ${ctaShortWinrateChart.width} ${ctaShortWinrateChart.height}`" class="h-40 w-full">
                  <line :x1="ctaShortWinrateChart.padding" :x2="ctaShortWinrateChart.width - ctaShortWinrateChart.padding" :y1="ctaShortWinrateChart.baseline" :y2="ctaShortWinrateChart.baseline" stroke="#cbd5e1" stroke-width="1" />
                  <g v-for="bar in ctaShortWinrateChart.bars" :key="`short-${bar.label}`">
                    <rect :x="bar.x" :y="bar.y" :width="bar.width" :height="bar.height" rx="5" fill="#f97316" fill-opacity="0.85" />
                  </g>
                </svg>
              </div>
              <div class="table-wrap m-3 overflow-auto max-h-[360px]">
                <table class="data-table min-w-full divide-y divide-slate-200 text-sm dark:divide-slate-800">
                  <thead class="sticky top-0 bg-slate-50 dark:bg-slate-950/95"><tr><th class="text-left font-medium text-slate-500">Family</th><th class="text-right font-medium text-slate-500">PnL</th><th class="text-right font-medium text-slate-500">WR</th></tr></thead>
                  <tbody class="divide-y divide-slate-100 dark:divide-slate-800">
                    <tr v-for="row in ctaShortFamilies.slice(0, 8)" :key="`${row.trigger_family}-${row.side}`"><td class="font-medium">{{ row.trigger_family }}</td><td class="text-right tabular-nums">{{ signedNumber(row.total_pnl) }}</td><td class="text-right tabular-nums">{{ percentText(row.win_rate) }}</td></tr>
                    <tr v-if="!ctaShortFamilies.length"><td colspan="3" class="py-6 text-center text-[13px] text-slate-500 dark:text-slate-400">暂无 short family 数据</td></tr>
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div class="elevated-panel p-4">
            <div class="panel-header">
              <div>
                <div class="panel-kicker">Quick Tuning</div>
                <h4 class="panel-title">Family 快速调参</h4>
                <p class="panel-desc">直接在驾驶舱里做常用调参，不用手动翻配置页。</p>
              </div>
            </div>
            <div class="mt-3 flex flex-wrap gap-2">
              <button class="rounded-md bg-sky-600 px-3 py-2 text-[12.5px] font-medium text-white transition hover:bg-sky-700" @click="applyCtaPreset({ 'cta.disable_trend_continuation_long': false, 'cta.trend_continuation_minimum_entry_pathway': 'FAST_TRACK' }, '已偏向放大趋势延续 family')">放大趋势延续</button>
              <button class="rounded-md bg-amber-500 px-3 py-2 text-[12.5px] font-medium text-white transition hover:bg-amber-600" @click="applyCtaPreset({ 'cta.disable_trend_continuation_long': true, 'cta.disable_near_breakout_release_long': true }, '已压制追突破型 family')">压制追突破</button>
              <button class="rounded-md bg-emerald-600 px-3 py-2 text-[12.5px] font-medium text-white transition hover:bg-emerald-700" @click="applyCtaPreset({ 'cta.long_standard_min_confidence': 0.55, 'cta.short_standard_min_confidence': 0.53 }, '已适度放宽多空标准置信度')">适度放宽置信度</button>
              <button class="rounded-md bg-rose-600 px-3 py-2 text-[12.5px] font-medium text-white transition hover:bg-rose-700" @click="applyCtaPreset({ 'cta.long_standard_min_confidence': 0.62, 'cta.short_standard_min_confidence': 0.60, 'cta.family_adaptation_boost_cap': 0.14 }, '已收紧放行并降低 family 自适应加成')">收紧放行</button>
              <button class="rounded-md border border-slate-300 px-3 py-2 text-[12.5px] font-medium transition hover:bg-slate-100 dark:border-slate-700 dark:hover:bg-slate-800" @click="rollbackCtaTuning">一键回滚 CTA 参数</button>
              <button class="rounded-md border border-slate-300 px-3 py-2 text-[12.5px] font-medium transition hover:bg-slate-100 dark:border-slate-700 dark:hover:bg-slate-800" @click="currentView = 'config'; activeConfigSection = 'cta'; activeConfigGroup = '自适应 / Family 开关'">打开 CTA 配置</button>
            </div>
            <div class="mt-3 rounded-lg border border-slate-200 bg-slate-50 p-3 text-[12px] text-slate-600 dark:border-slate-800 dark:bg-slate-900/40 dark:text-slate-300">
              当前 CTA 快照：{{ JSON.stringify(ctaTuningSnapshot) }}
            </div>
          </div>

          <div class="grid gap-3 xl:grid-cols-[1.15fr,0.85fr]">
            <div class="elevated-panel overflow-hidden">
              <div class="toolbar-strip"><div><h4 class="subpanel-title">Family 历史切换器</h4><p class="subpanel-desc">按 family 单独切换，观察累计 PnL 轨迹，而不是只盯固定前三。</p></div></div>
              <div class="p-3">
                <div class="mb-3 flex flex-wrap gap-2">
                  <button :class="['filter-pill', ctaFamilyTrendMode === 'cum_pnl' ? 'filter-pill-sky-active' : 'filter-pill-muted']" @click="ctaFamilyTrendMode = 'cum_pnl'">累计 PnL</button>
                  <button :class="['filter-pill', ctaFamilyTrendMode === 'trade_pnl' ? 'filter-pill-sky-active' : 'filter-pill-muted']" @click="ctaFamilyTrendMode = 'trade_pnl'">单笔 PnL</button>
                  <button :class="['filter-pill', ctaFamilyTrendMode === 'rolling_wr' ? 'filter-pill-sky-active' : 'filter-pill-muted']" @click="ctaFamilyTrendMode = 'rolling_wr'">WR 滚动</button>
                </div>
                <div class="mb-3 rounded-lg border border-slate-200 bg-slate-50 p-3 dark:border-slate-800 dark:bg-slate-900/40">
                  <div class="mb-2 text-[11px] text-slate-400">全量 Family 选择（不受 topN 限制）</div>
                  <select v-model="ctaSelectedFamily" class="config-input w-full">
                    <option v-for="family in ctaFamilyOptions" :key="`family-option-${family}`" :value="family">{{ family }}</option>
                  </select>
                  <div class="mt-2 flex flex-wrap gap-2">
                    <button
                      v-for="family in ctaFamilyOptions.slice(0, 12)"
                      :key="`family-switch-${family}`"
                      :class="['filter-pill', ctaActiveFamily === family ? 'filter-pill-sky-active' : 'filter-pill-muted']"
                      @click="ctaSelectedFamily = family"
                    >
                      {{ family }}
                    </button>
                  </div>
                </div>
                <div v-if="ctaActiveFamily" class="mb-3 grid gap-2 md:grid-cols-5">
                  <div class="rounded-lg border border-slate-200 bg-slate-50 p-3 dark:border-slate-800 dark:bg-slate-900/40">
                    <div class="text-[11px] text-slate-400">当前 Family</div>
                    <div class="mt-1 text-[14px] font-semibold">{{ ctaActiveFamilyMeta.family }}</div>
                  </div>
                  <div class="rounded-lg border border-slate-200 bg-slate-50 p-3 dark:border-slate-800 dark:bg-slate-900/40">
                    <div class="text-[11px] text-slate-400">累计 PnL</div>
                    <div class="mt-1 text-[14px] font-semibold" :class="pnlClass(ctaActiveFamilyMeta.latestCumPnl)">{{ signedNumber(ctaActiveFamilyMeta.latestCumPnl, 4) }}</div>
                  </div>
                  <div class="rounded-lg border border-slate-200 bg-slate-50 p-3 dark:border-slate-800 dark:bg-slate-900/40">
                    <div class="text-[11px] text-slate-400">最近单笔</div>
                    <div class="mt-1 text-[14px] font-semibold" :class="pnlClass(ctaActiveFamilyMeta.latestTradePnl)">{{ signedNumber(ctaActiveFamilyMeta.latestTradePnl, 4) }}</div>
                  </div>
                  <div class="rounded-lg border border-slate-200 bg-slate-50 p-3 dark:border-slate-800 dark:bg-slate-900/40">
                    <div class="text-[11px] text-slate-400">滚动 WR</div>
                    <div class="mt-1 text-[14px] font-semibold">{{ percentText(ctaActiveFamilyMeta.latestRollingWr) }}</div>
                  </div>
                  <div class="rounded-lg border border-slate-200 bg-slate-50 p-3 dark:border-slate-800 dark:bg-slate-900/40">
                    <div class="text-[11px] text-slate-400">历史点数</div>
                    <div class="mt-1 text-[14px] font-semibold">{{ ctaActiveFamilyMeta.points }}</div>
                  </div>
                </div>
                <svg v-if="ctaActiveFamily && ctaFamilyTrendChart.chart.path" :viewBox="`0 0 520 180`" class="h-48 w-full">
                  <g v-for="(band, idx) in ctaFamilyRegimeBands" :key="`regime-band-${idx}`">
                    <rect :x="band.x" y="18" :width="band.width" height="144" :fill="band.regime === 'trend' || band.regime === 'trend_impulse' ? 'rgba(16,185,129,0.08)' : band.regime === 'sideways' ? 'rgba(148,163,184,0.10)' : 'rgba(14,165,233,0.08)'" />
                    <text :x="band.x + 4" y="30" font-size="9" fill="#64748b">{{ band.regime }}</text>
                  </g>
                  <path :d="ctaFamilyTrendChart.chart.area" :fill="ctaFamilyTrendChart.color" fill-opacity="0.08" />
                  <path :d="ctaFamilyTrendChart.chart.path" fill="none" :stroke="ctaFamilyTrendChart.color" stroke-width="2.5" stroke-linecap="round" />
                  <g v-for="(point, idx) in ctaFamilyTrendChart.chart.points" :key="`trend-point-${idx}`" class="group">
                    <circle :cx="point.x" :cy="point.y" r="3.2" :fill="ctaFamilyTrendChart.color" />
                    <g class="pointer-events-none opacity-0 transition group-hover:opacity-100">
                      <rect :x="Math.max(8, Math.min(point.x - 62, 520 - 132))" :y="Math.max(8, point.y - 74)" width="124" height="64" rx="8" fill="rgba(15,23,42,0.94)" />
                      <text :x="Math.max(16, Math.min(point.x - 54, 520 - 124))" :y="Math.max(24, point.y - 56)" font-size="10" fill="#e2e8f0">{{ formatDateTime(point.raw.timestamp) }}</text>
                      <text :x="Math.max(16, Math.min(point.x - 54, 520 - 124))" :y="Math.max(38, point.y - 42)" font-size="10" fill="#f8fafc">{{ ctaFamilyTrendChart.metricLabel }}: {{ ctaFamilyTrendMode === 'rolling_wr' ? percentText(point.raw.rolling_wr) : signedNumber(ctaFamilyTrendMode === 'trade_pnl' ? point.raw.trade_pnl : point.raw.cum_pnl, 4) }}</text>
                      <text :x="Math.max(16, Math.min(point.x - 54, 520 - 124))" :y="Math.max(52, point.y - 28)" font-size="10" fill="#cbd5e1">trade_count: {{ point.raw.trade_count }}</text>
                      <text :x="Math.max(16, Math.min(point.x - 54, 520 - 124))" :y="Math.max(66, point.y - 14)" font-size="10" fill="#cbd5e1">single: {{ signedNumber(point.raw.trade_pnl, 4) }}</text>
                    </g>
                  </g>
                </svg>
                <div v-else class="py-8 text-center text-[13px] text-slate-500 dark:text-slate-400">暂无可切换的 family 历史数据</div>
              </div>
            </div>
            <div class="elevated-panel overflow-hidden">
              <div class="toolbar-strip"><div><h4 class="subpanel-title">Regime 切换对比</h4><p class="subpanel-desc">对比不同 market regime 段内的胜率和 PnL。</p></div></div>
              <div class="table-wrap m-3 overflow-auto max-h-[240px]">
                <table class="data-table min-w-full divide-y divide-slate-200 text-sm dark:divide-slate-800">
                  <thead class="sticky top-0 bg-slate-50 dark:bg-slate-950/95"><tr><th class="text-left font-medium text-slate-500">Regime</th><th class="text-right font-medium text-slate-500">次数</th><th class="text-right font-medium text-slate-500">WR</th><th class="text-right font-medium text-slate-500">PnL</th></tr></thead>
                  <tbody class="divide-y divide-slate-100 dark:divide-slate-800">
                    <tr v-for="(row, idx) in ctaRegimeTransitions" :key="`${row.market_regime}-${idx}`"><td class="font-medium">{{ row.market_regime }}</td><td class="text-right tabular-nums">{{ row.trade_count }}</td><td class="text-right tabular-nums">{{ percentText(row.win_rate) }}</td><td class="text-right tabular-nums">{{ signedNumber(row.total_pnl) }}</td></tr>
                    <tr v-if="!ctaRegimeTransitions.length"><td colspan="4" class="py-6 text-center text-[13px] text-slate-500 dark:text-slate-400">暂无 regime 切换对比数据</td></tr>
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div class="grid gap-3 xl:grid-cols-[1.15fr,0.85fr]">
            <div class="elevated-panel overflow-hidden">
              <div class="toolbar-strip"><div><h4 class="subpanel-title">Family × Regime 二维热力矩阵</h4><p class="subpanel-desc">支持 net / long / short / bias 四种视图，bias 专门看多空偏移，默认按偏移绝对值排序。</p></div></div>
              <div class="px-3 pt-3 flex flex-wrap gap-2">
                <button :class="['filter-pill', ctaHeatmapMode === 'bias' ? 'filter-pill-rose-active' : 'filter-pill-muted']" @click="ctaHeatmapMode = 'bias'">偏移最大</button>
                <button :class="['filter-pill', ctaHeatmapMode === 'net' ? 'filter-pill-sky-active' : 'filter-pill-muted']" @click="ctaHeatmapMode = 'net'">净表现</button>
                <button :class="['filter-pill', ctaHeatmapMode === 'long' ? 'filter-pill-sky-active' : 'filter-pill-muted']" @click="ctaHeatmapMode = 'long'">Long</button>
                <button :class="['filter-pill', ctaHeatmapMode === 'short' ? 'filter-pill-sky-active' : 'filter-pill-muted']" @click="ctaHeatmapMode = 'short'">Short</button>
              </div>
              <div class="px-3 pt-2 flex flex-wrap gap-2">
                <button :class="['filter-pill', ctaHeatmapSortMode === 'bias_abs' ? 'filter-pill-rose-active' : 'filter-pill-muted']" @click="ctaHeatmapSortMode = 'bias_abs'">按偏移 abs 排序</button>
                <button :class="['filter-pill', ctaHeatmapSortMode === 'net' ? 'filter-pill-sky-active' : 'filter-pill-muted']" @click="ctaHeatmapSortMode = 'net'">按净 pnl 排序</button>
                <button :class="['filter-pill', ctaHeatmapSortMode === 'long' ? 'filter-pill-sky-active' : 'filter-pill-muted']" @click="ctaHeatmapSortMode = 'long'">按 long 排序</button>
                <button :class="['filter-pill', ctaHeatmapSortMode === 'short' ? 'filter-pill-sky-active' : 'filter-pill-muted']" @click="ctaHeatmapSortMode = 'short'">按 short 排序</button>
              </div>
              <div class="m-3 overflow-auto">
                <table class="min-w-full border-separate border-spacing-1.5 text-sm">
                  <thead>
                    <tr>
                      <th class="sticky left-0 z-10 rounded-lg bg-slate-50 px-3 py-2 text-left text-[12px] font-medium text-slate-500 shadow-sm dark:bg-slate-950/95">Family \ Regime</th>
                      <th v-for="regime in ctaRegimeHeatMatrix.regimes" :key="`heat-head-${regime}`" :class="['rounded-lg px-3 py-2 text-center text-[12px] font-medium shadow-sm', ctaSelectedRegime === regime ? 'bg-sky-100 text-sky-700 ring-1 ring-sky-300 dark:bg-sky-950/40 dark:text-sky-300 dark:ring-sky-800' : 'bg-slate-50 text-slate-500 dark:bg-slate-950/95']">{{ regime }}</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr v-for="row in ctaRegimeHeatMatrix.rows" :key="`heat-row-${row.family}`">
                      <td class="sticky left-0 z-10 rounded-lg bg-white px-3 py-2 font-medium shadow-sm dark:bg-slate-900">
                        <div>{{ row.family }}</div>
                        <div class="mt-1 text-[10px] text-slate-400">bias abs {{ formatNumber(row.familyBiasAbs, 4) }}</div>
                      </td>
                      <td v-for="cell in row.cells" :key="`heat-cell-${row.family}-${cell.regime}`" class="min-w-[136px] cursor-pointer rounded-xl border px-3 py-2 align-top shadow-sm transition hover:scale-[1.01] hover:shadow-md"
                        @click="ctaSelectedFamily = row.family; ctaSelectedRegime = cell.regime"
                        :class="[
                          cell.tone === 'emerald' ? 'border-emerald-200 dark:border-emerald-900/50' : cell.tone === 'rose' ? 'border-rose-200 dark:border-rose-900/50' : 'border-slate-200 dark:border-slate-800',
                          ctaSelectedFamily === row.family && ctaSelectedRegime === cell.regime ? 'ring-2 ring-sky-400 ring-offset-1 dark:ring-sky-500' : ''
                        ]"
                        :style="cell.tone === 'emerald'
                          ? { backgroundColor: `rgba(16,185,129,${0.10 + cell.intensity * 0.38})` }
                          : cell.tone === 'rose'
                            ? { backgroundColor: `rgba(244,63,94,${0.10 + cell.intensity * 0.38})` }
                            : { backgroundColor: `rgba(148,163,184,${0.08 + cell.intensity * 0.18})` }">
                        <div class="flex items-start justify-between gap-2">
                          <div>
                            <div class="text-[11px] text-slate-500 dark:text-slate-300">总 {{ cell.totalTrades }} 笔</div>
                            <div class="mt-1 text-[13px] font-semibold" :class="pnlClass(cell.value)">{{ signedNumber(cell.value, 4) }}</div>
                          </div>
                          <div class="rounded-full bg-white/70 px-2 py-0.5 text-[10px] font-medium text-slate-600 dark:bg-slate-950/40 dark:text-slate-200">{{ percentText(cell.netWinRate) }}</div>
                        </div>
                        <div class="mt-2 grid grid-cols-2 gap-2 text-[10px] text-slate-600 dark:text-slate-200">
                          <div>Long {{ signedNumber(cell.longPnl, 3) }} / {{ cell.longTrades }}</div>
                          <div>Short {{ signedNumber(cell.shortPnl, 3) }} / {{ cell.shortTrades }}</div>
                        </div>
                      </td>
                    </tr>
                    <tr v-if="!ctaRegimeHeatMatrix.rows.length">
                      <td colspan="99" class="py-6 text-center text-[13px] text-slate-500 dark:text-slate-400">暂无热力矩阵数据</td>
                    </tr>
                  </tbody>
                </table>
              </div>
              <div class="px-3 pb-3 text-[11px] text-slate-500 dark:text-slate-400">
                bias = long pnl - short pnl；正值代表更偏多头有效，负值代表更偏空头有效。默认模式就是拿它来找“多空偏移最大”。
              </div>
            </div>

            <div class="space-y-3">
              <div class="elevated-panel overflow-hidden">
                <div class="toolbar-strip"><div><h4 class="subpanel-title">Family × Regime 动作标签</h4><p class="subpanel-desc">把热力矩阵结果直接翻译成提升 / 压制 / 偏多 / 偏空 / 观察标签，方便直接做决策。</p></div></div>
                <div class="space-y-2 p-3">
                  <div v-for="item in ctaFamilyRegimeActions.slice(0, 10)" :key="`family-action-${item.trigger_family}-${item.market_regime}`" class="rounded-lg border border-slate-200 bg-slate-50 p-3 dark:border-slate-800 dark:bg-slate-900/40">
                    <div class="flex items-center justify-between gap-3">
                      <div class="flex flex-wrap items-center gap-2">
                        <span class="text-[13px] font-semibold">{{ item.trigger_family }}</span>
                        <span class="rounded-full bg-slate-200 px-2 py-0.5 text-[10px] font-medium text-slate-700 dark:bg-slate-800 dark:text-slate-200">{{ item.market_regime }}</span>
                        <span :class="[
                          'rounded-full px-2 py-0.5 text-[10px] font-medium',
                          item.action === 'promote' ? 'bg-emerald-500/15 text-emerald-600 dark:text-emerald-300' :
                          item.action === 'suppress' ? 'bg-rose-500/15 text-rose-600 dark:text-rose-300' :
                          item.action === 'favor_long' ? 'bg-sky-500/15 text-sky-600 dark:text-sky-300' :
                          item.action === 'favor_short' ? 'bg-orange-500/15 text-orange-600 dark:text-orange-300' :
                          'bg-slate-500/15 text-slate-600 dark:text-slate-300'
                        ]">{{ item.label }}</span>
                      </div>
                      <button class="filter-pill filter-pill-muted" @click="ctaSelectedFamily = item.trigger_family; ctaSelectedRegime = item.market_regime">查看</button>
                    </div>
                    <div class="mt-2 text-[12px] text-slate-600 dark:text-slate-300">{{ item.detail }}</div>
                    <div class="mt-2 grid grid-cols-2 gap-2 text-[11px] text-slate-500 dark:text-slate-400">
                      <div>net {{ signedNumber(item.net_pnl, 4) }}</div>
                      <div>bias {{ signedNumber(item.bias_pnl, 4) }}</div>
                      <div>long {{ signedNumber(item.long_pnl, 4) }}</div>
                      <div>short {{ signedNumber(item.short_pnl, 4) }}</div>
                    </div>
                    <div v-if="item.preset_patch && Object.keys(item.preset_patch).length" class="mt-3 rounded-lg border border-dashed border-slate-300 p-2.5 dark:border-slate-700">
                      <div class="text-[11px] font-medium text-slate-500 dark:text-slate-400">Preset 候选：{{ item.preset_label }}</div>
                      <div class="mt-1 text-[11px] break-all text-slate-500 dark:text-slate-400">{{ JSON.stringify(item.preset_patch) }}</div>
                      <div class="mt-2 flex flex-wrap gap-2">
                        <button class="rounded-md bg-sky-600 px-3 py-1.5 text-[12px] font-medium text-white transition hover:bg-sky-700" @click="applyCtaPreset(item.preset_patch, `已应用建议 preset：${item.trigger_family} / ${item.market_regime} / ${item.preset_label || item.label}`, { trigger_family: item.trigger_family, market_regime: item.market_regime, action: item.action, preset_label: item.preset_label })">应用候选</button>
                      </div>
                    </div>
                  </div>
                  <div v-if="!ctaFamilyRegimeActions.length" class="rounded-lg border border-slate-200 bg-slate-50 p-3 text-[12px] text-slate-500 dark:border-slate-800 dark:bg-slate-900/40 dark:text-slate-400">当前暂无 family × regime 动作标签。</div>
                </div>
              </div>
              <div class="elevated-panel overflow-hidden">
                <div class="toolbar-strip"><div><h4 class="subpanel-title">Preset 应用记录 / 效果回看</h4><p class="subpanel-desc">记录最近应用过的 CTA preset，并给出 24h / 72h 回看时间点。</p></div></div>
                <div class="space-y-2 p-3">
                  <div v-for="(item, idx) in ctaPresetAudit.slice(0, 6)" :key="`preset-audit-${idx}`" class="rounded-lg border border-slate-200 bg-slate-50 p-3 dark:border-slate-800 dark:bg-slate-900/40">
                    <div class="flex items-center justify-between gap-3">
                      <div class="text-[13px] font-semibold">{{ item.meta?.trigger_family || 'CTA preset' }}</div>
                      <div class="text-[11px] text-slate-400">{{ formatDateTime(item.time) }}</div>
                    </div>
                    <div class="mt-1 text-[12px] text-slate-600 dark:text-slate-300">{{ item.successMessage }}</div>
                    <div class="mt-2 grid grid-cols-1 gap-1 text-[11px] text-slate-500 dark:text-slate-400">
                      <div v-for="row in item.diffRows" :key="row.path">{{ row.path }}: {{ JSON.stringify(row.before) }} → {{ JSON.stringify(row.after) }}</div>
                    </div>
                    <div class="mt-2 flex flex-wrap gap-2 text-[11px] text-slate-500 dark:text-slate-400">
                      <span class="rounded-full bg-slate-200 px-2 py-0.5 dark:bg-slate-800">24h 回看：{{ formatDateTime(item.review24hAt) }}</span>
                      <span v-if="item.review_24h" :class="['rounded-full px-2 py-0.5', item.review_24h.verdict === '变好' ? 'bg-emerald-500/15 text-emerald-600 dark:text-emerald-300' : item.review_24h.verdict === '变差' ? 'bg-rose-500/15 text-rose-600 dark:text-rose-300' : 'bg-slate-200 dark:bg-slate-800']">24h {{ item.review_24h.verdict }} / pnl {{ signedNumber(item.review_24h.pnl, 4) }} / {{ item.review_24h.trade_count }} 笔</span>
                    </div>
                    <div class="mt-2 flex flex-wrap gap-2 text-[11px] text-slate-500 dark:text-slate-400">
                      <span class="rounded-full bg-slate-200 px-2 py-0.5 dark:bg-slate-800">72h 回看：{{ formatDateTime(item.review72hAt) }}</span>
                      <span v-if="item.review_72h" :class="['rounded-full px-2 py-0.5', item.review_72h.verdict === '变好' ? 'bg-emerald-500/15 text-emerald-600 dark:text-emerald-300' : item.review_72h.verdict === '变差' ? 'bg-rose-500/15 text-rose-600 dark:text-rose-300' : 'bg-slate-200 dark:bg-slate-800']">72h {{ item.review_72h.verdict }} / pnl {{ signedNumber(item.review_72h.pnl, 4) }} / {{ item.review_72h.trade_count }} 笔</span>
                    </div>
                    <div class="mt-2 flex flex-wrap gap-2">
                      <button class="filter-pill filter-pill-muted" @click="ctaWindowHours = 24; refreshAll()">看 24h</button>
                      <button class="filter-pill filter-pill-muted" @click="ctaWindowHours = 72; refreshAll()">看 72h</button>
                    </div>
                  </div>
                  <div v-if="!ctaPresetAudit.length" class="rounded-lg border border-slate-200 bg-slate-50 p-3 text-[12px] text-slate-500 dark:border-slate-800 dark:bg-slate-900/40 dark:text-slate-400">还没有应用过 CTA preset。</div>
                </div>
              </div>
              <div class="elevated-panel overflow-hidden">
                <div class="toolbar-strip"><div><h4 class="subpanel-title">自动调参建议</h4><p class="subpanel-desc">基于当前 family 排名、错杀和放错结果给出建议。</p></div></div>
                <div class="space-y-2 p-3">
                  <div v-for="item in ctaSuggestions" :key="item.title" class="rounded-lg border border-slate-200 bg-slate-50 p-3 dark:border-slate-800 dark:bg-slate-900/40">
                    <div class="text-[13px] font-semibold">{{ item.title }}</div>
                    <div class="mt-1 text-[12px] text-slate-600 dark:text-slate-300">{{ item.detail }}</div>
                    <div class="mt-2 flex flex-wrap gap-2">
                      <button v-if="item.action === 'loosen_confidence'" class="rounded-md bg-emerald-600 px-3 py-1.5 text-[12px] font-medium text-white transition hover:bg-emerald-700" @click="applyCtaPreset({ 'cta.long_standard_min_confidence': 0.55, 'cta.short_standard_min_confidence': 0.53 }, '已按建议放宽 confidence floor')">按建议执行</button>
                      <button v-else-if="item.action === 'tighten' || item.action === 'tighten_release'" class="rounded-md bg-rose-600 px-3 py-1.5 text-[12px] font-medium text-white transition hover:bg-rose-700" @click="applyCtaPreset({ 'cta.long_standard_min_confidence': 0.62, 'cta.short_standard_min_confidence': 0.60, 'cta.family_adaptation_boost_cap': 0.14 }, '已按建议收紧 CTA 放行')">按建议执行</button>
                    </div>
                  </div>
                  <div v-if="!ctaSuggestions.length" class="rounded-lg border border-slate-200 bg-slate-50 p-3 text-[12px] text-slate-500 dark:border-slate-800 dark:bg-slate-900/40 dark:text-slate-400">当前暂无自动调参建议。</div>
                </div>
              </div>
              <div class="elevated-panel overflow-hidden">
                <div class="toolbar-strip"><div><h4 class="subpanel-title">最近错杀</h4><p class="subpanel-desc">值得复盘的 watch / probe / block 候选。</p></div></div>
                <div class="table-wrap m-3 overflow-auto max-h-[200px]">
                  <table class="data-table min-w-full divide-y divide-slate-200 text-sm dark:divide-slate-800">
                    <thead class="sticky top-0 bg-slate-50 dark:bg-slate-950/95"><tr><th class="text-left font-medium text-slate-500">Family</th><th class="text-left font-medium text-slate-500">方向</th><th class="text-left font-medium text-slate-500">Decider</th><th class="text-right font-medium text-slate-500">Conf</th></tr></thead>
                    <tbody class="divide-y divide-slate-100 dark:divide-slate-800">
                      <tr v-for="row in ctaMissedRows.slice(0, 8)" :key="`${row.timestamp}-${row.trigger_family}`"><td class="font-medium">{{ row.trigger_family }}</td><td>{{ row.side }}</td><td>{{ row.decider }}</td><td class="text-right tabular-nums">{{ formatNumber(row.confidence, 4) }}</td></tr>
                      <tr v-if="!ctaMissedRows.length"><td colspan="4" class="py-6 text-center text-[13px] text-slate-500 dark:text-slate-400">暂无错杀记录</td></tr>
                    </tbody>
                  </table>
                </div>
              </div>

              <div class="elevated-panel overflow-hidden">
                <div class="toolbar-strip"><div><h4 class="subpanel-title">最近放错</h4><p class="subpanel-desc">被系统放行但最终亏损的代表样本。</p></div></div>
                <div class="table-wrap m-3 overflow-auto max-h-[200px]">
                  <table class="data-table min-w-full divide-y divide-slate-200 text-sm dark:divide-slate-800">
                    <thead class="sticky top-0 bg-slate-50 dark:bg-slate-950/95"><tr><th class="text-left font-medium text-slate-500">Family</th><th class="text-left font-medium text-slate-500">方向</th><th class="text-right font-medium text-slate-500">PnL</th><th class="text-right font-medium text-slate-500">Conf</th></tr></thead>
                    <tbody class="divide-y divide-slate-100 dark:divide-slate-800">
                      <tr v-for="row in ctaBadReleaseRows.slice(0, 8)" :key="`${row.timestamp}-${row.trigger_family}`"><td class="font-medium">{{ row.trigger_family }}</td><td>{{ row.side }}</td><td class="text-right tabular-nums">{{ signedNumber(row.pnl) }}</td><td class="text-right tabular-nums">{{ formatNumber(row.confidence, 4) }}</td></tr>
                      <tr v-if="!ctaBadReleaseRows.length"><td colspan="4" class="py-6 text-center text-[13px] text-slate-500 dark:text-slate-400">暂无放错记录</td></tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        </section>

        <section v-if="currentView === 'control'" id="runtime" class="mt-4 space-y-3">
          <div class="elevated-panel">
              <div class="panel-header py-3.5">
                <div>
                  <div class="panel-kicker">Control Center</div>
                  <h3 class="panel-title">系统控制页</h3>
                  <p class="panel-desc">把刷新、轮询、主控启停和当前风险面放到一个标准控制台视图里。</p>
                </div>
                <div class="grid min-w-[220px] grid-cols-2 gap-px overflow-hidden rounded-lg border border-slate-200 bg-slate-200 dark:border-slate-800 dark:bg-slate-800">
                  <div class="bg-white px-3 py-2.5 dark:bg-slate-900">
                    <div class="text-[10px] uppercase tracking-[0.14em] text-slate-400">刷新时间</div>
                    <div class="mt-1 text-[12.5px] font-medium">{{ latestRefreshTime }}</div>
                  </div>
                  <div class="bg-white px-3 py-2.5 dark:bg-slate-900">
                    <div class="text-[10px] uppercase tracking-[0.14em] text-slate-400">登录账号</div>
                    <div class="mt-1 text-[12.5px] font-medium">{{ username }}</div>
                  </div>
                </div>
              </div>

              <div class="px-4 pt-3 pb-2">
                <div class="stat-strip rounded-none border-0 md:grid-cols-2 xl:grid-cols-4">
                  <div v-for="item in controlCards" :key="item.label" class="stat-cell">
                    <div class="text-[10px] uppercase tracking-[0.14em] text-slate-400">{{ item.label }}</div>
                    <div class="mt-2 text-[19px] font-semibold tracking-tight">{{ item.value }}</div>
                    <div class="mt-1 text-[12px] text-slate-500 dark:text-slate-400">{{ item.hint }}</div>
                  </div>
                </div>
              </div>

              <div class="grid gap-3 px-4 py-3 xl:grid-cols-[0.9fr,1.1fr]">
                <div class="subpanel-shell">
                  <div class="subpanel-header">
                    <div class="subpanel-title">控制动作</div>
                    <div class="subpanel-desc">危险操作集中在一行动作条里，避免页面碎片化。</div>
                  </div>
                  <div class="grid grid-cols-2 gap-2 p-3 lg:grid-cols-5">
                    <button class="rounded-md border border-slate-300 px-3 py-2 text-[12.5px] font-medium transition hover:bg-slate-100 dark:border-slate-700 dark:hover:bg-slate-800" :disabled="loading" @click="refreshAll()">手动刷新</button>
                    <button class="rounded-md border border-slate-300 px-3 py-2 text-[12.5px] font-medium transition hover:bg-slate-100 dark:border-slate-700 dark:hover:bg-slate-800" @click="toggleAutoRefresh">{{ autoRefreshEnabled ? '关闭轮询' : '开启轮询' }}</button>
                    <button class="rounded-md bg-emerald-600 px-3 py-2 text-[12.5px] font-medium text-white transition hover:bg-emerald-700" @click="startSystem">启动主控</button>
                    <button class="rounded-md bg-amber-500 px-3 py-2 text-[12.5px] font-medium text-white transition hover:bg-amber-600" @click="restartSystem">重启主控</button>
                    <button class="rounded-md bg-rose-600 px-3 py-2 text-[12.5px] font-medium text-white transition hover:bg-rose-700 col-span-2 lg:col-span-1" @click="stopSystem">停止主控</button>
                  </div>
                </div>

                <div class="subpanel-shell">
                  <div class="subpanel-header">
                    <div class="subpanel-title">控制面状态列表</div>
                    <div class="subpanel-desc">改成横向列表，不再用竖排小卡片堆叠。</div>
                  </div>
                  <div class="divide-y divide-slate-200 dark:divide-slate-800">
                    <div v-for="item in controlPulseRows" :key="item.label" class="grid gap-2 px-4 py-3 md:grid-cols-[140px,1fr,220px] md:items-center">
                      <div class="text-[12px] font-medium text-slate-600 dark:text-slate-300">{{ item.label }}</div>
                      <div class="text-[13px] font-semibold break-all text-slate-900 dark:text-slate-100">{{ item.value }}</div>
                      <div class="text-[11px] text-slate-500 dark:text-slate-400">{{ item.hint }}</div>
                    </div>
                  </div>
                </div>
              </div>
            </div>

          <div class="elevated-panel overflow-hidden">
            <div class="toolbar-strip">
              <div>
                <h3 class="subpanel-title">系统运行列表</h3>
                <p class="subpanel-desc">控制页统一收成横向列表/表格，不再拆成一堆卡片。</p>
              </div>
              <div class="result-badge">运行概览</div>
            </div>

            <div class="grid gap-2 p-3 sm:grid-cols-2 xl:grid-cols-4 2xl:grid-cols-8">
              <div v-for="item in controlWatchRows" :key="`watch-${item.label}`" class="metric-tile min-h-[88px] p-3">
                <div class="text-[10px] uppercase tracking-[0.14em] text-slate-400">{{ item.label }}</div>
                <div :class="['mt-2 text-[13px] font-semibold leading-5', item.tone === 'emerald' ? 'text-emerald-600 dark:text-emerald-400' : item.tone === 'rose' ? 'text-rose-600 dark:text-rose-400' : item.tone === 'amber' ? 'text-amber-600 dark:text-amber-400' : item.tone === 'sky' ? 'text-sky-600 dark:text-sky-400' : 'text-slate-700 dark:text-slate-200']">{{ item.value }}</div>
              </div>
            </div>

            <div class="panel-divider"></div>

            <div class="table-wrap m-3">
              <div class="overflow-auto">
                <table class="data-table min-w-full divide-y divide-slate-200 text-sm dark:divide-slate-800">
                  <thead class="bg-slate-50 dark:bg-slate-950/95">
                    <tr>
                      <th class="text-left font-medium text-slate-500">模块</th>
                      <th class="text-left font-medium text-slate-500">级别</th>
                      <th class="text-left font-medium text-slate-500">最近时间</th>
                      <th class="text-left font-medium text-slate-500">新鲜度</th>
                      <th class="text-left font-medium text-slate-500">摘要</th>
                    </tr>
                  </thead>
                  <tbody class="divide-y divide-slate-100 dark:divide-slate-800">
                    <tr v-for="item in workerHealthRows" :key="item.name" class="align-middle">
                      <td><div class="flex min-h-[44px] items-center font-medium">{{ item.name }}</div></td>
                      <td><div class="flex min-h-[44px] items-center"><span :class="['rounded-full px-2.5 py-1 text-xs font-medium', levelClass(item.level)]">{{ item.level }}</span></div></td>
                      <td class="tabular-nums text-slate-500"><div class="flex min-h-[44px] items-center">{{ item.time }}</div></td>
                      <td><div class="flex min-h-[44px] items-center"><span :class="['text-xs font-medium', freshnessClass(item.freshness)]">{{ item.freshness }}</span></div></td>
                      <td class="text-[12.5px] leading-5 text-slate-600 dark:text-slate-300">{{ item.summary }}</td>
                    </tr>
                    <tr v-if="!workerHealthRows.length">
                      <td colspan="5" class="py-6 text-center text-[13px] text-slate-500 dark:text-slate-400">暂无模块运行数据</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            <div class="panel-divider"></div>

            <div class="table-wrap m-3">
              <div class="overflow-auto max-h-[260px]">
                <table class="data-table min-w-full divide-y divide-slate-200 text-sm dark:divide-slate-800">
                  <thead class="sticky top-0 z-10 bg-slate-50 dark:bg-slate-950/95">
                    <tr>
                      <th class="text-left font-medium text-slate-500">时间</th>
                      <th class="text-left font-medium text-slate-500">动作</th>
                      <th class="text-left font-medium text-slate-500">结果</th>
                      <th class="text-left font-medium text-slate-500">说明</th>
                    </tr>
                  </thead>
                  <tbody class="divide-y divide-slate-100 dark:divide-slate-800">
                    <tr v-for="(item, index) in controlHistory" :key="`${item.time}-${item.type}-${index}`" class="align-middle">
                      <td class="tabular-nums text-slate-500"><div class="flex min-h-[44px] items-center">{{ item.time }}</div></td>
                      <td><div class="flex min-h-[44px] items-center"><span class="badge-soft text-xs">{{ item.type }}</span></div></td>
                      <td><div class="flex min-h-[44px] items-center"><span :class="['rounded-full px-2.5 py-1 text-xs font-medium', controlResultClass(item.result)]">{{ item.result }}</span></div></td>
                      <td class="whitespace-pre-wrap break-words text-[12.5px] text-slate-600 dark:text-slate-300">{{ item.detail || '--' }}</td>
                    </tr>
                    <tr v-if="!controlHistory.length">
                      <td colspan="4" class="py-6 text-center text-[13px] text-slate-500 dark:text-slate-400">暂无控制动作历史</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div class="grid gap-3 xl:grid-cols-[1fr,1fr]">
            <div class="elevated-panel overflow-hidden">
              <div class="toolbar-strip">
                <div>
                  <h3 class="subpanel-title">控制建议 / 最近告警</h3>
                  <p class="subpanel-desc">改成真正的表格列表，不再用 div 冒充列表。</p>
                </div>
                <div class="result-badge">重点事件</div>
              </div>
              <div class="table-wrap m-3">
                <div class="overflow-auto max-h-[420px]">
                  <table class="data-table min-w-full table-fixed divide-y divide-slate-200 text-sm dark:divide-slate-800">
                    <colgroup>
                      <col class="w-[92px]" />
                      <col class="w-[110px]" />
                      <col class="w-[170px]" />
                      <col />
                    </colgroup>
                    <thead class="sticky top-0 z-10 bg-slate-50 dark:bg-slate-950/95">
                      <tr>
                        <th class="text-left font-medium text-slate-500">级别</th>
                        <th class="text-left font-medium text-slate-500">分类</th>
                        <th class="text-left font-medium text-slate-500">时间 / 标题</th>
                        <th class="text-left font-medium text-slate-500">内容</th>
                      </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-100 dark:divide-slate-800">
                      <tr v-for="item in controlHints" :key="`${item.level}-${item.title}`" class="align-middle">
                        <td><div class="flex min-h-[44px] items-center"><span :class="['rounded-full px-2 py-0.5 text-[11px] font-medium', item.level === 'danger' ? 'bg-rose-500/15 text-rose-600 dark:text-rose-300' : item.level === 'warn' ? 'bg-amber-500/15 text-amber-600 dark:text-amber-300' : item.level === 'info' ? 'bg-sky-500/15 text-sky-600 dark:text-sky-300' : 'bg-emerald-500/15 text-emerald-600 dark:text-emerald-300']">{{ item.level === 'danger' ? '风险' : item.level === 'warn' ? '提醒' : item.level === 'info' ? '信息' : '正常' }}</span></div></td>
                        <td><div class="flex min-h-[44px] items-center"><span class="badge-soft text-xs">控制建议</span></div></td>
                        <td><div class="flex min-h-[44px] items-center text-[12px] font-semibold text-slate-700 dark:text-slate-200">{{ item.title }}</div></td>
                        <td class="whitespace-pre-wrap break-words text-[12.5px] text-slate-600 dark:text-slate-300">{{ item.detail }}</td>
                      </tr>
                      <tr v-for="(item, index) in recentAlerts.slice(0, 6)" :key="`control-alert-${item['时间']}-${index}`" class="align-middle">
                        <td><div class="flex min-h-[44px] items-center"><span :class="['rounded-full px-2 py-0.5 text-[11px] font-medium', alertBadgeClass(item)]">{{ item['级别'] }}</span></div></td>
                        <td><div class="flex min-h-[44px] items-center"><span :class="['rounded-full px-2 py-0.5 text-[11px] font-medium', categoryClass(item['分类'])]">{{ item['分类'] }}</span></div></td>
                        <td><div class="flex min-h-[44px] items-center tabular-nums text-[11px] text-slate-500 dark:text-slate-400">{{ item['时间'] }}</div></td>
                        <td class="whitespace-pre-wrap break-words text-[12.5px] text-slate-700 dark:text-slate-200">{{ item['内容'] }}</td>
                      </tr>
                      <tr v-if="!controlHints.length && !recentAlerts.length">
                        <td colspan="4" class="py-6 text-center text-[13px] text-slate-500 dark:text-slate-400">暂无重点事件</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>

            <div class="elevated-panel overflow-hidden">
              <div class="toolbar-strip">
                <div>
                  <h3 class="subpanel-title">CTA / 风控事件列表</h3>
                  <p class="subpanel-desc">全部压成横向表格，避免再出现一块块事件卡片。</p>
                </div>
                <div class="result-badge">事件流</div>
              </div>
              <div class="table-wrap m-3">
                <div class="overflow-auto max-h-[420px]">
                  <table class="data-table min-w-full table-fixed divide-y divide-slate-200 text-sm dark:divide-slate-800">
                    <colgroup>
                      <col class="w-[72px]" />
                      <col class="w-[150px]" />
                      <col class="w-[96px]" />
                      <col />
                    </colgroup>
                    <thead class="sticky top-0 z-10 bg-slate-50 dark:bg-slate-950/95">
                      <tr>
                        <th class="text-left font-medium text-slate-500">类型</th>
                        <th class="text-left font-medium text-slate-500">时间</th>
                        <th class="text-left font-medium text-slate-500">状态</th>
                        <th class="text-left font-medium text-slate-500">原始内容</th>
                      </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-100 dark:divide-slate-800">
                      <tr v-for="item in ctaEventRows.slice(0, 5)" :key="`cta-${item.time}-${item.label}`" class="align-middle">
                        <td><div class="flex min-h-[44px] items-center font-medium">CTA</div></td>
                        <td class="tabular-nums text-slate-500"><div class="flex min-h-[44px] items-center">{{ item.time }}</div></td>
                        <td><div class="flex min-h-[44px] items-center">{{ summarizeCtaEvent(item.label) }}</div></td>
                        <td class="whitespace-pre-wrap break-words text-[12.5px] text-slate-600 dark:text-slate-300">{{ item.raw }}</td>
                      </tr>
                      <tr v-for="item in riskEventRows.slice(0, 5)" :key="`risk-${item.time}-${item.blocked}`" class="align-middle">
                        <td><div class="flex min-h-[44px] items-center font-medium">风控</div></td>
                        <td class="tabular-nums text-slate-500"><div class="flex min-h-[44px] items-center">{{ item.time }}</div></td>
                        <td><div class="flex min-h-[44px] items-center">{{ item.blocked ? '已阻断' : '正常' }}</div></td>
                        <td class="whitespace-pre-wrap break-words text-[12.5px] text-slate-600 dark:text-slate-300">{{ item.raw }}</td>
                      </tr>
                      <tr v-if="!ctaEventRows.length && !riskEventRows.length">
                        <td colspan="4" class="py-6 text-center text-[13px] text-slate-500 dark:text-slate-400">暂无事件流数据</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        </section>

        <section v-if="currentView === 'trading'" id="positions" class="mt-4 space-y-3">
          <div class="grid gap-3 xl:grid-cols-[1.08fr,0.92fr]">
            <div class="elevated-panel">
              <div class="panel-header">
                <div>
                  <div class="panel-kicker">Execution Overview</div>
                  <h3 class="panel-title">交易执行页</h3>
                  <p class="panel-desc">先看持仓，再看挂单，把执行面压成更像交易后台的横向结构。</p>
                </div>
                <div class="result-badge">刷新于 {{ latestRefreshTime }}</div>
              </div>
              <div class="stat-strip rounded-none border-0 md:grid-cols-3">
                <div v-for="item in positionSummaryCards" :key="item.label" class="stat-cell">
                  <div class="text-[10px] uppercase tracking-[0.14em] text-slate-400">{{ item.label }}</div>
                  <div :class="['mt-1 text-[18px] font-semibold tracking-tight', item.tone === 'emerald' ? 'text-emerald-600 dark:text-emerald-400' : item.tone === 'rose' ? 'text-rose-600 dark:text-rose-400' : item.tone === 'amber' ? 'text-amber-600 dark:text-amber-400' : 'text-slate-900 dark:text-slate-100']">{{ item.value }}</div>
                  <div class="mt-1 text-[11px] text-slate-500 dark:text-slate-400">{{ item.hint }}</div>
                </div>
              </div>
              <div class="overflow-auto">
                <table class="data-table min-w-full divide-y divide-slate-200 text-sm dark:divide-slate-800">
                <thead class="sticky top-0 z-10 bg-slate-50 dark:bg-slate-950/95">
                  <tr>
                    <th class="px-3 py-2.5 text-left font-medium text-slate-500"><button class="inline-flex items-center gap-1" @click="toggleSort(positionSort, '交易对')">交易对 <span class="text-[11px]">{{ sortIndicator(positionSort, '交易对') }}</span></button></th>
                    <th class="px-3 py-2.5 text-left font-medium text-slate-500"><button class="inline-flex items-center gap-1" @click="toggleSort(positionSort, '方向')">方向 <span class="text-[11px]">{{ sortIndicator(positionSort, '方向') }}</span></button></th>
                    <th class="px-3 py-2.5 text-right font-medium text-slate-500"><button class="inline-flex items-center gap-1" @click="toggleSort(positionSort, '合约数量')">数量 <span class="text-[11px]">{{ sortIndicator(positionSort, '合约数量') }}</span></button></th>
                    <th class="px-3 py-2.5 text-right font-medium text-slate-500"><button class="inline-flex items-center gap-1" @click="toggleSort(positionSort, '开仓均价')">开仓均价 <span class="text-[11px]">{{ sortIndicator(positionSort, '开仓均价') }}</span></button></th>
                    <th class="px-3 py-2.5 text-right font-medium text-slate-500"><button class="inline-flex items-center gap-1" @click="toggleSort(positionSort, '标记价格')">标记价格 <span class="text-[11px]">{{ sortIndicator(positionSort, '标记价格') }}</span></button></th>
                    <th class="px-3 py-2.5 text-right font-medium text-slate-500"><button class="inline-flex items-center gap-1" @click="toggleSort(positionSort, '未实现盈亏')">未实现盈亏 <span class="text-[11px]">{{ sortIndicator(positionSort, '未实现盈亏') }}</span></button></th>
                    <th class="px-3 py-2.5 text-right font-medium text-slate-500"><button class="inline-flex items-center gap-1" @click="toggleSort(positionSort, '名义价值')">名义价值 <span class="text-[11px]">{{ sortIndicator(positionSort, '名义价值') }}</span></button></th>
                  </tr>
                </thead>
                <tbody class="divide-y divide-slate-100 dark:divide-slate-800">
                  <tr v-for="item in sortedPositions" :key="`${item['交易对']}-${item['方向']}`" class="align-middle hover:bg-slate-50 dark:hover:bg-slate-800/40">
                    <td class="px-3 py-2.5 font-medium"><div class="flex min-h-[44px] items-center">{{ item['交易对'] }}</div></td>
                    <td class="px-3 py-2.5"><div class="flex min-h-[44px] items-center"><span :class="['rounded-full px-2.5 py-1 text-xs font-medium', positionSideClass(item['方向'])]">{{ item['方向'] }}</span></div></td>
                    <td class="px-4 py-3 text-right tabular-nums">{{ formatNumber(item['合约数量'], 4) }}</td>
                    <td class="px-4 py-3 text-right tabular-nums">{{ formatNumber(item['开仓均价'], 2) }}</td>
                    <td class="px-4 py-3 text-right tabular-nums">{{ formatNumber(item['标记价格'], 2) }}</td>
                    <td :class="['px-4 py-3 text-right font-semibold tabular-nums', pnlClass(item['未实现盈亏'])]">{{ signedNumber(item['未实现盈亏']) }}</td>
                    <td class="px-4 py-3 text-right tabular-nums">{{ formatNumber(item['名义价值'], 2) }}</td>
                  </tr>
                  <tr v-if="!sortedPositions.length">
                    <td colspan="7" class="px-4 py-6 text-center text-[13px] text-slate-500 dark:text-slate-400">当前没有持仓</td>
                  </tr>
                </tbody>
                </table>
              </div>
            </div>

            <div class="space-y-4">
              <div class="elevated-panel">
                <div class="subpanel-header">
                  <div class="subpanel-title">执行焦点</div>
                  <div class="subpanel-desc">把最值得盯的持仓和挂单单独抽出来。</div>
                </div>
                <div class="space-y-3 p-3.5">
                  <div v-for="item in tradingFocusRows" :key="item.label" class="metric-tile p-3.5">
                    <div class="text-[10px] uppercase tracking-[0.14em] text-slate-400">{{ item.label }}</div>
                    <div class="mt-2 text-[13px] font-semibold">{{ item.title }}</div>
                    <div class="subpanel-desc">{{ item.detail }}</div>
                  </div>
                </div>
              </div>

              <div id="orders" class="elevated-panel">
                <div class="subpanel-header">
                  <div class="subpanel-title">委托摘要</div>
                  <div class="subpanel-desc">优先看挂单数量、结构和 reduce-only 占比。</div>
                </div>
                <div class="stat-strip rounded-none border-0 md:grid-cols-3">
                  <div v-for="item in orderSummaryCards" :key="item.label" class="stat-cell">
                    <div class="text-[10px] uppercase tracking-[0.14em] text-slate-400">{{ item.label }}</div>
                    <div class="mt-1 text-[16px] font-semibold tracking-tight">{{ item.value }}</div>
                    <div class="mt-1 text-[11px] text-slate-500 dark:text-slate-400">{{ item.hint }}</div>
                  </div>
                </div>
                <div class="overflow-auto">
                  <table class="data-table min-w-full divide-y divide-slate-200 text-sm dark:divide-slate-800">
                    <thead class="sticky top-0 z-10 bg-slate-50 dark:bg-slate-950/95">
                      <tr>
                        <th class="px-3 py-2.5 text-left font-medium text-slate-500"><button class="inline-flex items-center gap-1" @click="toggleSort(orderSort, '交易对')">交易对 <span class="text-[11px]">{{ sortIndicator(orderSort, '交易对') }}</span></button></th>
                        <th class="px-3 py-2.5 text-left font-medium text-slate-500"><button class="inline-flex items-center gap-1" @click="toggleSort(orderSort, '方向')">方向 <span class="text-[11px]">{{ sortIndicator(orderSort, '方向') }}</span></button></th>
                        <th class="px-3 py-2.5 text-left font-medium text-slate-500"><button class="inline-flex items-center gap-1" @click="toggleSort(orderSort, '类型')">类型 <span class="text-[11px]">{{ sortIndicator(orderSort, '类型') }}</span></button></th>
                        <th class="px-3 py-2.5 text-right font-medium text-slate-500"><button class="inline-flex items-center gap-1" @click="toggleSort(orderSort, '价格')">价格 <span class="text-[11px]">{{ sortIndicator(orderSort, '价格') }}</span></button></th>
                        <th class="px-3 py-2.5 text-right font-medium text-slate-500"><button class="inline-flex items-center gap-1" @click="toggleSort(orderSort, '数量')">数量 <span class="text-[11px]">{{ sortIndicator(orderSort, '数量') }}</span></button></th>
                        <th class="px-3 py-2.5 text-right font-medium text-slate-500"><button class="inline-flex items-center gap-1" @click="toggleSort(orderSort, '剩余')">剩余 <span class="text-[11px]">{{ sortIndicator(orderSort, '剩余') }}</span></button></th>
                        <th class="px-3 py-2.5 text-center font-medium text-slate-500"><button class="inline-flex items-center gap-1" @click="toggleSort(orderSort, '仅减仓')">仅减仓 <span class="text-[11px]">{{ sortIndicator(orderSort, '仅减仓') }}</span></button></th>
                      </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-100 dark:divide-slate-800">
                      <tr v-for="item in sortedOrders" :key="item['订单ID']" class="align-middle hover:bg-slate-50 dark:hover:bg-slate-800/40">
                        <td class="px-3 py-2.5 font-medium"><div class="flex min-h-[44px] items-center">{{ item['交易对'] }}</div></td>
                        <td class="px-3 py-2.5"><div class="flex min-h-[44px] items-center"><span :class="['rounded-full px-2.5 py-1 text-xs font-medium', positionSideClass(item['方向'])]">{{ item['方向'] }}</span></div></td>
                        <td class="px-3 py-2.5"><span :class="['rounded-full px-2.5 py-1 text-xs font-medium', orderTypeClass(item['类型'])]">{{ item['类型'] }}</span></td>
                        <td class="px-4 py-3 text-right tabular-nums">{{ formatNumber(item['价格'], 2) }}</td>
                        <td class="px-4 py-3 text-right tabular-nums">{{ formatNumber(item['数量'], 4) }}</td>
                        <td class="px-4 py-3 text-right tabular-nums">{{ formatNumber(item['剩余'], 4) }}</td>
                        <td class="px-4 py-3 text-center">{{ boolText(item['仅减仓']) }}</td>
                      </tr>
                      <tr v-if="!sortedOrders.length">
                        <td colspan="7" class="px-4 py-6 text-center text-[13px] text-slate-500 dark:text-slate-400">当前没有委托</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        </section>

        <section v-if="currentView === 'logs'" id="logs" class="mt-4 space-y-3">
          <div class="card-shell dark:bg-slate-900">
            <div class="panel-header items-start py-3.5">
              <div>
                <div class="panel-kicker">Audit Console</div>
                <h3 class="panel-title">日志审计页</h3>
                <p class="panel-desc">把筛选、统计、重点事件和主日志表拆成标准审计台结构。</p>
              </div>
              <div class="flex flex-wrap items-center gap-2">
                <span class="result-badge">结果 {{ logTableRows.length }}</span>
                <button class="filter-pill filter-pill-muted" @click="logKeyword = ''; logQuickFilter = '全部'; logFilter = '全部'; moduleFilter = '全部模块'">清空筛选</button>
              </div>
            </div>

            <div class="grid gap-4 px-4 py-4 xl:grid-cols-[360px,minmax(0,1fr)]">
              <div class="space-y-4 xl:sticky xl:top-4 self-start">
                <div class="subpanel-shell">
                  <div class="subpanel-header">
                    <div class="subpanel-title">快速筛选</div>
                    <div class="subpanel-desc">先用预设视角切日志，再做二次检索。</div>
                  </div>
                  <div class="space-y-3 p-3">
                    <div class="flex flex-wrap gap-2">
                      <button v-for="item in logQuickFilters" :key="item" :class="['filter-pill', logQuickFilter === item ? 'filter-pill-rose-active' : 'filter-pill-muted']" @click="logQuickFilter = item">{{ item }}</button>
                    </div>
                    <div>
                      <input v-model="logKeyword" type="text" placeholder="搜索时间 / 模块 / 分类 / 级别 / 内容" class="w-full rounded-lg border border-slate-300 bg-white px-4 py-3 text-sm outline-none transition focus:border-slate-500 dark:border-slate-700 dark:bg-slate-950" />
                    </div>
                  </div>
                </div>

                <div class="subpanel-shell">
                  <div class="subpanel-header">
                    <div class="subpanel-title">分类 / 模块</div>
                    <div class="subpanel-desc">筛选后，右侧主日志表会立刻响应。</div>
                  </div>
                  <div class="space-y-3 p-3">
                    <div>
                      <div class="mb-2 text-[11px] uppercase tracking-[0.14em] text-slate-400">分类</div>
                      <div class="flex flex-wrap gap-2">
                        <button v-for="item in logCategories" :key="item" :class="['filter-pill', logFilter === item ? 'filter-pill-dark-active' : 'filter-pill-muted']" @click="logFilter = item">{{ item }}</button>
                      </div>
                    </div>
                    <div>
                      <div class="mb-2 text-[11px] uppercase tracking-[0.14em] text-slate-400">模块</div>
                      <div class="flex flex-wrap gap-2">
                        <button v-for="item in logModules" :key="item" :class="['filter-pill', moduleFilter === item ? 'filter-pill-sky-active' : 'filter-pill-muted']" @click="moduleFilter = item">{{ item }}</button>
                      </div>
                    </div>
                  </div>
                </div>

                <div class="stat-strip sm:grid-cols-2">
                  <div v-for="item in logAuditSummaryCards" :key="item.label" class="stat-cell">
                    <div class="text-[10px] uppercase tracking-[0.14em] text-slate-400">{{ item.label }}</div>
                    <div :class="['mt-1 text-[18px] font-semibold tracking-tight', item.tone === 'rose' ? 'text-rose-600 dark:text-rose-400' : item.tone === 'amber' ? 'text-amber-600 dark:text-amber-400' : item.tone === 'sky' ? 'text-sky-600 dark:text-sky-400' : 'text-slate-900 dark:text-slate-100']">{{ item.value }}</div>
                    <div class="mt-1 text-[11px] text-slate-500 dark:text-slate-400">{{ item.hint }}</div>
                  </div>
                </div>
              </div>

              <div class="space-y-4 min-w-0">
                <div class="elevated-panel overflow-hidden dark:bg-slate-900">
                  <div class="toolbar-strip">
                    <div>
                      <h4 class="subpanel-title">审计主日志表</h4>
                      <p class="subpanel-desc">筛选区和结果表放到同一视线层级，避免误以为筛选没反应。</p>
                    </div>
                    <div class="flex items-center gap-2">
                      <span class="result-badge">结果 {{ logTableRows.length }}</span>
                      <span class="result-badge">按时间倒序 / 支持排序</span>
                    </div>
                  </div>
                  <div class="max-h-[calc(100vh-260px)] overflow-auto">
                    <table class="data-table min-w-full table-fixed divide-y divide-slate-200 text-sm dark:divide-slate-800">
                      <colgroup>
                        <col class="w-[160px]" />
                        <col class="w-[110px]" />
                        <col class="w-[92px]" />
                        <col class="w-[88px]" />
                        <col />
                      </colgroup>
                      <thead class="sticky top-0 z-10 bg-slate-50 dark:bg-slate-950/95">
                        <tr>
                          <th class="text-left font-medium text-slate-500"><button class="inline-flex items-center gap-1" @click="toggleSort(logSort, '时间')">时间 <span class="text-[11px]">{{ sortIndicator(logSort, '时间') }}</span></button></th>
                          <th class="text-left font-medium text-slate-500"><button class="inline-flex items-center gap-1" @click="toggleSort(logSort, '模块')">模块 <span class="text-[11px]">{{ sortIndicator(logSort, '模块') }}</span></button></th>
                          <th class="text-left font-medium text-slate-500"><button class="inline-flex items-center gap-1" @click="toggleSort(logSort, '分类')">分类 <span class="text-[11px]">{{ sortIndicator(logSort, '分类') }}</span></button></th>
                          <th class="text-left font-medium text-slate-500"><button class="inline-flex items-center gap-1" @click="toggleSort(logSort, '级别')">级别 <span class="text-[11px]">{{ sortIndicator(logSort, '级别') }}</span></button></th>
                          <th class="text-left font-medium text-slate-500">内容</th>
                        </tr>
                      </thead>
                      <tbody class="divide-y divide-slate-100 dark:divide-slate-800">
                        <tr v-for="(line, index) in logTableRows" :key="`${index}-${line['内容']}`" class="align-middle">
                          <td class="tabular-nums text-slate-500">{{ line['时间'] }}</td>
                          <td class="truncate">{{ line['模块'] }}</td>
                          <td><div class="flex min-h-[44px] items-center"><span :class="['rounded-full px-2 py-0.5 text-[11px] font-medium', categoryClass(line['分类'])]">{{ line['分类'] }}</span></div></td>
                          <td><div class="flex min-h-[44px] items-center"><span :class="['rounded-full px-2 py-0.5 text-[11px] font-medium', levelClass(line['级别'])]">{{ line['级别'] }}</span></div></td>
                          <td>
                            <div class="whitespace-pre-wrap break-words leading-5 text-[12.5px] text-slate-700 dark:text-slate-200">{{ line['内容'] }}</div>
                          </td>
                        </tr>
                        <tr v-if="!logTableRows.length">
                          <td colspan="5" class="py-6 text-center text-[13px] text-slate-500 dark:text-slate-400">当前筛选下没有日志</td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <div class="elevated-panel overflow-hidden dark:bg-slate-900">
            <div class="toolbar-strip">
              <div>
                <h4 class="subpanel-title">重点审计事件</h4>
                <p class="subpanel-desc">把错误、警告、风控最新命中单独拉出来，作为筛选结果之外的重点视图。</p>
              </div>
              <div class="result-badge">重点事件</div>
            </div>
            <div class="grid gap-4 px-4 py-4 xl:grid-cols-[1fr,360px]">
              <div class="table-wrap">
                <div class="overflow-auto max-h-[420px]">
                  <table class="data-table min-w-full table-fixed divide-y divide-slate-200 text-sm dark:divide-slate-800">
                    <colgroup>
                      <col class="w-[92px]" />
                      <col class="w-[110px]" />
                      <col class="w-[170px]" />
                      <col />
                    </colgroup>
                    <thead class="sticky top-0 z-10 bg-slate-50 dark:bg-slate-950/95">
                      <tr>
                        <th class="text-left font-medium text-slate-500">级别</th>
                        <th class="text-left font-medium text-slate-500">分类</th>
                        <th class="text-left font-medium text-slate-500">时间 / 标题</th>
                        <th class="text-left font-medium text-slate-500">内容</th>
                      </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-100 dark:divide-slate-800">
                      <tr v-for="item in recentAlerts" :key="`${item['时间']}-${item['模块']}-${item['内容']}`" class="align-middle">
                        <td><div class="flex min-h-[44px] items-center"><span :class="['rounded-full px-2 py-0.5 text-[11px] font-medium', levelClass(item['级别'])]">{{ item['级别'] }}</span></div></td>
                        <td><div class="flex min-h-[44px] items-center"><span :class="['rounded-full px-2 py-0.5 text-[11px] font-medium', categoryClass(item['分类'])]">{{ item['分类'] }}</span></div></td>
                        <td class="text-[12px] leading-5">
                          <div class="font-medium text-slate-700 dark:text-slate-200">{{ item['模块'] }}</div>
                          <div class="mt-1 tabular-nums text-slate-500">{{ item['时间'] }}</div>
                        </td>
                        <td><div class="whitespace-pre-wrap break-words leading-5 text-[12.5px] text-slate-700 dark:text-slate-200">{{ item['内容'] }}</div></td>
                      </tr>
                      <tr v-if="!recentAlerts.length">
                        <td colspan="4" class="py-6 text-center text-[13px] text-slate-500 dark:text-slate-400">暂无重点审计事件</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>

              <div class="space-y-4">
                <div v-for="item in logFocusRows" :key="item.label" class="metric-tile p-3.5">
                  <div class="flex items-center justify-between gap-3">
                    <div class="text-[10px] uppercase tracking-[0.14em] text-slate-400">{{ item.label }}</div>
                    <span :class="['rounded-full px-2 py-0.5 text-[10px] font-medium', item.tone === 'rose' ? 'bg-rose-500/15 text-rose-600 dark:text-rose-300' : item.tone === 'amber' ? 'bg-amber-500/15 text-amber-600 dark:text-amber-300' : item.tone === 'orange' ? 'bg-orange-500/15 text-orange-600 dark:text-orange-300' : 'bg-slate-500/15 text-slate-600 dark:text-slate-300']">{{ item.tone === 'slate' ? '空' : '命中' }}</span>
                  </div>
                  <div class="mt-2 text-[13px] font-semibold">{{ item.title }}</div>
                  <div class="mt-1 line-clamp-3 text-xs break-all text-slate-500 dark:text-slate-400">{{ item.detail }}</div>
                </div>
              </div>
            </div>
          </div>
        </section>

        <section v-if="currentView === 'finance'" id="finance" class="mt-4 space-y-3">
          <div class="elevated-panel overflow-hidden dark:bg-slate-900">
            <div class="panel-header items-start py-3.5">
              <div>
                <div class="panel-kicker">Capital Calendar</div>
                <h3 class="panel-title">资金日历</h3>
                <p class="panel-desc">集中管理初始资金基线，并像交易所资产页一样查看每月每天的盈亏结算。</p>
              </div>
              <div class="flex flex-wrap items-center gap-2">
                <button class="rounded-md border border-slate-300 px-3.5 py-2 text-[13px] font-medium transition hover:bg-slate-100 dark:border-slate-700 dark:hover:bg-slate-800" @click="refreshAll()">刷新数据</button>
              </div>
            </div>

            <div class="grid gap-3 px-4 py-4 xl:grid-cols-[420px,minmax(0,1fr)]">
              <div class="subpanel-shell overflow-hidden border-sky-200/80 dark:border-sky-900/50">
                <div class="subpanel-header bg-sky-50/70 dark:bg-sky-950/20">
                  <div class="subpanel-title">初始资金</div>
                  <div class="subpanel-desc">总盈亏统计基于数据库中的初始资金基线，不再依赖 YAML 静态值。</div>
                </div>
                <div class="space-y-3 p-4">
                  <div class="grid gap-2 sm:grid-cols-2">
                    <div class="rounded-xl border border-sky-300 bg-gradient-to-br from-sky-50 to-sky-100/80 p-3 shadow-sm dark:border-sky-900/50 dark:bg-sky-950/25 dark:bg-none">
                      <div class="text-[11px] text-sky-700/70 dark:text-sky-300/70">当前基线</div>
                      <div class="mt-1 text-[20px] font-semibold tracking-tight text-sky-900 dark:text-sky-100">{{ formatNumber(initialEquity, 2) }}</div>
                    </div>
                    <div class="rounded-xl border border-violet-300 bg-gradient-to-br from-violet-50 to-violet-100/80 p-3 shadow-sm dark:border-violet-900/50 dark:bg-violet-950/25 dark:bg-none">
                      <div class="text-[11px] text-violet-700/70 dark:text-violet-300/70">当前权益</div>
                      <div class="mt-1 text-[20px] font-semibold tracking-tight text-violet-900 dark:text-violet-100">{{ formatNumber(overview?.['账户权益'], 2) }}</div>
                    </div>
                    <div :class="['rounded-xl border p-3 shadow-sm', financeBaselineStats.totalPnl >= 0 ? 'border-emerald-200 bg-emerald-50/85 dark:border-emerald-900/50 dark:bg-emerald-950/25' : 'border-rose-200 bg-rose-50/85 dark:border-rose-900/50 dark:bg-rose-950/25']">
                      <div :class="['text-[11px]', financeBaselineStats.totalPnl >= 0 ? 'text-emerald-700/70 dark:text-emerald-300/70' : 'text-rose-700/70 dark:text-rose-300/70']">总盈亏</div>
                      <div :class="['mt-1 text-[20px] font-semibold tracking-tight', pnlClass(financeBaselineStats.totalPnl)]">{{ financeBaselineStats.totalPnlText }}</div>
                    </div>
                    <div :class="['rounded-xl border p-3 shadow-sm', financeBaselineStats.totalPnl >= 0 ? 'border-emerald-300 bg-gradient-to-br from-emerald-50 to-emerald-100/80 dark:border-emerald-900/50 dark:bg-emerald-950/25 dark:bg-none' : 'border-rose-300 bg-gradient-to-br from-rose-50 to-rose-100/80 dark:border-rose-900/50 dark:bg-rose-950/25 dark:bg-none']">
                      <div :class="['text-[11px]', financeBaselineStats.totalPnl >= 0 ? 'text-emerald-700/70 dark:text-emerald-300/70' : 'text-rose-700/70 dark:text-rose-300/70']">总盈亏率</div>
                      <div :class="['mt-1 text-[20px] font-semibold tracking-tight', pnlClass(financeBaselineStats.totalPnl)]">{{ financeBaselineStats.totalPnlRateText }}</div>
                    </div>
                  </div>
                  <div>
                    <label class="mb-2 block text-[12px] font-medium text-slate-600 dark:text-slate-300">修改初始资金（USDT）</label>
                    <div class="flex gap-2">
                      <input v-model="initialEquityInput" type="number" step="0.01" class="config-input max-w-[220px]" />
                      <button class="min-w-[108px] whitespace-nowrap rounded-md bg-slate-900 px-3.5 py-2 text-[13px] font-medium text-white transition hover:bg-slate-700 dark:bg-slate-100 dark:text-slate-900 dark:hover:bg-slate-300" :disabled="initialEquitySaving" @click="saveInitialEquity">{{ initialEquitySaving ? '保存中...' : '保存基线' }}</button>
                    </div>
                  </div>
                  <div class="text-[12px] text-slate-500 dark:text-slate-400">
                    来源：{{ initialEquityMeta.source || '--' }}
                    <span v-if="initialEquityMeta.updatedAt"> · 更新时间：{{ formatDateTime(initialEquityMeta.updatedAt) }}</span>
                  </div>
                </div>
              </div>

              <div class="subpanel-shell overflow-hidden border-amber-200/80 dark:border-amber-900/40">
                <div class="toolbar-strip bg-amber-50/65 dark:bg-amber-950/15">
                  <div>
                    <div class="subpanel-title">{{ financeMonthLabel }}</div>
                    <div class="subpanel-desc">上方显示当前月份总盈亏，下方日历按天展示 USDT 结算结果。</div>
                  </div>
                  <div class="flex items-center gap-2">
                    <button class="filter-pill filter-pill-muted" @click="changeFinanceMonth(-1)">上个月</button>
                    <span class="result-badge">{{ financeMonth }}</span>
                    <button class="filter-pill filter-pill-muted" @click="changeFinanceMonth(1)">下个月</button>
                  </div>
                </div>
                <div class="grid gap-2 p-4 sm:grid-cols-3 xl:grid-cols-4">
                  <div :class="['rounded-xl border p-3 shadow-sm', Number(financeCalendar?.monthTotalPnl || 0) >= 0 ? 'border-emerald-200 bg-emerald-50/85 dark:border-emerald-900/50 dark:bg-emerald-950/25' : 'border-rose-200 bg-rose-50/85 dark:border-rose-900/50 dark:bg-rose-950/25']">
                    <div :class="['text-[11px]', Number(financeCalendar?.monthTotalPnl || 0) >= 0 ? 'text-emerald-700/70 dark:text-emerald-300/70' : 'text-rose-700/70 dark:text-rose-300/70']">月度总盈亏</div>
                    <div :class="['mt-1 text-[20px] font-semibold tracking-tight', pnlClass(financeCalendar?.monthTotalPnl)]">{{ financeMonthSummary.totalPnl }}</div>
                  </div>
                  <div class="rounded-xl border border-amber-200 bg-amber-50/80 p-3 shadow-sm dark:border-amber-900/50 dark:bg-amber-950/20">
                    <div class="text-[11px] text-amber-700/70 dark:text-amber-300/70">已记录天数</div>
                    <div class="mt-1 text-[20px] font-semibold tracking-tight text-amber-900 dark:text-amber-100">{{ financeMonthSummary.dayCount }}</div>
                  </div>
                  <div class="rounded-xl border border-sky-200 bg-sky-50/80 p-3 shadow-sm dark:border-sky-900/50 dark:bg-sky-950/20">
                    <div class="text-[11px] text-sky-700/70 dark:text-sky-300/70">初始资金</div>
                    <div class="mt-1 text-[20px] font-semibold tracking-tight text-sky-900 dark:text-sky-100">{{ financeMonthSummary.initialEquity }}</div>
                  </div>
                  <div class="rounded-xl border border-violet-300 bg-gradient-to-br from-violet-50 to-violet-100/80 p-3 shadow-sm dark:border-violet-900/50 dark:bg-violet-950/20 dark:bg-none">
                    <div class="text-[11px] text-violet-700/70 dark:text-violet-300/70">当前查看月份</div>
                    <div class="mt-1 text-[16px] font-semibold tracking-tight text-violet-900 dark:text-violet-100">{{ financeMonth }}</div>
                  </div>
                </div>

                <div class="grid grid-cols-7 gap-2 px-4 pb-2 text-center text-[11px] font-medium text-slate-400">
                  <div>一</div>
                  <div>二</div>
                  <div>三</div>
                  <div>四</div>
                  <div>五</div>
                  <div>六</div>
                  <div>日</div>
                </div>
                <div class="grid grid-cols-7 gap-2 bg-slate-50/55 p-4 pt-0 dark:bg-slate-950/15">
                  <div v-for="cell in financeCalendarCells" :key="cell.key" class="min-h-[108px]">
                    <div v-if="cell.empty" class="h-full rounded-xl border border-dashed border-slate-200 bg-transparent dark:border-slate-800"></div>
                    <div v-else :class="['h-full rounded-xl border p-2.5 shadow-sm', calendarDayShellClass(cell.item)]">
                      <div class="flex items-center justify-between gap-2">
                        <div class="text-[15px] font-semibold leading-none">{{ cell.day }}</div>
                        <span v-if="cell.item?.kind === 'live'" class="rounded-full bg-sky-500/15 px-2 py-0.5 text-[10px] font-medium text-sky-600 dark:text-sky-300">实时</span>
                      </div>
                      <div class="mt-3">
                        <div class="text-[10px] text-slate-400">日盈亏</div>
                        <div :class="['mt-1 text-[13px] font-semibold leading-5', pnlClass(cell.item?.dailyPnl)]">{{ cell.item ? signedNumber(cell.item.dailyPnl, 2) : '--' }}</div>
                      </div>
                      <div class="mt-2 text-[10px] text-slate-400">结算权益</div>
                      <div class="mt-1 text-[12px] font-medium text-slate-700 dark:text-slate-200">{{ cell.item ? formatNumber(cell.item.equity, 2) : '--' }}</div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </section>

        <section v-if="currentView === 'config'" id="config" class="mt-4 space-y-3">
          <div class="elevated-panel overflow-hidden dark:bg-slate-900">
            <div class="panel-header items-start py-3.5">
              <div>
                <div class="panel-kicker">System Config</div>
                <h3 class="panel-title">系统配置</h3>
                <p class="panel-desc">严格按大类分类展示。先切模块 Tab，再切分组 Tab，避免整页过长和输入框横向失控。</p>
              </div>
              <div class="flex flex-wrap items-center gap-2">
                <button class="rounded-md border border-slate-300 px-3.5 py-2 text-[13px] font-medium transition hover:bg-slate-100 dark:border-slate-700 dark:hover:bg-slate-800" @click="loadConfigSections">重新加载</button>
                <button class="rounded-md bg-slate-900 px-3.5 py-2 text-[13px] font-medium text-white transition hover:bg-slate-700 dark:bg-slate-100 dark:text-slate-900 dark:hover:bg-slate-300" :disabled="configSaving" @click="saveConfigSections">{{ configSaving ? '保存中...' : '保存配置' }}</button>
              </div>
            </div>

            <div class="config-shell px-4 py-4">
              <template v-if="configSections.length && activeConfigSectionData">
                <div class="config-top-tabs overflow-x-auto pb-1">
                  <div class="flex min-w-max gap-2">
                    <button
                      v-for="section in configSections"
                      :key="section.section"
                      :class="['config-main-tab', configSectionTabClass(section.section)]"
                      @click="selectConfigSection(section.section)"
                    >
                      {{ section.label }}
                    </button>
                  </div>
                </div>

                <div class="config-page mt-4 space-y-4">
                  <div class="subpanel-shell overflow-hidden">
                    <div class="panel-header border-b-0 py-3">
                      <div>
                        <div class="subpanel-title">{{ activeConfigSectionData.label }}</div>
                        <div class="subpanel-desc">{{ activeConfigSectionData.description }}</div>
                      </div>
                      <div class="flex flex-wrap items-center gap-2 text-[11px] text-slate-400">
                        <span class="result-badge">大类 {{ configStats.sectionCount }}</span>
                        <span class="result-badge">分组 {{ configStats.groupCount }}</span>
                        <span class="result-badge">参数 {{ configStats.fieldCount }}</span>
                        <span class="result-badge">高影响 {{ configStats.highImpactCount }}</span>
                      </div>
                    </div>

                    <div class="border-t border-slate-200 px-4 py-3 dark:border-slate-800">
                      <div class="config-sub-tabs overflow-x-auto pb-1">
                        <div class="flex min-w-max gap-2">
                          <button
                            v-for="group in activeConfigGroups"
                            :key="`${activeConfigSectionData.section}-${group.name}`"
                            :class="['config-sub-tab', configGroupTabClass(group.name)]"
                            @click="selectConfigGroup(group.name)"
                          >
                            <span>{{ group.name }}</span>
                            <span class="text-[11px] opacity-70">{{ group.items.length }}</span>
                          </button>
                        </div>
                      </div>
                    </div>
                  </div>

                  <div v-if="activeConfigGroupData" class="subpanel-shell overflow-hidden">
                    <div class="subpanel-header">
                      <div>
                        <div class="subpanel-title">{{ activeConfigGroupData.name }}</div>
                        <div class="subpanel-desc">当前仅显示这个分组的参数，避免系统配置页无限拉长。</div>
                      </div>
                    </div>

                    <div class="divide-y divide-slate-200 dark:divide-slate-800">
                      <div
                        v-for="field in activeConfigGroupData.items"
                        :key="field.path"
                        class="config-row"
                      >
                        <div class="config-row__meta">
                          <div class="flex flex-wrap items-center gap-2">
                            <div class="text-[12.5px] font-semibold text-slate-800 dark:text-slate-100">{{ field.label }}</div>
                            <span v-if="field.highImpact" class="rounded-full bg-rose-500/15 px-2 py-0.5 text-[10px] font-medium text-rose-600 dark:text-rose-300">高影响</span>
                          </div>
                          <div class="mt-1 break-all text-[11px] text-slate-400">{{ field.path }}</div>
                          <div class="mt-2 text-[12px] leading-5 text-slate-500 dark:text-slate-400">{{ field.description }}</div>
                        </div>

                        <div class="config-row__editor">
                          <template v-if="field.type === 'boolean'">
                            <label class="inline-flex min-h-[38px] items-center gap-2 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-[13px] dark:border-slate-800 dark:bg-slate-900">
                              <input v-model="field.value" type="checkbox" class="h-4 w-4 rounded border-slate-300 text-slate-900 focus:ring-slate-500" />
                              <span>启用 / 允许</span>
                            </label>
                          </template>
                          <template v-else-if="field.type === 'select'">
                            <select v-model="field.value" class="config-input">
                              <option v-for="option in field.options || []" :key="option" :value="option">{{ option }}</option>
                            </select>
                          </template>
                          <template v-else>
                            <input v-model="field.value" :type="field.type === 'number' ? 'number' : 'text'" :step="field.step || 'any'" class="config-input" />
                          </template>
                        </div>

                        <div class="config-row__status">
                          <div><span :class="['rounded-full px-2.5 py-1 text-[11px] font-medium', field.mutable ? 'bg-emerald-500/15 text-emerald-600 dark:text-emerald-300' : 'bg-slate-500/15 text-slate-600 dark:text-slate-300']">{{ field.mutable ? '可修改' : '只读' }}</span></div>
                          <div><span :class="['rounded-full px-2.5 py-1 text-[11px] font-medium', field.restartRequired ? 'bg-amber-500/15 text-amber-600 dark:text-amber-300' : 'bg-sky-500/15 text-sky-600 dark:text-sky-300']">{{ field.restartRequired ? '需重启' : '即时读取' }}</span></div>
                          <div class="text-[11.5px] leading-5 text-slate-500 dark:text-slate-400">{{ field.restartRequired ? '保存后建议重启主控生效' : '保存后可直接生效/读取' }}</div>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </template>

              <div v-else class="rounded-lg border border-dashed border-slate-300 px-4 py-8 text-center text-[13px] text-slate-500 dark:border-slate-700 dark:text-slate-400">暂时没有可编辑配置，或配置读取失败。</div>
            </div>
          </div>
        </section>
          </div>
        </div>
      </template>
    </main>
  </div>
</template>
plate>
