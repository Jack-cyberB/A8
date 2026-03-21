const { createApp, nextTick } = Vue;

const API_BASE = '';

function buildQuery(params) {
  const q = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== '') q.set(k, v);
  });
  const raw = q.toString();
  return raw ? `?${raw}` : '';
}

createApp({
  data() {
    return {
      pages: [
        { key: 'overview', label: '数据总览' },
        { key: 'analysis', label: '能耗分析' },
        { key: 'anomaly', label: '故障监控' },
        { key: 'assistant', label: '智能助手' },
      ],
      analysisMetrics: [
        { value: 'electricity', label: '电力' },
        { value: 'water', label: '水' },
        { value: 'hvac', label: '空调' },
        { value: 'environment', label: '环境' },
      ],
      activePage: 'overview',
      buildings: [],
      globalRangeText: '-',
      globalRange: { startTime: '', endTime: '' },
      filters: { buildingId: '', range: [] },
      analysisMetric: 'electricity',
      analysisOverlayWeather: true,
      analysisUnsupportedMessage: '',
      anomalyQuery: {
        severity: '',
        status: '',
        sort: 'timestamp_desc',
        page: 1,
        pageSize: 20,
        total: 0,
      },
      trendSeries: [],
      anomalyRows: [],
      anomalyTypeStats: {},
      overview: { totalKwh: 0, avgKwh: 0, anomalyCount: 0, savingPct: 0, carbonKg: 0 },
      analysisSummary: {
        metric_type: 'electricity',
        unit: 'kWh',
        total_value: 0,
        avg_value: 0,
        peak_value: 0,
        volatility_pct: 0,
      },
      analysisTrend: {
        metric_type: 'electricity',
        series: [],
        weather_series: [],
        overlayAvailable: false,
        summary: { window_change_pct: 0, temperature_correlation: 0 },
      },
      analysisDistribution: {
        hourly_profile: [],
        weekday_weekend_split: [],
        day_night_split: [],
        weekday_peak_hours: [],
        night_base_load: { avg_value: 0, ratio_vs_avg_pct: 0 },
      },
      analysisCompare: {
        building: null,
        peer_group: null,
        items: [],
        peer_ranking: [],
        message: '',
      },
      analysisInsights: {
        scope_summary: {
          building_id: 'ALL',
          building_name: '全部建筑',
          building_type: 'portfolio',
          selected_start_time: null,
          selected_end_time: null,
          data_start_time: null,
          data_end_time: null,
          point_count: 0,
          granularity: 'hourly',
          anomaly_count: 0,
          metric_label: '电力',
          unit: 'kWh',
        },
        trend_findings: [],
        weather_findings: [],
        compare_findings: [],
        saving_opportunities: [],
        anomaly_windows: [],
      },
      analysisInsight: null,
      analysisFeedbackLabel: '',
      chatInput: '',
      aiProvider: 'auto',
      aiStats: {
        windowHours: 24,
        totalCalls: 0,
        llmCalls: 0,
        realLlmCalls: 0,
        templateCalls: 0,
        fallbackCalls: 0,
        fallbackRate: 0,
        avgLatency: 0,
      },
      aiEvaluate: {
        windowHours: 24,
        template: { total: 0, successRate: 0, avgLatency: 0, fallbackRate: 0, completeRate: 0 },
        llm: { total: 0, successRate: 0, avgLatency: 0, fallbackRate: 0, completeRate: 0 },
        feedback: { total: 0, usefulRate: 0 },
      },
      health: { status: 'unknown', regression: 'unknown', aiConfigured: false, aiModelReady: false },
      diagnosis: null,
      diagnosisFeedbackLabel: '',
      selectedAnomaly: null,
      anomalyDetailVisible: false,
      anomalyDetail: null,
      anomalyHistory: [],
      postmortemForm: {
        anomalyId: null,
        causeConfirmed: '',
        actionTaken: '',
        resultSummary: '',
        recurrenceRisk: 'medium',
        reviewer: '',
      },
      actionDialogVisible: false,
      actionForm: {
        anomalyId: null,
        action: 'ack',
        assignee: '',
        note: '',
      },
      loading: {
        overview: false,
        trend: false,
        anomaly: false,
        ai: false,
        detail: false,
        action: false,
        analysisSummary: false,
        analysisTrend: false,
        analysisDistribution: false,
        analysisCompare: false,
        analysisInsights: false,
        analysisInsight: false,
      },
      errors: {
        overview: '',
        trend: '',
        anomaly: '',
        analysisSummary: '',
        analysisTrend: '',
        analysisDistribution: '',
        analysisCompare: '',
        analysisInsights: '',
      },
      charts: {
        overview: null,
        trend: null,
        hourlyProfile: null,
        split: null,
        compare: null,
        anomalyType: null,
      },
      refreshTimer: null,
      analysisRequestSeq: 0,
    };
  },
  mounted() {
    this.bootstrap();
    window.addEventListener('resize', this.resizeCharts);
  },
  beforeUnmount() {
    window.removeEventListener('resize', this.resizeCharts);
    if (this.refreshTimer) clearTimeout(this.refreshTimer);
  },
  methods: {
    async fetchJson(url, options) {
      const res = await fetch(url, options);
      if (!res.ok) {
        let message = `${res.status} ${res.statusText}`;
        try {
          const payload = await res.json();
          if (payload?.message) message = payload.message;
        } catch (_) {
          // ignore
        }
        throw new Error(message);
      }
      const json = await res.json();
      if (json.code !== 0) throw new Error(json.message || 'API error');
      return json.data;
    },
    formatNumber(value, digits = 1) {
      const num = Number(value || 0);
      return Number.isFinite(num) ? num.toFixed(digits) : '0.0';
    },
    formatCompactDateTime(value) {
      if (!value) return '-';
      return String(value).replace('T', ' ').slice(0, 16);
    },
    providerLabel(value) {
      const map = {
        template: '模板兜底',
        llm: 'DeepSeek',
        auto: '优先 DeepSeek（失败降级）',
        template_provider: '模板兜底',
        llm_provider: 'DeepSeek',
      };
      return map[value] || value || '-';
    },
    requestedProviderLabel(value) {
      const map = {
        template: '模板兜底',
        llm: 'DeepSeek',
        auto: '优先 DeepSeek（失败降级）',
      };
      return map[value] || '优先 DeepSeek（失败降级）';
    },
    resultSourceText(result) {
      if (!result) return '-';
      return this.providerLabel(result.provider);
    },
    resultTriggerText(result) {
      if (!result) return this.requestedProviderLabel(this.aiProvider);
      return this.requestedProviderLabel(result.requested_provider || this.aiProvider);
    },
    resultFallbackText(result) {
      if (!result?.fallback_used) return '';
      return this.humanizeDegradeMessage(result.degrade_message);
    },
    analysisInsightPendingText() {
      return '请先在能耗分析页确认建筑和时间范围，再点击“AI 分析”进入这里生成完整结论。页面筛选和刷新不会自动消耗额度。';
    },
    aiAvailabilityText() {
      return this.health.aiConfigured
        ? 'DeepSeek 已就绪。建议先在能耗分析页确认当前建筑和时间范围，再在这里生成完整结论。'
        : '当前环境尚未配置 DeepSeek API Key，点击后会自动使用模板兜底，不会影响主流程。';
    },
    humanizeDegradeMessage(message) {
      const raw = String(message || '').trim();
      const lower = raw.toLowerCase();
      if (!raw) return '在线模型暂时不可用，系统已切换到模板兜底。';
      if (lower.includes('not configured')) return '当前环境未配置 DeepSeek API Key，本次使用模板兜底。';
      if (lower.includes('network error')) return 'DeepSeek 网络请求失败，本次使用模板兜底。';
      if (lower.includes('http status 429')) return 'DeepSeek 当前触发限流，本次使用模板兜底。';
      if (lower.includes('http status')) return 'DeepSeek 服务响应异常，本次使用模板兜底。';
      if (lower.includes('parse error')) return 'DeepSeek 返回格式异常，本次使用模板兜底。';
      if (lower.includes('simulated llm failure')) return '当前为模拟失败场景，本次使用模板兜底。';
      return raw;
    },
    severityLabel(value) {
      const map = { high: '高', medium: '中', low: '低' };
      return map[value] || value || '-';
    },
    severityTagType(value) {
      const map = { high: 'danger', medium: 'warning', low: 'info' };
      return map[value] || 'info';
    },
    statusDisplay(value) {
      const map = { new: '新告警', acknowledged: '已确认', ignored: '已忽略', resolved: '已完成' };
      return map[value] || value || '-';
    },
    statusTagType(value) {
      const map = { new: 'danger', acknowledged: 'warning', ignored: 'info', resolved: 'success' };
      return map[value] || 'info';
    },
    currentMetricLabel() {
      return this.analysisMetrics.find((item) => item.value === this.analysisMetric)?.label || '电力';
    },
    selectedBuildingMeta() {
      return this.buildings.find((item) => item.id === this.filters.buildingId) || null;
    },
    currentValidScope() {
      const building = this.selectedBuildingMeta();
      if (building?.startTime && building?.endTime) {
        return {
          startTime: building.startTime,
          endTime: building.endTime,
          label: `${building.name} 可用时间`,
        };
      }
      if (this.globalRange.startTime && this.globalRange.endTime) {
        return {
          startTime: this.globalRange.startTime,
          endTime: this.globalRange.endTime,
          label: '全局数据时间',
        };
      }
      return null;
    },
    currentValidRangeText() {
      const scope = this.currentValidScope();
      return scope ? `${scope.startTime} ~ ${scope.endTime}` : '-';
    },
    currentAnalysisScopeText() {
      const building = this.selectedBuildingMeta();
      const buildingText = building ? `${building.name} (${building.type})` : '全部建筑';
      const rangeText = this.isCompleteRange(this.filters.range) ? `${this.filters.range[0]} ~ ${this.filters.range[1]}` : '全量时间范围';
      return `${this.currentMetricLabel()} | ${buildingText} | ${rangeText}`;
    },
    isCompleteRange(range) {
      return Array.isArray(range) && range.length === 2 && !!range[0] && !!range[1];
    },
    getTimeParams() {
      const complete = this.isCompleteRange(this.filters.range);
      return {
        building_id: this.filters.buildingId,
        start_time: complete ? this.filters.range[0] : undefined,
        end_time: complete ? this.filters.range[1] : undefined,
      };
    },
    getAnalysisParams() {
      return {
        ...this.getTimeParams(),
        metric_type: this.analysisMetric,
      };
    },
    clampRangeToScope(range) {
      const scope = this.currentValidScope();
      if (!scope || !this.isCompleteRange(range)) return { range, changed: false };
      let [start, end] = range;
      let changed = false;
      if (start < scope.startTime) {
        start = scope.startTime;
        changed = true;
      }
      if (end > scope.endTime) {
        end = scope.endTime;
        changed = true;
      }
      if (start > end) {
        start = scope.startTime;
        end = scope.endTime;
        changed = true;
      }
      return { range: [start, end], changed };
    },
    ensureRangeWithinScope(options = {}) {
      const { applyDefaultIfEmpty = false, silent = false } = options;
      const scope = this.currentValidScope();
      if (!scope) return false;
      if (!this.isCompleteRange(this.filters.range)) {
        if (applyDefaultIfEmpty) {
          this.filters.range = [scope.startTime, scope.endTime];
          if (!silent) this.$message.info(`已切换到有效时间范围：${scope.startTime} ~ ${scope.endTime}`);
          return true;
        }
        return false;
      }
      const clamped = this.clampRangeToScope(this.filters.range);
      if (clamped.changed) {
        this.filters.range = clamped.range;
        if (!silent) this.$message.warning('所选时间超出当前数据范围，已自动调整');
        return true;
      }
      return false;
    },
    applyValidRange() {
      const scope = this.currentValidScope();
      if (!scope) return;
      this.filters.range = [scope.startTime, scope.endTime];
      this.anomalyQuery.page = 1;
      this.analysisInsight = null;
      this.analysisFeedbackLabel = '';
      this.refreshCurrentPage();
    },
    isDateDisabled(date) {
      const scope = this.currentValidScope();
      if (!scope) return false;
      const start = new Date(scope.startTime.replace(' ', 'T')).getTime();
      const end = new Date(scope.endTime.replace(' ', 'T')).getTime();
      const value = date.getTime();
      return value < start || value > end;
    },
    clearAnalysisData() {
      this.analysisSummary = {
        metric_type: this.analysisMetric,
        unit: 'kWh',
        total_value: 0,
        avg_value: 0,
        peak_value: 0,
        volatility_pct: 0,
      };
      this.analysisTrend = {
        metric_type: this.analysisMetric,
        series: [],
        weather_series: [],
        overlayAvailable: false,
        summary: { window_change_pct: 0, temperature_correlation: 0 },
      };
      this.analysisDistribution = {
        hourly_profile: [],
        weekday_weekend_split: [],
        day_night_split: [],
        weekday_peak_hours: [],
        night_base_load: { avg_value: 0, ratio_vs_avg_pct: 0 },
      };
      this.analysisCompare = {
        building: null,
        peer_group: null,
        items: [],
        peer_ranking: [],
        message: '',
      };
      this.analysisInsights = {
        scope_summary: {
          building_id: this.filters.buildingId || 'ALL',
          building_name: this.selectedBuildingMeta()?.name || '全部建筑',
          building_type: this.selectedBuildingMeta()?.type || 'portfolio',
          selected_start_time: this.isCompleteRange(this.filters.range) ? this.filters.range[0] : null,
          selected_end_time: this.isCompleteRange(this.filters.range) ? this.filters.range[1] : null,
          data_start_time: null,
          data_end_time: null,
          point_count: 0,
          granularity: 'hourly',
          anomaly_count: 0,
          metric_label: this.currentMetricLabel(),
          unit: 'kWh',
        },
        trend_findings: [],
        weather_findings: [],
        compare_findings: [],
        saving_opportunities: [],
        anomaly_windows: [],
      };
    },
    setAnalysisUnsupported(message) {
      this.analysisUnsupportedMessage = message || '暂未接入该分析类型';
      this.clearAnalysisData();
    },
    nextAnalysisRequestId() {
      this.analysisRequestSeq += 1;
      return this.analysisRequestSeq;
    },
    isActiveAnalysisRequest(requestId) {
      return requestId === this.analysisRequestSeq;
    },
    async bootstrap() {
      await this.loadBuildings();
      await this.loadSystemHealth();
      await this.loadAiStats();
      await this.loadAiEvaluate();
      await this.refreshAll();
    },
    onBuildingChange() {
      this.anomalyQuery.page = 1;
      this.analysisInsight = null;
      this.analysisFeedbackLabel = '';
      this.ensureRangeWithinScope({ applyDefaultIfEmpty: true, silent: true });
      this.refreshCurrentPage();
    },
    onAnalysisMetricChange() {
      this.analysisInsight = null;
      this.analysisFeedbackLabel = '';
      if (this.activePage === 'analysis' || this.activePage === 'assistant') {
        this.refreshCurrentPage();
      }
    },
    onAnalysisOverlayChange() {
      if (this.activePage === 'analysis' && !this.analysisUnsupportedMessage) {
        this.renderTrendChart();
      }
    },
    onDateRangeChange() {
      if (this.refreshTimer) clearTimeout(this.refreshTimer);
      this.refreshTimer = setTimeout(() => {
        this.anomalyQuery.page = 1;
        this.analysisInsight = null;
        this.analysisFeedbackLabel = '';
        this.ensureRangeWithinScope({ applyDefaultIfEmpty: false, silent: false });
        this.refreshCurrentPage();
      }, 200);
    },
    onAnomalyFilterChange() {
      this.anomalyQuery.page = 1;
      this.loadAnomalies();
    },
    onAnomalyPageChange(page) {
      this.anomalyQuery.page = page;
      this.loadAnomalies();
    },
    onAnomalyPageSizeChange(pageSize) {
      this.anomalyQuery.pageSize = pageSize;
      this.anomalyQuery.page = 1;
      this.loadAnomalies();
    },
    async loadBuildings() {
      try {
        const data = await this.fetchJson(`${API_BASE}/api/buildings`);
        this.buildings = (data.items || []).map((x) => ({
          id: x.building_id,
          name: x.building_name,
          type: x.building_type,
          startTime: x.start_time,
          endTime: x.end_time,
          recordCount: x.record_count,
        }));
        const range = data.global_range || {};
        this.globalRange = {
          startTime: range.start_time || '',
          endTime: range.end_time || '',
        };
        this.globalRangeText = range.start_time && range.end_time ? `${range.start_time} ~ ${range.end_time}` : '-';
      } catch (err) {
        console.error(err);
        this.globalRangeText = '加载失败';
      }
    },
    async loadOverviewMetrics() {
      this.loading.overview = true;
      this.errors.overview = '';
      try {
        const data = await this.fetchJson(`${API_BASE}/api/metrics/overview${buildQuery(this.getTimeParams())}`);
        this.overview.totalKwh = data.total_kwh || 0;
        this.overview.avgKwh = data.avg_kwh || 0;
        this.overview.anomalyCount = data.anomaly_count || 0;
        this.overview.savingPct = data.saving_potential_pct || 0;
        this.overview.carbonKg = data.carbon_reduction_kg || 0;
      } catch (err) {
        console.error(err);
        this.errors.overview = '总览指标加载失败';
      } finally {
        this.loading.overview = false;
      }
    },
    async loadTrend() {
      this.loading.trend = true;
      this.errors.trend = '';
      try {
        const data = await this.fetchJson(`${API_BASE}/api/energy/trend${buildQuery(this.getTimeParams())}`);
        this.trendSeries = data.series || [];
      } catch (err) {
        console.error(err);
        this.errors.trend = '趋势数据加载失败';
      } finally {
        this.loading.trend = false;
      }
    },
    buildFallbackInsights() {
      const meta = this.selectedBuildingMeta();
      const peer = this.analysisCompare.peer_group || {};
      const summary = this.analysisSummary || {};
      const trendSummary = this.analysisTrend.summary || {};
      const distribution = this.analysisDistribution || {};
      const selectedStart = this.isCompleteRange(this.filters.range) ? this.filters.range[0] : (meta?.startTime || this.globalRange.startTime || null);
      const selectedEnd = this.isCompleteRange(this.filters.range) ? this.filters.range[1] : (meta?.endTime || this.globalRange.endTime || null);
      const weekdayPeakHours = distribution.weekday_peak_hours || [];
      const opportunities = [];
      if (weekdayPeakHours.length) {
        opportunities.push({
          title: '工作日峰段优化',
          detail: `高负荷集中在 ${weekdayPeakHours[0].label} 左右，可优先检查错峰、排程与设定点。`,
          priority: 'medium',
          estimated_loss_kwh: Number(summary.peak_value || 0),
        });
      }
      if ((distribution.night_base_load?.ratio_vs_avg_pct || 0) > 45) {
        opportunities.push({
          title: '夜间基线偏高',
          detail: `夜间基线约为平均负荷的 ${this.formatNumber(distribution.night_base_load.ratio_vs_avg_pct, 1)}%，建议核查非工作时段设备待机与常开策略。`,
          priority: 'high',
          estimated_loss_kwh: Number(distribution.night_base_load?.avg_value || 0),
        });
      }
      return {
        scope_summary: {
          building_id: this.filters.buildingId || 'ALL',
          building_name: meta?.name || '全部建筑',
          building_type: meta?.type || 'portfolio',
          selected_start_time: selectedStart,
          selected_end_time: selectedEnd,
          data_start_time: selectedStart,
          data_end_time: selectedEnd,
          point_count: Array.isArray(this.analysisTrend.series) ? this.analysisTrend.series.length : 0,
          granularity: 'filtered',
          anomaly_count: Array.isArray(this.analysisTrend.markers) ? this.analysisTrend.markers.length : 0,
          metric_label: this.currentMetricLabel(),
          unit: summary.unit || 'kWh',
        },
        trend_findings: [
          {
            title: '负荷波动概况',
            detail: `当前时间范围均值 ${this.formatNumber(summary.avg_value, 1)} ${summary.unit || 'kWh'}，峰值 ${this.formatNumber(summary.peak_value, 1)} ${summary.unit || 'kWh'}，窗口变化 ${this.formatNumber(trendSummary.window_change_pct, 1)}%。`,
            severity: Number(summary.volatility_pct || 0) > 30 ? 'warning' : 'info',
          },
        ],
        weather_findings: [
          {
            title: '温度相关性',
            detail: `当前温度相关系数约 ${this.formatNumber(trendSummary.temperature_correlation, 2)}，可结合季节变化判断温控负荷影响。`,
            severity: Math.abs(Number(trendSummary.temperature_correlation || 0)) > 0.45 ? 'info' : 'success',
          },
        ],
        compare_findings: peer.peer_count ? [
          {
            title: '同类位置',
            detail: `当前建筑与同类均值差异 ${this.formatNumber(peer.gap_pct, 1)}%，同类百分位 ${this.formatNumber(peer.peer_percentile, 1)}。`,
            severity: Number(peer.gap_pct || 0) > 10 ? 'warning' : 'info',
          },
        ] : [],
        saving_opportunities: opportunities,
        anomaly_windows: [],
      };
    },
    async loadAnalysisSummary(requestId = this.analysisRequestSeq) {
      this.loading.analysisSummary = true;
      if (this.isActiveAnalysisRequest(requestId)) this.errors.analysisSummary = '';
      try {
        const data = await this.fetchJson(`${API_BASE}/api/analysis/summary${buildQuery(this.getAnalysisParams())}`);
        if (!this.isActiveAnalysisRequest(requestId)) return;
        this.analysisUnsupportedMessage = '';
        this.analysisSummary = data;
      } catch (err) {
        console.error(err);
        if (!this.isActiveAnalysisRequest(requestId)) return;
        if (String(err.message || '').includes('暂未接入')) {
          this.setAnalysisUnsupported(err.message);
          return;
        }
        this.errors.analysisSummary = '分析摘要加载失败';
      } finally {
        if (this.isActiveAnalysisRequest(requestId)) this.loading.analysisSummary = false;
      }
    },
    async loadAnalysisTrend(requestId = this.analysisRequestSeq) {
      this.loading.analysisTrend = true;
      if (this.isActiveAnalysisRequest(requestId)) this.errors.analysisTrend = '';
      try {
        const data = await this.fetchJson(`${API_BASE}/api/analysis/trend${buildQuery(this.getAnalysisParams())}`);
        if (!this.isActiveAnalysisRequest(requestId)) return;
        this.analysisTrend = {
          ...data,
          overlayAvailable: !!data.overlay_available,
          summary: data.summary || { window_change_pct: 0, temperature_correlation: 0 },
        };
      } catch (err) {
        console.error(err);
        if (!this.isActiveAnalysisRequest(requestId)) return;
        if (String(err.message || '').includes('暂未接入')) {
          this.setAnalysisUnsupported(err.message);
          return;
        }
        this.errors.analysisTrend = '分析趋势加载失败';
      } finally {
        if (this.isActiveAnalysisRequest(requestId)) this.loading.analysisTrend = false;
      }
    },
    async loadAnalysisDistribution(requestId = this.analysisRequestSeq) {
      this.loading.analysisDistribution = true;
      if (this.isActiveAnalysisRequest(requestId)) this.errors.analysisDistribution = '';
      try {
        const data = await this.fetchJson(`${API_BASE}/api/analysis/distribution${buildQuery(this.getAnalysisParams())}`);
        if (!this.isActiveAnalysisRequest(requestId)) return;
        this.analysisDistribution = data;
      } catch (err) {
        console.error(err);
        if (!this.isActiveAnalysisRequest(requestId)) return;
        if (String(err.message || '').includes('暂未接入')) {
          this.setAnalysisUnsupported(err.message);
          return;
        }
        this.errors.analysisDistribution = '结构分析加载失败';
      } finally {
        if (this.isActiveAnalysisRequest(requestId)) this.loading.analysisDistribution = false;
      }
    },
    async loadAnalysisCompare(requestId = this.analysisRequestSeq) {
      this.loading.analysisCompare = true;
      if (this.isActiveAnalysisRequest(requestId)) this.errors.analysisCompare = '';
      try {
        const data = await this.fetchJson(`${API_BASE}/api/analysis/compare${buildQuery(this.getAnalysisParams())}`);
        if (!this.isActiveAnalysisRequest(requestId)) return;
        this.analysisCompare = data;
      } catch (err) {
        console.error(err);
        if (!this.isActiveAnalysisRequest(requestId)) return;
        if (String(err.message || '').includes('暂未接入')) {
          this.setAnalysisUnsupported(err.message);
          return;
        }
        this.errors.analysisCompare = '对比分析加载失败';
      } finally {
        if (this.isActiveAnalysisRequest(requestId)) this.loading.analysisCompare = false;
      }
    },
    async loadAnalysisInsights(requestId = this.analysisRequestSeq) {
      this.loading.analysisInsights = true;
      if (this.isActiveAnalysisRequest(requestId)) this.errors.analysisInsights = '';
      try {
        const data = await this.fetchJson(`${API_BASE}/api/analysis/insights${buildQuery(this.getAnalysisParams())}`);
        if (!this.isActiveAnalysisRequest(requestId)) return;
        this.analysisInsights = data;
      } catch (err) {
        console.error(err);
        if (!this.isActiveAnalysisRequest(requestId)) return;
        if (String(err.message || '').includes('暂未接入')) {
          this.setAnalysisUnsupported(err.message);
          return;
        }
        this.analysisInsights = this.buildFallbackInsights();
        this.errors.analysisInsights = '规则摘要加载失败，已切换到本地规则摘要';
      } finally {
        if (this.isActiveAnalysisRequest(requestId)) this.loading.analysisInsights = false;
      }
    },
    async loadAnalysisWorkspace() {
      const requestId = this.nextAnalysisRequestId();
      this.analysisUnsupportedMessage = '';
      this.clearAnalysisData();
      await this.loadAnalysisSummary(requestId);
      if (!this.isActiveAnalysisRequest(requestId) || this.analysisUnsupportedMessage) {
        await nextTick();
        return;
      }
      await Promise.all([
        this.loadAnalysisTrend(requestId),
        this.loadAnalysisDistribution(requestId),
        this.loadAnalysisCompare(requestId),
        this.loadAnalysisInsights(requestId),
      ]);
      if (!this.isActiveAnalysisRequest(requestId)) return;
      await this.syncVisibleCharts();
    },
    async loadAnomalies() {
      this.loading.anomaly = true;
      this.errors.anomaly = '';
      try {
        const data = await this.fetchJson(`${API_BASE}/api/anomaly/list${buildQuery({ ...this.getTimeParams(), severity: this.anomalyQuery.severity, status: this.anomalyQuery.status, sort: this.anomalyQuery.sort, page: this.anomalyQuery.page, page_size: this.anomalyQuery.pageSize })}`);
        this.anomalyRows = data.items || [];
        this.anomalyTypeStats = data.by_type || {};
        this.anomalyQuery.total = data.total_count || 0;
        this.anomalyQuery.page = data.page || this.anomalyQuery.page;
        this.anomalyQuery.pageSize = data.page_size || this.anomalyQuery.pageSize;
        await nextTick();
        this.renderAnomalyTypeChart(this.anomalyTypeStats);
      } catch (err) {
        console.error(err);
        this.errors.anomaly = '异常数据加载失败';
        this.anomalyRows = [];
        this.anomalyTypeStats = {};
      } finally {
        this.loading.anomaly = false;
      }
    },
    async loadAiStats() {
      try {
        const data = await this.fetchJson(`${API_BASE}/api/ai/stats?hours=24`);
        this.aiStats.windowHours = data.window_hours ?? 24;
        this.aiStats.totalCalls = data.total_calls ?? 0;
        this.aiStats.llmCalls = data.llm_calls ?? 0;
        this.aiStats.fallbackCalls = data.fallback_calls ?? 0;
        this.aiStats.fallbackRate = data.fallback_rate_pct ?? 0;
        this.aiStats.avgLatency = data.avg_latency_ms ?? 0;
        this.aiStats.realLlmCalls = data.by_provider?.llm_provider ?? 0;
        this.aiStats.templateCalls = data.by_provider?.template_provider ?? 0;
      } catch (err) {
        console.error(err);
      }
    },
    async loadAiEvaluate() {
      try {
        const data = await this.fetchJson(`${API_BASE}/api/ai/evaluate`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ hours: 24 }),
        });
        this.aiEvaluate.windowHours = data.window_hours ?? 24;
        this.aiEvaluate.template = {
          total: data.template?.total ?? 0,
          successRate: data.template?.success_rate_pct ?? 0,
          avgLatency: data.template?.avg_latency_ms ?? 0,
          fallbackRate: data.template?.fallback_rate_pct ?? 0,
          completeRate: data.template?.field_completeness_pct ?? 0,
        };
        this.aiEvaluate.llm = {
          total: data.llm?.total ?? 0,
          successRate: data.llm?.success_rate_pct ?? 0,
          avgLatency: data.llm?.avg_latency_ms ?? 0,
          fallbackRate: data.llm?.fallback_rate_pct ?? 0,
          completeRate: data.llm?.field_completeness_pct ?? 0,
        };
        this.aiEvaluate.feedback = {
          total: data.feedback?.total_labeled ?? 0,
          usefulRate: data.feedback?.useful_rate_pct ?? 0,
        };
      } catch (err) {
        console.error(err);
      }
    },
    async loadSystemHealth() {
      try {
        const data = await this.fetchJson(`${API_BASE}/api/system/health`);
        this.health.status = data.status || 'unknown';
        this.health.regression = data.recent_regression?.status || 'unknown';
        this.health.aiConfigured = !!data.ai_provider?.configured;
        this.health.aiModelReady = !!data.ai_provider?.model;
      } catch (err) {
        console.error(err);
        this.health.status = 'unknown';
        this.health.regression = 'unknown';
        this.health.aiConfigured = false;
        this.health.aiModelReady = false;
      }
    },
    async exportAnomalies() {
      const params = buildQuery({
        ...this.getTimeParams(),
        severity: this.anomalyQuery.severity,
        status: this.anomalyQuery.status,
        sort: this.anomalyQuery.sort,
      });
      window.open(`${API_BASE}/api/anomaly/export${params}`, '_blank');
    },
    async refreshAll() {
      await Promise.all([this.loadOverviewMetrics(), this.loadTrend(), this.loadAnomalies(), this.loadAnalysisWorkspace()]);
      await this.syncVisibleCharts();
    },
    async refreshCurrentPage() {
      if (this.activePage === 'overview') {
        await Promise.all([this.loadOverviewMetrics(), this.loadTrend(), this.loadAnomalies()]);
        await this.syncVisibleCharts();
        return;
      }
      if (this.activePage === 'analysis') {
        await this.loadAnalysisWorkspace();
        return;
      }
      if (this.activePage === 'anomaly') {
        await Promise.all([this.loadOverviewMetrics(), this.loadAnomalies()]);
        await this.syncVisibleCharts();
        return;
      }
      await Promise.all([this.loadAiStats(), this.loadAiEvaluate()]);
    },
    async switchPage(key) {
      this.activePage = key;
      await this.refreshCurrentPage();
    },
    statusLabel(v) {
      const map = { new: 'new', acknowledged: 'acknowledged', ignored: 'ignored', resolved: 'resolved' };
      return map[v] || v;
    },
    openActionDialog(row, action) {
      this.actionForm.anomalyId = row.anomaly_id;
      this.actionForm.action = action;
      this.actionForm.assignee = row.assignee || '';
      this.actionForm.note = '';
      this.actionDialogVisible = true;
    },
    async submitAnomalyAction() {
      if (!this.actionForm.anomalyId) return;
      this.loading.action = true;
      try {
        await this.fetchJson(`${API_BASE}/api/anomaly/action`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            anomaly_id: this.actionForm.anomalyId,
            action: this.actionForm.action,
            assignee: this.actionForm.assignee,
            note: this.actionForm.note,
          }),
        });
        this.actionDialogVisible = false;
        this.$message.success('处理动作已保存');
        await this.loadAnomalies();
        if (this.anomalyDetailVisible && this.anomalyDetail?.anomaly?.anomaly_id === this.actionForm.anomalyId) {
          await this.openAnomalyDetail({ anomaly_id: this.actionForm.anomalyId });
        }
      } catch (err) {
        console.error(err);
        this.$message.error(`提交失败：${err.message}`);
      } finally {
        this.loading.action = false;
      }
    },
    async openAnomalyDetail(row) {
      this.loading.detail = true;
      try {
        const detail = await this.fetchJson(`${API_BASE}/api/anomaly/detail?anomaly_id=${encodeURIComponent(row.anomaly_id)}`);
        const history = await this.fetchJson(`${API_BASE}/api/anomaly/history?anomaly_id=${encodeURIComponent(row.anomaly_id)}`);
        this.anomalyDetail = detail;
        this.anomalyHistory = history.items || [];
        const note = detail.postmortem_note || {};
        this.postmortemForm = {
          anomalyId: row.anomaly_id,
          causeConfirmed: note.cause_confirmed || '',
          actionTaken: note.action_taken || '',
          resultSummary: note.result_summary || '',
          recurrenceRisk: note.recurrence_risk || 'medium',
          reviewer: note.reviewer || '',
        };
        this.anomalyDetailVisible = true;
      } catch (err) {
        console.error(err);
        this.$message.error('异常详情加载失败');
      } finally {
        this.loading.detail = false;
      }
    },
    fillPostmortemFromDiagnosis() {
      if (!this.diagnosis) {
        this.$message.warning('请先生成诊断');
        return;
      }
      this.postmortemForm.causeConfirmed = (this.diagnosis.causes || []).join('；');
      this.postmortemForm.actionTaken = (this.diagnosis.recommended_actions || []).join('；');
      this.postmortemForm.resultSummary = this.diagnosis.conclusion || '';
      this.postmortemForm.recurrenceRisk = this.diagnosis.risk_level || 'medium';
    },
    async submitPostmortemNote() {
      if (!this.postmortemForm.anomalyId) return;
      try {
        const payload = {
          anomaly_id: this.postmortemForm.anomalyId,
          cause_confirmed: this.postmortemForm.causeConfirmed,
          action_taken: this.postmortemForm.actionTaken,
          result_summary: this.postmortemForm.resultSummary,
          recurrence_risk: this.postmortemForm.recurrenceRisk,
          reviewer: this.postmortemForm.reviewer,
        };
        const saved = await this.fetchJson(`${API_BASE}/api/anomaly/note`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        if (this.anomalyDetail) {
          this.anomalyDetail.postmortem_note = saved;
        }
        this.$message.success('复盘记录已保存');
      } catch (err) {
        console.error(err);
        this.$message.error(`复盘保存失败：${err.message}`);
      }
    },
    applyDiagnosisTemplate(kind) {
      const building = this.filters.buildingId || '当前建筑';
      const templates = {
        spike: `${building} 最近出现电量突增，请按原因-步骤-预防输出诊断。`,
        offline: `${building} 工作时段出现低负荷，请给出排查步骤和恢复建议。`,
        highload: `${building} 持续高负荷运行，请评估风险并给出优先动作。`,
      };
      this.chatInput = templates[kind] || templates.spike;
    },
    diagnoseFromAnomaly(row) {
      this.selectedAnomaly = row;
      this.activePage = 'assistant';
      this.chatInput = `${row.building_name} 在 ${row.timestamp} 出现 ${row.anomaly_name}，请给出诊断和处理建议。`;
      this.submitDiagnosis();
    },
    copyDiagnosisToActionNote() {
      if (!this.diagnosis || !this.selectedAnomaly) return;
      this.actionForm.anomalyId = this.selectedAnomaly.anomaly_id;
      this.actionForm.action = 'ack';
      this.actionForm.assignee = this.selectedAnomaly.assignee || '';
      const lines = [];
      lines.push(`诊断结论：${this.diagnosis.conclusion || ''}`);
      if (Array.isArray(this.diagnosis.recommended_actions) && this.diagnosis.recommended_actions.length) {
        lines.push(`建议动作：${this.diagnosis.recommended_actions.join('；')}`);
      }
      lines.push(`Provider=${this.diagnosis.provider}，Fallback=${this.diagnosis.fallback_used}`);
      this.actionForm.note = lines.join('\n');
      this.actionDialogVisible = true;
      this.activePage = 'anomaly';
    },
    async submitDiagnosis() {
      if (!this.chatInput.trim() && !this.selectedAnomaly) {
        this.$message.warning('请先输入问题或从异常列表触发诊断');
        return;
      }
      this.loading.ai = true;
      try {
        const payload = {
          message: this.chatInput,
          provider: this.aiProvider,
          building_id: this.selectedAnomaly?.building_id || this.filters.buildingId || null,
          anomaly_id: this.selectedAnomaly?.anomaly_id || null,
          timestamp: this.selectedAnomaly?.timestamp || null,
          anomaly_type: this.selectedAnomaly?.anomaly_type || null,
        };
        const data = await this.fetchJson(`${API_BASE}/api/ai/diagnose`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        const diag = data.diagnosis || {};
        diag.causes = diag.causes || diag.possible_causes || [];
        diag.steps = diag.steps || [];
        diag.prevention = diag.prevention || [];
        diag.recommended_actions = diag.recommended_actions || [];
        diag.evidence = diag.evidence || [];
        this.diagnosis = diag;
        this.diagnosisFeedbackLabel = '';
        await this.loadAiStats();
        await this.loadAiEvaluate();
      } catch (err) {
        console.error(err);
        this.$message.error('诊断失败，请稍后重试');
      } finally {
        this.loading.ai = false;
      }
    },
    async submitDiagnosisFeedback(label) {
      if (!this.diagnosis?.trace_id) {
        this.$message.warning('暂无可标注的诊断');
        return;
      }
      try {
        await this.fetchJson(`${API_BASE}/api/ai/feedback`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ trace_id: this.diagnosis.trace_id, label }),
        });
        this.diagnosisFeedbackLabel = label;
        await this.loadAiEvaluate();
        this.$message.success('质量标记已保存');
      } catch (err) {
        console.error(err);
        this.$message.error(`标记失败：${err.message}`);
      }
    },
    async submitAnalysisReport() {
      this.loading.analysisInsight = true;
      try {
        const payload = {
          provider: this.aiProvider,
          metric_type: this.analysisMetric,
          building_id: this.filters.buildingId || null,
          start_time: this.isCompleteRange(this.filters.range) ? this.filters.range[0] : null,
          end_time: this.isCompleteRange(this.filters.range) ? this.filters.range[1] : null,
          insights: this.analysisInsights,
          summary_snapshot: this.analysisSummary,
          trend_snapshot: this.analysisTrend.summary,
          distribution_snapshot: {
            weekday_peak_hours: this.analysisDistribution.weekday_peak_hours || [],
            night_base_load: this.analysisDistribution.night_base_load || {},
          },
          compare_snapshot: this.analysisCompare.peer_group || null,
          message: `${this.currentAnalysisScopeText()}，请输出结构化分析结论。`,
        };
        const data = await this.fetchJson(`${API_BASE}/api/ai/analyze`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        this.analysisInsight = data.analysis || null;
        this.analysisFeedbackLabel = '';
        await this.loadAiStats();
        await this.loadAiEvaluate();
        this.$message.success(this.analysisInsight?.fallback_used ? 'DeepSeek 不可用，已切换为模板兜底' : '分析结论已生成');
      } catch (err) {
        console.error(err);
        this.$message.error(`分析结论生成失败：${err.message}`);
      } finally {
        this.loading.analysisInsight = false;
      }
    },
    async runAnalysisAssistant() {
      this.activePage = 'assistant';
      await nextTick();
      await this.submitAnalysisReport();
    },
    async submitAnalysisFeedback(label) {
      if (!this.analysisInsight?.trace_id) {
        this.$message.warning('暂无可标注的分析结论');
        return;
      }
      try {
        await this.fetchJson(`${API_BASE}/api/ai/feedback`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ trace_id: this.analysisInsight.trace_id, label }),
        });
        this.analysisFeedbackLabel = label;
        await this.loadAiEvaluate();
        this.$message.success('质量标记已保存');
      } catch (err) {
        console.error(err);
        this.$message.error(`标记失败：${err.message}`);
      }
    },
    getChartNode(domId) {
      return document.getElementById(domId);
    },
    canRenderChart(domId) {
      const node = this.getChartNode(domId);
      return !!(node && node.clientWidth > 0 && node.clientHeight > 0);
    },
    ensureChart(key, domId) {
      const node = document.getElementById(domId);
      if (!node || typeof echarts === 'undefined' || !this.canRenderChart(domId)) return null;
      if (this.charts[key]) {
        const existingDom = this.charts[key].getDom?.();
        if (existingDom === node) return this.charts[key];
        this.charts[key].dispose?.();
        this.charts[key] = null;
      }
      this.charts[key] = echarts.init(node);
      return this.charts[key];
    },
    chartAxisStyle(name, extra = {}) {
      return {
        axisLine: { lineStyle: { color: 'rgba(84, 103, 135, 0.45)' } },
        axisTick: { show: false },
        axisLabel: { color: '#5a6982', fontSize: 11, margin: 12 },
        splitLine: { lineStyle: { color: 'rgba(115, 137, 176, 0.12)' } },
        name,
        nameTextStyle: { color: '#5a6982', padding: [0, 0, 8, 0] },
        ...extra,
      };
    },
    buildTimeAxisLabels(items) {
      const count = items.length;
      const interval = count > 720 ? 143 : count > 360 ? 71 : count > 180 ? 35 : count > 90 ? 17 : count > 45 ? 8 : 0;
      return {
        interval,
        hideOverlap: true,
        showMinLabel: true,
        showMaxLabel: true,
        formatter: (value) => {
          const text = String(value || '');
          if (!text) return '';
          if (count > 180) return text.slice(5, 10);
          if (count > 45) return text.slice(5, 16).replace('T', ' ');
          return text.slice(5, 16).replace('T', '\n');
        },
      };
    },
    insightToneClass(level) {
      const map = {
        danger: 'is-danger',
        warning: 'is-warning',
        success: 'is-success',
        info: 'is-info',
        high: 'is-danger',
        medium: 'is-warning',
        low: 'is-success',
      };
      return map[level] || 'is-info';
    },
    priorityTagType(level) {
      const map = { high: 'danger', medium: 'warning', low: 'success' };
      return map[level] || 'info';
    },
    formatInsightKwh(value) {
      const num = Number(value || 0);
      return Number.isFinite(num) ? `${num.toFixed(1)} kWh` : '0.0 kWh';
    },
    bucketAnalysisSeries(series, bucketHours) {
      if (!Array.isArray(series) || !series.length || !bucketHours || bucketHours <= 1) {
        return series || [];
      }
      const buckets = [];
      for (let index = 0; index < series.length; index += bucketHours) {
        const chunk = series.slice(index, index + bucketHours);
        const values = chunk.map((item) => Number(item.value || 0)).filter((item) => Number.isFinite(item));
        if (!values.length) continue;
        const first = chunk[0];
        const last = chunk[chunk.length - 1];
        buckets.push({
          timestamp: first.timestamp,
          bucket_start: first.timestamp,
          bucket_end: last.timestamp,
          value: values.reduce((sum, item) => sum + item, 0) / values.length,
        });
      }
      return buckets;
    },
    buildTrendChartSeries() {
      const rawSeries = this.analysisTrend.series || [];
      const rawComparison = this.analysisTrend.comparison_series || [];
      const rawWeather = this.analysisTrend.weather_series || [];
      let bucketHours = 1;
      if (rawSeries.length > 1200) bucketHours = 24;
      else if (rawSeries.length > 480) bucketHours = 12;
      else if (rawSeries.length > 240) bucketHours = 6;
      const series = this.bucketAnalysisSeries(rawSeries, bucketHours);
      const comparisonSeries = this.bucketAnalysisSeries(rawComparison, bucketHours);
      const weatherSeries = this.bucketAnalysisSeries(rawWeather, bucketHours);
      return {
        series,
        comparisonSeries,
        weatherSeries,
        bucketHours,
      };
    },
    findTrendBucketForTimestamp(series, timestamp) {
      if (!Array.isArray(series) || !series.length || !timestamp) return null;
      const target = new Date(String(timestamp).replace(' ', 'T')).getTime();
      let matched = null;
      let minDiff = Number.POSITIVE_INFINITY;
      series.forEach((item) => {
        const bucketStart = new Date(String(item.bucket_start || item.timestamp).replace(' ', 'T')).getTime();
        const bucketEnd = new Date(String(item.bucket_end || item.timestamp).replace(' ', 'T')).getTime();
        if (target >= bucketStart && target <= bucketEnd) {
          matched = item;
          minDiff = 0;
          return;
        }
        const diff = Math.abs(bucketStart - target);
        if (diff < minDiff) {
          matched = item;
          minDiff = diff;
        }
      });
      return matched;
    },
    buildTrendMarkerPoints(series) {
      return (this.analysisTrend.markers || [])
        .map((item) => {
          const bucket = this.findTrendBucketForTimestamp(series, item.timestamp);
          if (!bucket) return null;
          return {
            name: item.anomaly_name,
            value: item.value,
            coord: [bucket.timestamp, bucket.value],
            itemStyle: {
              color: item.severity === 'high' ? '#d94f4f' : item.severity === 'medium' ? '#f0a024' : '#3cb179',
            },
            label: {
              show: false,
            },
            anomaly: item,
          };
        })
        .filter(Boolean);
    },
    renderOverviewChart() {
      const chart = this.ensureChart('overview', 'overviewChart');
      if (!chart) return;
      const data = this.trendSeries.slice(-72);
      chart.setOption({
        grid: { left: 48, right: 24, top: 26, bottom: 40 },
        tooltip: { trigger: 'axis' },
        xAxis: { type: 'category', data: data.map((i) => i.timestamp.slice(5, 16)), ...this.chartAxisStyle('') },
        yAxis: { type: 'value', ...this.chartAxisStyle('kWh') },
        series: [{ type: 'line', data: data.map((i) => i.value), smooth: true, showSymbol: false, areaStyle: { color: 'rgba(35, 124, 255, 0.18)' }, lineStyle: { width: 3, color: '#1f78ff' } }],
      });
    },
    renderTrendChart() {
      const chart = this.ensureChart('trend', 'trendChart');
      if (!chart) return;
      const { series, comparisonSeries, weatherSeries, bucketHours } = this.buildTrendChartSeries();
      if (!series.length) {
        chart.clear();
        return;
      }
      const timeAxisLabel = this.buildTimeAxisLabels(series.map((item) => item.timestamp));
      const tooltipLabel = bucketHours > 1 ? `平均负荷（${bucketHours}h）` : `${this.currentMetricLabel()}负荷`;
      const markerPoints = this.buildTrendMarkerPoints(series);
      const option = {
        grid: { left: 60, right: 62, top: 52, bottom: 46 },
        tooltip: {
          trigger: 'axis',
          backgroundColor: 'rgba(15, 25, 43, 0.92)',
          borderWidth: 0,
          textStyle: { color: '#f5f8ff' },
          formatter: (params) => {
            const head = params?.[0]?.axisValueLabel || '-';
            const lines = [head];
            params.forEach((item) => {
              const unit = item.seriesName === '气温' ? '°C' : this.analysisSummary.unit || 'kWh';
              lines.push(`${item.marker}${item.seriesName}：${this.formatNumber(item.value, 1)} ${unit}`);
            });
            return lines.join('<br/>');
          },
        },
        legend: { top: 0, itemWidth: 14, itemHeight: 8, textStyle: { color: '#52617b', fontSize: 12 } },
        xAxis: {
          type: 'category',
          data: series.map((i) => i.timestamp),
          ...this.chartAxisStyle(''),
          boundaryGap: false,
          axisLabel: { ...this.chartAxisStyle('').axisLabel, ...timeAxisLabel },
        },
        yAxis: [
          { type: 'value', ...this.chartAxisStyle(this.analysisSummary.unit || 'kWh'), splitNumber: 4 },
          { type: 'value', ...this.chartAxisStyle('°C'), splitLine: { show: false }, splitNumber: 4 },
        ],
        series: [
          {
            name: tooltipLabel,
            type: 'line',
            data: series.map((i) => i.value),
            showSymbol: false,
            sampling: 'lttb',
            smooth: 0.22,
            lineStyle: { width: 2.5, color: '#1462d9' },
            areaStyle: { color: 'rgba(20, 98, 217, 0.09)' },
            markPoint: markerPoints.length ? {
              symbol: 'circle',
              symbolSize: 11,
              itemStyle: { color: '#d94f4f' },
              data: markerPoints,
              tooltip: {
                formatter: (params) => {
                  const item = params?.data?.anomaly;
                  if (!item) return params?.name || '';
                  return [
                    `${item.timestamp}`,
                    `${item.anomaly_name} / ${item.severity}`,
                    `偏差：${this.formatNumber(item.deviation_pct, 1)}%`,
                    `影响估算：${this.formatNumber(item.estimated_loss_kwh, 1)} kWh`,
                  ].join('<br/>');
                },
              },
            } : undefined,
          },
        ],
      };
      if (comparisonSeries?.length) {
        option.series.push({
          name: '同小时基线',
          type: 'line',
          data: comparisonSeries.map((i) => i.value),
          showSymbol: false,
          sampling: 'lttb',
          smooth: 0.14,
          lineStyle: { width: 1.6, type: 'dashed', color: '#7e90ac' },
        });
      }
      if (this.analysisOverlayWeather && this.analysisTrend.overlayAvailable && weatherSeries?.length) {
        option.series.push({
          name: '气温',
          type: 'line',
          yAxisIndex: 1,
          data: weatherSeries.map((i) => i.value),
          showSymbol: false,
          sampling: 'lttb',
          smooth: 0.18,
          lineStyle: { width: 2, type: 'solid', color: '#f08a24' },
        });
      }
      chart.setOption(option, true);
    },
    renderHourlyProfileChart() {
      const chart = this.ensureChart('hourlyProfile', 'patternChart');
      if (!chart) return;
      const items = this.analysisDistribution.hourly_profile || [];
      chart.setOption({
        grid: { left: 48, right: 16, top: 28, bottom: 42 },
        tooltip: {
          trigger: 'axis',
          axisPointer: { type: 'shadow' },
          formatter: (params) => {
            const item = params?.[0];
            return `${item?.axisValue || ''}<br/>平均负荷：${this.formatNumber(item?.value, 1)} ${this.analysisSummary.unit || 'kWh'}`;
          },
        },
        xAxis: {
          type: 'category',
          data: items.map((item) => item.label),
          axisLabel: { color: '#61718c', interval: 5, fontSize: 11 },
          axisTick: { show: false },
        },
        yAxis: { type: 'value', ...this.chartAxisStyle(this.analysisSummary.unit || 'kWh'), splitNumber: 4 },
        series: [{
          type: 'bar',
          barMaxWidth: 12,
          data: items.map((item) => item.avg_value),
          itemStyle: { color: '#2d82f7', borderRadius: [4, 4, 0, 0] },
        }],
      }, true);
    },
    renderSplitChart() {
      const chart = this.ensureChart('split', 'splitChart');
      if (!chart) return;
      const weekdayWeekend = this.analysisDistribution.weekday_weekend_split || [];
      const dayNight = this.analysisDistribution.day_night_split || [];
      const rows = [
        ...weekdayWeekend.map((item) => ({ name: item.label, value: item.ratio_pct, color: String(item.label || '').includes('周末') ? '#89c86a' : '#4a78d5' })),
        ...dayNight.map((item) => ({ name: item.label, value: item.ratio_pct, color: String(item.label || '').includes('夜') ? '#8f6de8' : '#f2a43c' })),
      ];
      chart.setOption({
        grid: { left: 74, right: 30, top: 16, bottom: 18 },
        tooltip: {
          trigger: 'axis',
          axisPointer: { type: 'shadow' },
          formatter: (params) => {
            const item = params?.[0];
            return `${item?.name || ''}<br/>占比：${this.formatNumber(item?.value, 1)}%`;
          },
        },
        xAxis: {
          type: 'value',
          max: 100,
          ...this.chartAxisStyle('%', { splitNumber: 4 }),
        },
        yAxis: {
          type: 'category',
          data: rows.map((item) => item.name),
          axisLine: { show: false },
          axisTick: { show: false },
          axisLabel: { color: '#55657e', fontSize: 12 },
        },
        series: [{
          type: 'bar',
          barWidth: 16,
          data: rows.map((item) => ({
            value: item.value,
            itemStyle: { color: item.color, borderRadius: 8 },
          })),
          label: {
            show: true,
            position: 'right',
            color: '#42526d',
            fontSize: 11,
            formatter: ({ value }) => `${this.formatNumber(value, 1)}%`,
          },
        }],
      }, true);
    },
    renderCompareChart() {
      const chart = this.ensureChart('compare', 'compareChart');
      if (!chart) return;
      const items = this.analysisCompare.items || [];
      if (!items.length) {
        chart.clear();
        return;
      }
      chart.setOption({
        grid: { left: 56, right: 24, top: 26, bottom: 20 },
        tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
        xAxis: { type: 'value', ...this.chartAxisStyle(this.analysisSummary.unit || 'kWh'), splitNumber: 4 },
        yAxis: {
          type: 'category',
          data: items.map((item) => item.label),
          axisLabel: {
            color: '#596982',
            fontSize: 11,
            width: 80,
            overflow: 'truncate',
          },
          axisTick: { show: false },
        },
        series: [{
          type: 'bar',
          barMaxWidth: 18,
          data: items.map((item) => ({
            value: item.value,
            itemStyle: {
              color: item.label.includes('当前') ? '#0f6adf' : '#7f90aa',
              borderRadius: 8,
            },
          })),
          label: {
            show: true,
            position: 'right',
            color: '#42526d',
            fontSize: 11,
            formatter: ({ value }) => `${this.formatNumber(value, 1)}`,
          },
        }],
      }, true);
    },
    renderAnomalyTypeChart(byType) {
      const chart = this.ensureChart('anomalyType', 'anomalyTypeChart');
      if (!chart) return;
      chart.setOption({
        tooltip: { trigger: 'item' },
        legend: { bottom: 0, textStyle: { color: '#5f6c84' } },
        series: [{ type: 'pie', radius: ['34%', '72%'], data: Object.entries(byType).map(([name, value]) => ({ name, value })) }],
      });
    },
    async syncVisibleCharts() {
      await nextTick();
      if (this.activePage === 'overview') {
        this.renderOverviewChart();
      } else if (this.activePage === 'analysis' && !this.analysisUnsupportedMessage) {
        this.renderTrendChart();
        this.renderHourlyProfileChart();
        this.renderSplitChart();
        this.renderCompareChart();
      } else if (this.activePage === 'anomaly') {
        this.renderAnomalyTypeChart(this.anomalyTypeStats);
      }
    },
    resizeCharts() {
      this.syncVisibleCharts();
    },
  },
})
  .use(ElementPlus)
  .mount('#app');
