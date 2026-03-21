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
      activePage: 'overview',
      buildings: [],
      globalRangeText: '-',
      filters: { buildingId: '', range: [] },
      anomalyQuery: {
        severity: '',
        status: '',
        sort: 'timestamp_desc',
        page: 1,
        pageSize: 20,
        total: 0,
      },
      trendSeries: [],
      rankRows: [],
      anomalyRows: [],
      anomalyTypeStats: {},
      overview: { totalKwh: 0, avgKwh: 0, anomalyCount: 0, savingPct: 0, carbonKg: 0 },
      chatInput: '',
      aiProvider: 'template',
      aiStats: {
        windowHours: 24,
        totalCalls: 0,
        llmCalls: 0,
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
      health: {
        status: 'unknown',
        regression: 'unknown',
      },
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
        rank: false,
        anomaly: false,
        ai: false,
        detail: false,
        action: false,
      },
      errors: { overview: '', trend: '', rank: '', anomaly: '' },
      charts: { overview: null, trend: null, rank: null, anomalyType: null },
      refreshTimer: null,
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
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
      const json = await res.json();
      if (json.code !== 0) throw new Error(json.message || 'API error');
      return json.data;
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
    async bootstrap() {
      await this.loadBuildings();
      await this.loadSystemHealth();
      await this.loadAiStats();
      await this.loadAiEvaluate();
      await this.refreshAll();
    },
    onBuildingChange() {
      this.anomalyQuery.page = 1;
      this.refreshCurrentPage();
    },
    onDateRangeChange() {
      if (this.refreshTimer) clearTimeout(this.refreshTimer);
      this.refreshTimer = setTimeout(() => {
        this.anomalyQuery.page = 1;
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
        this.buildings = (data.items || []).map((x) => ({ id: x.building_id, name: x.building_name, type: x.building_type }));
        const range = data.global_range || {};
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
    async loadRank() {
      this.loading.rank = true;
      this.errors.rank = '';
      try {
        const data = await this.fetchJson(`${API_BASE}/api/energy/rank`);
        this.rankRows = data.items || [];
      } catch (err) {
        console.error(err);
        this.errors.rank = '排名数据加载失败';
      } finally {
        this.loading.rank = false;
      }
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
      } catch (err) {
        console.error(err);
        this.health.status = 'unknown';
        this.health.regression = 'unknown';
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
      await Promise.all([this.loadOverviewMetrics(), this.loadTrend(), this.loadRank(), this.loadAnomalies()]);
      await this.syncVisibleCharts();
    },
    async refreshCurrentPage() {
      if (this.activePage === 'overview') {
        await Promise.all([this.loadOverviewMetrics(), this.loadTrend(), this.loadAnomalies()]);
        await this.syncVisibleCharts();
        return;
      }
      if (this.activePage === 'analysis') {
        await Promise.all([this.loadOverviewMetrics(), this.loadTrend(), this.loadRank()]);
        await this.syncVisibleCharts();
        return;
      }
      if (this.activePage === 'anomaly') {
        await Promise.all([this.loadOverviewMetrics(), this.loadAnomalies()]);
        await this.syncVisibleCharts();
        return;
      }
      await Promise.all([this.loadOverviewMetrics(), this.loadAnomalies()]);
      await this.syncVisibleCharts();
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
      if (this.charts[key]) return this.charts[key];
      this.charts[key] = echarts.init(node);
      return this.charts[key];
    },
    renderOverviewChart() {
      const chart = this.ensureChart('overview', 'overviewChart');
      if (!chart) return;
      const data = this.trendSeries.slice(-72);
      chart.setOption({
        tooltip: { trigger: 'axis' },
        xAxis: { type: 'category', data: data.map((i) => i.timestamp.slice(5, 16)) },
        yAxis: { type: 'value', name: 'kWh' },
        series: [{ type: 'line', data: data.map((i) => i.value), smooth: true, areaStyle: {} }],
      });
    },
    renderTrendChart() {
      const chart = this.ensureChart('trend', 'trendChart');
      if (!chart) return;
      const data = this.trendSeries;
      chart.setOption({
        tooltip: { trigger: 'axis' },
        dataZoom: [{ type: 'inside' }, { type: 'slider' }],
        xAxis: { type: 'category', data: data.map((i) => i.timestamp.slice(0, 16)) },
        yAxis: { type: 'value', name: 'kWh' },
        series: [{ name: '用电量', type: 'line', data: data.map((i) => i.value), showSymbol: false, smooth: true }],
      });
    },
    renderRankChart() {
      const chart = this.ensureChart('rank', 'rankChart');
      if (!chart) return;
      chart.setOption({
        tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
        xAxis: { type: 'value', name: 'kWh' },
        yAxis: { type: 'category', data: this.rankRows.map((r) => r.building_name) },
        series: [{ type: 'bar', data: this.rankRows.map((r) => r.avg_kwh), itemStyle: { color: '#1368ce' } }],
      });
    },
    renderAnomalyTypeChart(byType) {
      const chart = this.ensureChart('anomalyType', 'anomalyTypeChart');
      if (!chart) return;
      chart.setOption({
        tooltip: { trigger: 'item' },
        series: [{ type: 'pie', radius: ['35%', '70%'], data: Object.entries(byType).map(([name, value]) => ({ name, value })) }],
      });
    },
    async syncVisibleCharts() {
      await nextTick();
      if (this.activePage === 'overview') {
        this.renderOverviewChart();
      } else if (this.activePage === 'analysis') {
        this.renderTrendChart();
        this.renderRankChart();
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
