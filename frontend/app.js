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
      },
      analysisCompare: {
        building: null,
        peer_group: null,
        items: [],
        peer_ranking: [],
        message: '',
      },
      analysisInsight: null,
      analysisFeedbackLabel: '',
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
      health: { status: 'unknown', regression: 'unknown' },
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
    currentAnalysisScopeText() {
      const building = this.buildings.find((item) => item.id === this.filters.buildingId);
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
      };
      this.analysisCompare = {
        building: null,
        peer_group: null,
        items: [],
        peer_ranking: [],
        message: '',
      };
    },
    setAnalysisUnsupported(message) {
      this.analysisUnsupportedMessage = message || '暂未接入该分析类型';
      this.clearAnalysisData();
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
      this.refreshCurrentPage();
    },
    onAnalysisMetricChange() {
      this.analysisInsight = null;
      this.analysisFeedbackLabel = '';
      if (this.activePage === 'analysis' || this.activePage === 'assistant') {
        this.refreshCurrentPage();
      }
    },
    onDateRangeChange() {
      if (this.refreshTimer) clearTimeout(this.refreshTimer);
      this.refreshTimer = setTimeout(() => {
        this.anomalyQuery.page = 1;
        this.analysisInsight = null;
        this.analysisFeedbackLabel = '';
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
    async loadAnalysisSummary() {
      this.loading.analysisSummary = true;
      this.errors.analysisSummary = '';
      try {
        const data = await this.fetchJson(`${API_BASE}/api/analysis/summary${buildQuery(this.getAnalysisParams())}`);
        this.analysisUnsupportedMessage = '';
        this.analysisSummary = data;
      } catch (err) {
        console.error(err);
        if (String(err.message || '').includes('暂未接入')) {
          this.setAnalysisUnsupported(err.message);
          return;
        }
        this.errors.analysisSummary = '分析摘要加载失败';
      } finally {
        this.loading.analysisSummary = false;
      }
    },
    async loadAnalysisTrend() {
      this.loading.analysisTrend = true;
      this.errors.analysisTrend = '';
      try {
        const data = await this.fetchJson(`${API_BASE}/api/analysis/trend${buildQuery(this.getAnalysisParams())}`);
        this.analysisTrend = {
          ...data,
          overlayAvailable: !!data.overlay_available,
          summary: data.summary || { window_change_pct: 0, temperature_correlation: 0 },
        };
      } catch (err) {
        console.error(err);
        if (String(err.message || '').includes('暂未接入')) {
          this.setAnalysisUnsupported(err.message);
          return;
        }
        this.errors.analysisTrend = '分析趋势加载失败';
      } finally {
        this.loading.analysisTrend = false;
      }
    },
    async loadAnalysisDistribution() {
      this.loading.analysisDistribution = true;
      this.errors.analysisDistribution = '';
      try {
        const data = await this.fetchJson(`${API_BASE}/api/analysis/distribution${buildQuery(this.getAnalysisParams())}`);
        this.analysisDistribution = data;
      } catch (err) {
        console.error(err);
        if (String(err.message || '').includes('暂未接入')) {
          this.setAnalysisUnsupported(err.message);
          return;
        }
        this.errors.analysisDistribution = '结构分析加载失败';
      } finally {
        this.loading.analysisDistribution = false;
      }
    },
    async loadAnalysisCompare() {
      this.loading.analysisCompare = true;
      this.errors.analysisCompare = '';
      try {
        const data = await this.fetchJson(`${API_BASE}/api/analysis/compare${buildQuery(this.getAnalysisParams())}`);
        this.analysisCompare = data;
      } catch (err) {
        console.error(err);
        if (String(err.message || '').includes('暂未接入')) {
          this.setAnalysisUnsupported(err.message);
          return;
        }
        this.errors.analysisCompare = '对比分析加载失败';
      } finally {
        this.loading.analysisCompare = false;
      }
    },
    async loadAnalysisWorkspace() {
      this.analysisUnsupportedMessage = '';
      await this.loadAnalysisSummary();
      if (this.analysisUnsupportedMessage) {
        await nextTick();
        return;
      }
      await Promise.all([this.loadAnalysisTrend(), this.loadAnalysisDistribution(), this.loadAnalysisCompare()]);
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
          message: `${this.currentAnalysisScopeText()}，请输出结构化分析结论。`,
        };
        const data = await this.fetchJson(`${API_BASE}/api/ai/analyze`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        this.analysisInsight = data.analysis || null;
        this.analysisFeedbackLabel = '';
        this.activePage = 'assistant';
        await this.loadAiStats();
        await this.loadAiEvaluate();
      } catch (err) {
        console.error(err);
        this.$message.error(`分析结论生成失败：${err.message}`);
      } finally {
        this.loading.analysisInsight = false;
      }
    },
    async runAnalysisAssistant() {
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
      const interval = count > 600 ? 119 : count > 240 ? 47 : count > 96 ? 11 : count > 48 ? 5 : 0;
      return {
        interval,
        hideOverlap: true,
        formatter: (value) => {
          const text = String(value || '');
          if (count > 240) return text.slice(0, 10);
          return text.slice(5, 16).replace('T', ' ');
        },
      };
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
      const rawWeather = this.analysisTrend.weather_series || [];
      let bucketHours = 1;
      if (rawSeries.length > 1200) bucketHours = 24;
      else if (rawSeries.length > 480) bucketHours = 12;
      else if (rawSeries.length > 240) bucketHours = 6;
      const series = this.bucketAnalysisSeries(rawSeries, bucketHours);
      const weatherSeries = this.bucketAnalysisSeries(rawWeather, bucketHours);
      return {
        series,
        weatherSeries,
        bucketHours,
      };
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
      const { series, weatherSeries, bucketHours } = this.buildTrendChartSeries();
      if (!series.length) {
        chart.clear();
        return;
      }
      const timeAxisLabel = this.buildTimeAxisLabels(series.map((item) => item.timestamp));
      const tooltipLabel = bucketHours > 1 ? `平均负荷（${bucketHours}h）` : `${this.currentMetricLabel()}负荷`;
      const option = {
        grid: { left: 60, right: 62, top: 52, bottom: 72 },
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
        dataZoom: series.length > 120
          ? [{ type: 'inside' }, { type: 'slider', height: 14, bottom: 16, brushSelect: false }]
          : [],
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
          },
        ],
      };
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
          data: items.map((item) => item.value),
          itemStyle: { color: '#0f6adf', borderRadius: 8 },
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
