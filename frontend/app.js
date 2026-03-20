const { createApp, nextTick } = Vue;

const API_BASE = '';

function buildQuery(params) {
  const q = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== '') {
      q.set(k, v);
    }
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
      filters: {
        buildingId: '',
        range: [],
      },
      anomalyQuery: {
        severity: '',
        sort: 'timestamp_desc',
        page: 1,
        pageSize: 20,
        total: 0,
      },
      trendSeries: [],
      rankRows: [],
      anomalyRows: [],
      overview: {
        totalKwh: 0,
        avgKwh: 0,
        anomalyCount: 0,
        savingPct: 0,
        carbonKg: 0,
      },
      chatInput: '',
      diagnosis: null,
      selectedAnomaly: null,
      anomalyDetailVisible: false,
      anomalyDetail: null,
      loading: {
        overview: false,
        trend: false,
        rank: false,
        anomaly: false,
        ai: false,
        detail: false,
      },
      errors: {
        overview: '',
        trend: '',
        rank: '',
        anomaly: '',
      },
      charts: {
        overview: null,
        trend: null,
        rank: null,
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
    if (this.refreshTimer) {
      clearTimeout(this.refreshTimer);
      this.refreshTimer = null;
    }
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
      const range = this.filters.range || [];
      const hasCompleteRange = this.isCompleteRange(range);
      const [start, end] = hasCompleteRange ? range : [undefined, undefined];
      return {
        building_id: this.filters.buildingId,
        start_time: start,
        end_time: end,
      };
    },
    async bootstrap() {
      await this.loadBuildings();
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
        this.buildings = (data.items || []).map((x) => ({
          id: x.building_id,
          name: x.building_name,
          type: x.building_type,
          startTime: x.start_time,
          endTime: x.end_time,
        }));
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
        await nextTick();
        this.renderOverviewChart();
        this.renderTrendChart();
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
        await nextTick();
        this.renderRankChart();
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
        const query = {
          ...this.getTimeParams(),
          severity: this.anomalyQuery.severity,
          sort: this.anomalyQuery.sort,
          page: this.anomalyQuery.page,
          page_size: this.anomalyQuery.pageSize,
        };
        const data = await this.fetchJson(`${API_BASE}/api/anomaly/list${buildQuery(query)}`);
        this.anomalyRows = data.items || [];
        this.anomalyQuery.total = data.total_count || 0;
        this.anomalyQuery.page = data.page || this.anomalyQuery.page;
        this.anomalyQuery.pageSize = data.page_size || this.anomalyQuery.pageSize;
        await nextTick();
        this.renderAnomalyTypeChart(data.by_type || {});
      } catch (err) {
        console.error(err);
        this.errors.anomaly = '异常数据加载失败';
        this.anomalyRows = [];
      } finally {
        this.loading.anomaly = false;
      }
    },
    async refreshAll() {
      await Promise.all([this.loadOverviewMetrics(), this.loadTrend(), this.loadRank(), this.loadAnomalies()]);
    },
    async refreshCurrentPage() {
      if (this.activePage === 'overview') {
        await Promise.all([this.loadOverviewMetrics(), this.loadTrend(), this.loadAnomalies()]);
        return;
      }
      if (this.activePage === 'analysis') {
        await Promise.all([this.loadOverviewMetrics(), this.loadTrend(), this.loadRank()]);
        return;
      }
      if (this.activePage === 'anomaly') {
        await Promise.all([this.loadOverviewMetrics(), this.loadAnomalies()]);
        return;
      }
      await Promise.all([this.loadOverviewMetrics(), this.loadAnomalies()]);
    },
    switchPage(key) {
      this.activePage = key;
      this.refreshCurrentPage();
    },
    async openAnomalyDetail(row) {
      this.loading.detail = true;
      try {
        const data = await this.fetchJson(`${API_BASE}/api/anomaly/detail?anomaly_id=${encodeURIComponent(row.anomaly_id)}`);
        this.anomalyDetail = data;
        this.anomalyDetailVisible = true;
      } catch (err) {
        console.error(err);
        this.$message.error('异常详情加载失败');
      } finally {
        this.loading.detail = false;
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
    async submitDiagnosis() {
      if (!this.chatInput.trim() && !this.selectedAnomaly) {
        this.$message.warning('请先输入问题或从异常列表触发诊断');
        return;
      }
      this.loading.ai = true;
      try {
        const payload = {
          message: this.chatInput,
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
      } catch (err) {
        console.error(err);
        this.$message.error('诊断失败，请稍后重试');
      } finally {
        this.loading.ai = false;
      }
    },
    ensureChart(key, domId) {
      const node = document.getElementById(domId);
      if (!node || typeof echarts === 'undefined') return null;
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
      const rows = this.rankRows;
      chart.setOption({
        tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
        xAxis: { type: 'value', name: 'kWh' },
        yAxis: { type: 'category', data: rows.map((r) => r.building_name) },
        series: [{ type: 'bar', data: rows.map((r) => r.avg_kwh), itemStyle: { color: '#1368ce' } }],
      });
    },
    renderAnomalyTypeChart(byType) {
      const chart = this.ensureChart('anomalyType', 'anomalyTypeChart');
      if (!chart) return;
      const data = Object.entries(byType).map(([name, value]) => ({ name, value }));
      chart.setOption({
        tooltip: { trigger: 'item' },
        series: [{ type: 'pie', radius: ['35%', '70%'], data }],
      });
    },
    resizeCharts() {
      Object.values(this.charts).forEach((c) => {
        if (c) c.resize();
      });
    },
  },
})
  .use(ElementPlus)
  .mount('#app');
