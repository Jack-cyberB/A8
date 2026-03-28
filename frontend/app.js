const { createApp, nextTick } = Vue;

const API_BASE = '';

const PAGE_WORKSPACE_CONFIG = {
  overview: {
    label: '数据总览',
    kicker: 'Overview Workspace',
    description: '把总量、建筑画像和系统状态拆开看，首屏先回答现在发生了什么。',
    submodules: [
      { key: 'kpi', label: '核心指标', desc: '总量、趋势与当前能耗态势' },
      { key: 'profile', label: '建筑画像', desc: '当前建筑、范围与演示入口' },
      { key: 'status', label: '系统状态', desc: '接口、知识库与 AI 就绪情况' },
    ],
  },
  analysis: {
    label: '能耗分析',
    kicker: 'Analysis Workspace',
    description: '围绕当前建筑和时间范围，把趋势、结构、对标和节能机会分成独立任务工作区。',
    submodules: [
      { key: 'trend', label: '趋势分析', desc: '趋势变化与天气联动' },
      { key: 'structure', label: '结构分析与对标', desc: '分时段结构 + 同类样本对比' },
      { key: 'opportunity', label: '节能机会', desc: '高耗窗口与优化动作' },
    ],
  },
  anomaly: {
    label: '运维处置',
    kicker: 'Operations Workspace',
    description: '把异常总览、事件处理和复盘拆开，形成更清晰的处置闭环。',
    submodules: [
      { key: 'board', label: '异常看板', desc: '异常分布与近期态势' },
      { key: 'events', label: '事件列表', desc: '过滤、处理和导出' },
      { key: 'review', label: '处理复盘', desc: '闭环事件与复盘入口' },
    ],
  },
  assistant: {
    label: '智能助手',
    kicker: 'Smart O&M Workspace',
    description: '不再让系统猜你的意图，先选任务子模块，再生成专用结果。',
    submodules: [
      { key: 'knowledge', label: '知识问答', desc: '知识检索与依据引用' },
      { key: 'saving', label: '节能建议', desc: '针对当前范围输出优化动作' },
      { key: 'diagnosis', label: '异常诊断', desc: '围绕异常定位原因与步骤' },
      { key: 'interpretation', label: '分析解读', desc: '解释当前分析结论与动作' },
    ],
  },
};

const ASSISTANT_SUBMODULE_CONFIG = {
  knowledge: {
    label: '知识问答',
    kicker: 'Knowledge Workspace',
    title: '知识问答工作台',
    placeholder: '输入运维制度、设备原理、排查规范等问题。Enter 发送，Shift+Enter 换行',
    welcomeTitle: '先问知识，再看依据。',
    welcomeDesc: '这里专门处理知识库问答。点击回答里的引用编号，可以先看片段，再看原文。',
  },
  saving: {
    label: '节能建议',
    kicker: 'Energy Saving Workspace',
    title: '节能建议工作台',
    placeholder: '输入节能优化目标、重点时段或建筑对象。Enter 发送，Shift+Enter 换行',
    welcomeTitle: '围绕当前分析，直接生成节能动作。',
    welcomeDesc: '这里不再混入诊断和知识检索，只聚焦节能结论、优先动作和收益影响。',
  },
  diagnosis: {
    label: '异常诊断',
    kicker: 'Diagnosis Workspace',
    title: '异常诊断工作台',
    placeholder: '输入异常现象、时间或对象，生成原因、步骤和动作。Enter 发送，Shift+Enter 换行',
    welcomeTitle: '围绕异常对象输出结构化诊断。',
    welcomeDesc: '这里专门处理异常原因、排查步骤、立即动作和预防建议，适合从运维处置页直接进入。',
  },
  interpretation: {
    label: '分析解读',
    kicker: 'Interpretation Workspace',
    title: '分析解读工作台',
    placeholder: '输入你想解释的趋势、结构或对标问题。Enter 发送，Shift+Enter 换行',
    welcomeTitle: '把分析结果翻译成能汇报、能执行的结论。',
    welcomeDesc: '这里只解释当前图表和范围，不混入知识依据和诊断模板。',
  },
};

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
        { key: 'anomaly', label: '运维处置' },
        { key: 'assistant', label: '智能助手' },
      ],
      analysisMetrics: [
        { value: 'electricity', label: '电力' },
        { value: 'water', label: '水' },
        { value: 'hvac', label: '空调' },
        { value: 'environment', label: '环境' },
      ],
      activePage: 'overview',
      activeSubmoduleMap: {
        overview: 'kpi',
        analysis: 'trend',
        anomaly: 'board',
        assistant: 'knowledge',
      },
      assistantSubmodule: 'knowledge',
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
          building_name: '未选择建筑',
          building_type: '',
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
      assistantPromptDraft: '',
      assistantPromptKind: 'analysis',
      assistantMode: 'knowledge',
      assistantKnowledgeInput: '',
      assistantChat: {
        loading: false,
        sessionId: '',
        error: '',
        lastLatencyMs: 0,
        messages: [],
      },
      unifiedChat: {
        messages: [],
        input: '',
        loading: false,
        streaming: false,
        streamBuffer: '',
        sessionId: '',
      },
      activeKnowledgeCitation: {
        messageId: '',
        citationIndex: null,
        title: '',
        sourceType: '',
        documentKey: '',
      },
      knowledgeSnippetOverlay: {
        visible: false,
        messageId: '',
        citationIndex: null,
        title: '',
        sourceType: '',
        similarity: null,
        content: '',
      },
      knowledgeDocumentViewer: {
        loading: false,
        messageId: '',
        citationIndex: null,
        title: '',
        sourceType: '',
        documentKey: '',
        content: '',
        format: 'markdown',
        expanded: false,
      },
      knowledgeDocumentCache: {},
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
      health: {
        status: 'unknown',
        regression: 'unknown',
        aiConfigured: false,
        aiModelReady: false,
        ragflowConfigured: false,
        ragflowEnabled: false,
        ragflowDatasetCount: 0,
        ragflowStandardDatasetCount: 0,
        ragflowStandardConfigured: false,
        ragflowChatReady: false,
        ragflowChatId: '',
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
    cleanRagflowAnswerText(text) {
      if (text === undefined || text === null) return '';
      return String(text)
        .replace(/\r\n/g, '\n')
        .replace(/\r/g, '\n');
    },
    formatKnowledgeAnswerText(text) {
      return this.cleanRagflowAnswerText(text);
    },
    escapeHtml(text) {
      return String(text || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
    },
    escapeHtmlAttribute(text) {
      return String(text || '')
        .replace(/&/g, '&amp;')
        .replace(/"/g, '&quot;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
    },
    simpleMarkdownToHtml(text) {
      return String(text || '')
        .split(/\n{2,}/)
        .map((block) => block.trim())
        .filter(Boolean)
        .map((block) => `<p>${block.replace(/\n/g, '<br>')}</p>`)
        .join('');
    },
    renderKnowledgeDocument(text, format = 'markdown') {
      const formatted = this.cleanRagflowAnswerText(text);
      if (!formatted) return '';
      const safeMarkdown = this.escapeHtml(formatted);
      if (format === 'markdown' && typeof marked !== 'undefined') {
        try {
          return marked.parse(safeMarkdown, { breaks: true, gfm: true });
        } catch (e) {}
      }
      return this.simpleMarkdownToHtml(safeMarkdown);
    },
    decorateKnowledgeAnswerHtml(html, messageId = '', references = []) {
      const availableCount = Array.isArray(references) ? references.length : 0;
      const safeMessageId = this.escapeHtmlAttribute(messageId);
      return String(html || '').replace(/\[ID:\s*(\d+)\]/gi, (_, idxText) => {
        const idx = Number(idxText);
        const disabled = !Number.isInteger(idx) || idx < 0 || idx >= availableCount;
        const isActive =
          this.activeKnowledgeCitation.messageId === messageId &&
          this.activeKnowledgeCitation.citationIndex === idx;
        const className = [
          'knowledge-citation',
          disabled ? 'knowledge-citation--disabled' : '',
          isActive ? 'knowledge-citation--active' : '',
        ].filter(Boolean).join(' ');
        const attrs = disabled
          ? `class="${className}" disabled`
          : `class="${className}" data-message-id="${safeMessageId}" data-ref-index="${idx}"`;
        return `<button type="button" ${attrs}>[ID:${idx}]</button>`;
      });
    },
    renderKnowledgeAnswer(text, messageId = '', references = []) {
      const formatted = this.formatKnowledgeAnswerText(text);
      if (!formatted) return '';
      const safeMarkdown = this.escapeHtml(formatted);
      let html = '';
      if (typeof marked !== 'undefined') {
        try {
          html = marked.parse(safeMarkdown, { breaks: true, gfm: true });
        } catch (e) {}
      }
      if (!html) {
        html = this.simpleMarkdownToHtml(safeMarkdown);
      }
      return this.decorateKnowledgeAnswerHtml(html, messageId, references);
    },
    renderMarkdown(text) {
      if (!text || typeof text !== 'string') return '';
      const clean = this.cleanRagflowAnswerText(text);
      if (typeof marked !== 'undefined') {
        try { return marked.parse(clean); } catch(e) {}
      }
      return clean.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/\n/g,'<br>');
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
        error: '请求失败',
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
    knowledgeSourceLabel(value) {
      const map = {
        ragflow: '场景知识',
        standard: '标准规范',
        mixed: '双知识源',
        local: '本地知识',
        local_knowledge: '本地知识',
        none: '未命中知识',
        llm_generated: 'LLM生成',
      };
      return map[value] || value || '未命中知识';
    },
    analysisInsightPendingText() {
      return '请先在能耗分析页确认建筑和时间范围，再点击“AI 分析”进入这里生成完整结论。页面筛选和刷新不会自动消耗额度。';
    },
    aiAvailabilityText() {
      if (this.health.aiConfigured && this.health.ragflowConfigured && this.health.ragflowChatReady) {
        const standardText = this.health.ragflowStandardConfigured ? `，标准库 ${this.health.ragflowStandardDatasetCount || 0} 个数据集已接入` : '';
        return `DeepSeek 与 RAGFlow 已就绪。当前场景库 ${this.health.ragflowDatasetCount || 0} 个数据集${standardText}，知识问答会按问题类型选择知识源。`;
      }
      if (this.health.aiConfigured) {
        return 'DeepSeek 已就绪，但 RAGFlow 知识库尚未完整接通。';
      }
      return '当前环境尚未配置 DeepSeek API Key，点击后会自动使用模板兜底，不会影响主流程。';
    },
    ragflowStatusText() {
      if (this.health.ragflowConfigured && this.health.ragflowChatReady) {
        const standardText = this.health.ragflowStandardConfigured
          ? ` · 标准库 ${this.health.ragflowStandardDatasetCount || 0} 个`
          : '';
        return `RAGFlow 知识问答已就绪 · 场景库 ${this.health.ragflowDatasetCount || 0} 个${standardText}`;
      }
      if (this.health.ragflowConfigured) {
        return `RAGFlow 检索已配置 · 会话待就绪`;
      }
      return 'RAGFlow 未配置';
    },
    assistantKnowledgeStarters() {
      const scope = this.analysisInsights.scope_summary || {};
      const building = scope.building_name || '当前建筑';
      const anomaly = this.selectedAnomaly;
      return [
        {
          key: 'analysis-focus',
          title: '当前分析解读',
          question: `结合${building}在${this.currentAnalysisScopeText()}下的表现，解释夜间基线、非工作时段负荷和节能优化上最该关注什么。`,
        },
        {
          key: 'cooling-check',
          title: '空调运行排查',
          question: `如果${building}在高温白天空调用电偏高，运维上应优先排查哪些系统与控制策略？`,
        },
        {
          key: 'anomaly-focus',
          title: '当前异常知识追问',
          question: anomaly
            ? `针对${anomaly.building_type || '当前'}建筑出现“${anomaly.anomaly_name}”，从运维常识看优先排查哪些设备系统？为什么这种现象会发生？`
            : `针对${building}出现异常负荷波动时，通常应从哪些设备系统和控制策略开始排查？`,
        },
      ];
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
    healthToneClass(value) {
      return value === 'ok' ? 'is-success' : 'is-warning';
    },
    regressionToneClass(value) {
      return value === 'pass' ? 'is-success' : 'is-warning';
    },
    severityLabel(value) {
      const map = { high: '高', medium: '中', low: '低' };
      return map[value] || value || '-';
    },
    severityToneClass(value) {
      const map = { high: 'is-danger', medium: 'is-warning', low: 'is-success' };
      return map[value] || 'is-info';
    },
    severityTagType(value) {
      const map = { high: 'danger', medium: 'warning', low: 'info' };
      return map[value] || 'info';
    },
    statusDisplay(value) {
      const map = { new: '新告警', acknowledged: '已确认', ignored: '已忽略', resolved: '已完成' };
      return map[value] || value || '-';
    },
    statusToneClass(value) {
      const map = { new: 'is-danger', acknowledged: 'is-warning', ignored: 'is-info', resolved: 'is-success' };
      return map[value] || 'is-info';
    },
    statusTagType(value) {
      const map = { new: 'danger', acknowledged: 'warning', ignored: 'info', resolved: 'success' };
      return map[value] || 'info';
    },
    priorityToneClass(level) {
      const map = { high: 'is-danger', medium: 'is-warning', low: 'is-success' };
      return map[level] || 'is-info';
    },
    priorityLabel(level) {
      const map = { high: '高优先', medium: '中优先', low: '低优先' };
      return map[level] || '一般';
    },
    pageWorkspaceConfig(pageKey = this.activePage) {
      return PAGE_WORKSPACE_CONFIG[pageKey] || PAGE_WORKSPACE_CONFIG.overview;
    },
    currentPageSubmodules() {
      return this.pageWorkspaceConfig().submodules || [];
    },
    currentSubmoduleKey(pageKey = this.activePage) {
      return this.activeSubmoduleMap[pageKey];
    },
    currentSubmoduleConfig(pageKey = this.activePage) {
      return this.pageWorkspaceConfig(pageKey).submodules.find((item) => item.key === this.currentSubmoduleKey(pageKey)) || null;
    },
    currentSubmoduleLabel(pageKey = this.activePage) {
      return this.currentSubmoduleConfig(pageKey)?.label || '未选择';
    },
    assistantSubmoduleConfig() {
      return ASSISTANT_SUBMODULE_CONFIG[this.assistantSubmodule] || ASSISTANT_SUBMODULE_CONFIG.knowledge;
    },
    assistantTitle() {
      return this.assistantSubmoduleConfig().title;
    },
    assistantPlaceholder() {
      return this.assistantSubmoduleConfig().placeholder;
    },
    assistantWelcomeTitle() {
      return this.assistantSubmoduleConfig().welcomeTitle;
    },
    assistantWelcomeDescription() {
      return this.assistantSubmoduleConfig().welcomeDesc;
    },
    assistantKnowledgeStatusText() {
      if (this.health.ragflowChatReady && this.health.ragflowStandardConfigured) return '场景库 + 标准库可用';
      return this.health.ragflowChatReady ? '场景知识库可用' : '知识库待配置';
    },
    currentAssistantContextText() {
      if (this.assistantSubmodule === 'diagnosis') {
        if (this.selectedAnomaly) {
          return `${this.selectedAnomaly.building_name} · ${this.selectedAnomaly.anomaly_name} · ${this.formatCompactDateTime(this.selectedAnomaly.timestamp)}`;
        }
        return '等待从运维处置页带入异常对象，或直接输入异常现象。';
      }
      if (this.assistantSubmodule === 'knowledge') {
        return `${this.currentAnalysisScopeText()} · ${this.assistantKnowledgeStatusText()}`;
      }
      if (this.assistantSubmodule === 'saving') {
        return `${this.currentAnalysisScopeText()} · 聚焦节能机会和优先动作`;
      }
      return this.currentAnalysisScopeText();
    },
    assistantLiveStatusText() {
      const label = this.assistantSubmoduleConfig().label;
      if (this.unifiedChat.streaming) return `正在生成${label}结果`;
      if (this.unifiedChat.loading) return `正在准备${label}上下文`;
      return `等待输入${label}问题`;
    },
    assistantCapabilityText() {
      if (this.assistantSubmodule === 'knowledge') {
        const knowledge = this.health.ragflowChatReady ? '知识库问答可用' : '知识库问答待配置';
        const llm = this.health.aiConfigured ? 'DeepSeek 已接入' : 'DeepSeek 未配置';
        return `${knowledge} · ${llm}`;
      }
      if (this.assistantSubmodule === 'diagnosis') {
        return this.selectedAnomaly ? '已绑定异常对象，可直接诊断' : '可手动输入异常现象，也可从运维处置页带入';
      }
      if (this.assistantSubmodule === 'saving') {
        return '基于当前分析快照生成节能动作，不复用诊断模板';
      }
      if (this.assistantSubmodule === 'interpretation') {
        return '围绕趋势、结构和对标结论输出可汇报的解释';
      }
      const knowledge = this.health.ragflowChatReady ? '知识库问答可用' : '知识库问答待配置';
      const llm = this.health.aiConfigured ? 'DeepSeek 已接入' : 'DeepSeek 未配置';
      return `${knowledge} · ${llm}`;
    },
    assistantContextFacts() {
      const building = this.selectedBuildingMeta();
      const scopeText = this.isCompleteRange(this.filters.range) ? `${this.filters.range[0]} ~ ${this.filters.range[1]}` : this.currentValidRangeText();
      if (this.assistantSubmodule === 'diagnosis') {
        return [
          {
            label: '当前异常',
            value: this.selectedAnomaly ? this.selectedAnomaly.anomaly_name : '未绑定异常对象',
          },
          {
            label: '异常上下文',
            value: this.selectedAnomaly
              ? `${this.selectedAnomaly.building_name} · 偏差 ${this.formatNumber(this.selectedAnomaly.deviation_pct, 1)}%`
              : '可从事件列表进入，也可直接输入异常描述',
          },
          {
            label: '时间窗口',
            value: this.selectedAnomaly ? this.formatCompactDateTime(this.selectedAnomaly.timestamp) : scopeText,
          },
        ];
      }
      if (this.assistantSubmodule === 'knowledge') {
        return [
          { label: '当前建筑', value: building ? building.name : '未选择建筑' },
          { label: '时间范围', value: scopeText },
          { label: '知识状态', value: this.assistantKnowledgeStatusText() },
        ];
      }
      if (this.assistantSubmodule === 'saving') {
        return [
          { label: '当前对象', value: this.currentMetricLabel() },
          { label: '当前范围', value: scopeText },
          { label: '优化来源', value: `${this.analysisInsights.saving_opportunities.length} 个节能机会点` },
        ];
      }
      return [
        { label: '分析对象', value: this.currentMetricLabel() },
        { label: '当前建筑', value: building ? building.name : '未选择建筑' },
        { label: '时间窗口', value: scopeText },
      ];
    },
    normalizeUnifiedKnowledgeReferences(references = []) {
      return (Array.isArray(references) ? references : []).map((item, index) => {
        const similarityValue = Number(item?.similarity);
        return {
          id: item?.id || item?.chunk_id || `knowledge-ref-${index}`,
          chunk_id: item?.chunk_id || item?.id || `knowledge-ref-${index}`,
          citation_index: index,
          title: String(item?.title || '知识片段').replace(/\.(md|markdown|txt|pdf)$/i, ''),
          excerpt: String(item?.excerpt || '').replace(/\r\n/g, '\n').replace(/\r/g, '\n').trim(),
          snippet_text: String(item?.snippet_text || item?.excerpt || '').replace(/\r\n/g, '\n').replace(/\r/g, '\n').trim(),
          document_key: String(item?.document_key || item?.title || '').replace(/\.(md|markdown|txt|pdf)$/i, '').trim(),
          source_type: item?.source_type || item?.sourceType || 'ragflow',
          similarity: Number.isFinite(similarityValue) ? similarityValue : null,
          section: item?.section || '',
        };
      });
    },
    referenceSimilarityText(ref) {
      const value = Number(ref?.similarity);
      if (!Number.isFinite(value)) return '';
      return `相似度 ${this.formatNumber(value * 100, 1)}%`;
    },
    resetKnowledgeReferenceState({ preserveCache = true } = {}) {
      this.activeKnowledgeCitation = {
        messageId: '',
        citationIndex: null,
        title: '',
        sourceType: '',
        documentKey: '',
      };
      this.knowledgeSnippetOverlay = {
        visible: false,
        messageId: '',
        citationIndex: null,
        title: '',
        sourceType: '',
        similarity: null,
        content: '',
      };
      this.knowledgeDocumentViewer = {
        loading: false,
        messageId: '',
        citationIndex: null,
        title: '',
        sourceType: '',
        documentKey: '',
        content: '',
        format: 'markdown',
        expanded: false,
      };
      if (!preserveCache) {
        this.knowledgeDocumentCache = {};
      }
    },
    closeKnowledgeSnippetOverlay() {
      this.knowledgeSnippetOverlay = {
        ...this.knowledgeSnippetOverlay,
        visible: false,
      };
    },
    knowledgeDocumentCacheKey(ref) {
      const sourceType = String(ref?.source_type || ref?.sourceType || 'ragflow').trim() || 'ragflow';
      const documentKey = String(ref?.document_key || ref?.documentKey || ref?.title || '').trim();
      return `${sourceType}:${documentKey}`;
    },
    normalizeKnowledgeSearchText(text) {
      return String(text || '')
        .replace(/\r\n/g, '\n')
        .replace(/\r/g, '\n')
        .replace(/\s+/g, ' ')
        .trim()
        .toLowerCase();
    },
    async loadKnowledgeDocument(ref, messageId, citationIndex, expanded = false) {
      const cacheKey = this.knowledgeDocumentCacheKey(ref);
      const fallbackContent = ref?.snippet_text || ref?.excerpt || '';
      if (cacheKey && this.knowledgeDocumentCache[cacheKey]) {
        this.knowledgeDocumentViewer = {
          ...this.knowledgeDocumentCache[cacheKey],
          messageId,
          citationIndex,
          expanded,
        };
        await this.$nextTick();
        if (expanded) {
          this.focusKnowledgeDocumentMatch(ref);
        }
        return;
      }

      this.knowledgeDocumentViewer = {
        loading: true,
        messageId,
        citationIndex,
        title: ref?.title || '原文',
        sourceType: ref?.source_type || ref?.sourceType || 'ragflow',
        documentKey: ref?.document_key || ref?.documentKey || '',
        content: fallbackContent,
        format: 'markdown',
        expanded,
      };

      try {
        const data = await this.fetchJson(`${API_BASE}/api/ragflow/reference/document`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            title: ref?.title || '',
            source_type: ref?.source_type || ref?.sourceType || 'ragflow',
            document_key: ref?.document_key || ref?.documentKey || '',
          }),
        });
        const viewer = {
          loading: false,
          messageId,
          citationIndex,
          title: data?.title || ref?.title || '原文',
          sourceType: data?.source_type || ref?.source_type || ref?.sourceType || 'ragflow',
          documentKey: data?.document_key || ref?.document_key || ref?.documentKey || '',
          content: data?.content || fallbackContent,
          format: data?.format || 'markdown',
          expanded,
        };
        this.knowledgeDocumentViewer = viewer;
        if (cacheKey) {
          this.knowledgeDocumentCache = {
            ...this.knowledgeDocumentCache,
            [cacheKey]: {
              ...viewer,
              messageId: '',
              citationIndex: null,
              expanded: false,
            },
          };
        }
      } catch (_) {
        this.knowledgeDocumentViewer = {
          loading: false,
          messageId,
          citationIndex,
          title: ref?.title || '原文',
          sourceType: ref?.source_type || ref?.sourceType || 'ragflow',
          documentKey: ref?.document_key || ref?.documentKey || '',
          content: fallbackContent,
          format: 'markdown',
          expanded,
        };
      }

      await this.$nextTick();
      if (expanded) {
        this.focusKnowledgeDocumentMatch(ref);
      }
    },
    focusKnowledgeDocumentMatch(ref) {
      const scrollContainer = this.$refs.knowledgeDocumentScroll;
      const contentContainer = this.$refs.knowledgeDocumentContent;
      const scrollEl = Array.isArray(scrollContainer) ? scrollContainer[0] : scrollContainer;
      const contentEl = Array.isArray(contentContainer) ? contentContainer[0] : contentContainer;
      if (!contentEl) return;

      contentEl.querySelectorAll('.is-knowledge-match').forEach((node) => node.classList.remove('is-knowledge-match'));

      const needleSource = this.normalizeKnowledgeSearchText(ref?.snippet_text || ref?.excerpt || '');
      const needle = needleSource.slice(0, 40);
      if (!needle) {
        if (scrollEl) scrollEl.scrollTop = 0;
        return;
      }

      const candidates = Array.from(contentEl.querySelectorAll('p, li, h1, h2, h3, h4, h5, h6, blockquote, pre'));
      const target = candidates.find((node) => this.normalizeKnowledgeSearchText(node.textContent).includes(needle));
      if (!target) {
        if (scrollEl) scrollEl.scrollTop = 0;
        return;
      }
      target.classList.add('is-knowledge-match');
      target.scrollIntoView({ behavior: 'smooth', block: 'center' });
      if (this._knowledgeDocumentMatchTimer) {
        clearTimeout(this._knowledgeDocumentMatchTimer);
      }
      this._knowledgeDocumentMatchTimer = setTimeout(() => {
        target.classList.remove('is-knowledge-match');
      }, 2200);
    },
    async openKnowledgeCitation(msg, citationIndex) {
      const ref = Array.isArray(msg?.references) ? msg.references[citationIndex] : null;
      if (!ref) return;
      const keepExpanded =
        this.knowledgeDocumentViewer.expanded &&
        this.knowledgeDocumentViewer.messageId === msg.id;
      this.activeKnowledgeCitation = {
        messageId: msg.id,
        citationIndex,
        title: ref.title || '知识片段',
        sourceType: ref.source_type || ref.sourceType || 'ragflow',
        documentKey: ref.document_key || ref.documentKey || '',
      };
      this.knowledgeSnippetOverlay = {
        visible: true,
        messageId: msg.id,
        citationIndex,
        title: ref.title || '知识片段',
        sourceType: ref.source_type || ref.sourceType || 'ragflow',
        similarity: ref.similarity,
        content: ref.snippet_text || ref.excerpt || '',
      };
      await this.loadKnowledgeDocument(ref, msg.id, citationIndex, keepExpanded);
    },
    activeKnowledgeReference(msg) {
      if (!msg || this.activeKnowledgeCitation.messageId !== msg.id) return null;
      const refs = Array.isArray(msg.references) ? msg.references : [];
      const idx = this.activeKnowledgeCitation.citationIndex;
      return Number.isInteger(idx) ? refs[idx] || null : null;
    },
    knowledgeDocumentEntryVisible(msg) {
      if (!msg) return false;
      return (
        this.activeKnowledgeCitation.messageId === msg.id ||
        this.knowledgeDocumentViewer.messageId === msg.id
      );
    },
    knowledgeDocumentExpanded(msg) {
      if (!msg) return false;
      return (
        this.knowledgeDocumentViewer.messageId === msg.id &&
        !!this.knowledgeDocumentViewer.expanded
      );
    },
    knowledgeDocumentEntryTitle(msg) {
      if (!msg) return '原文';
      if (this.knowledgeDocumentViewer.messageId === msg.id && this.knowledgeDocumentViewer.title) {
        return this.knowledgeDocumentViewer.title;
      }
      if (this.activeKnowledgeCitation.messageId === msg.id && this.activeKnowledgeCitation.title) {
        return this.activeKnowledgeCitation.title;
      }
      return '原文';
    },
    knowledgeDocumentEntrySource(msg) {
      if (!msg) return '';
      if (this.knowledgeDocumentViewer.messageId === msg.id && this.knowledgeDocumentViewer.sourceType) {
        return this.knowledgeDocumentViewer.sourceType;
      }
      if (this.activeKnowledgeCitation.messageId === msg.id && this.activeKnowledgeCitation.sourceType) {
        return this.activeKnowledgeCitation.sourceType;
      }
      return '';
    },
    knowledgeDocumentEntryCitationIndex(msg) {
      if (!msg) return null;
      if (
        this.activeKnowledgeCitation.messageId === msg.id &&
        Number.isInteger(this.activeKnowledgeCitation.citationIndex)
      ) {
        return this.activeKnowledgeCitation.citationIndex;
      }
      if (
        this.knowledgeDocumentViewer.messageId === msg.id &&
        Number.isInteger(this.knowledgeDocumentViewer.citationIndex)
      ) {
        return this.knowledgeDocumentViewer.citationIndex;
      }
      return null;
    },
    async toggleKnowledgeDocumentViewer(msg) {
      if (!this.knowledgeDocumentEntryVisible(msg)) return;
      const nextExpanded = !this.knowledgeDocumentExpanded(msg);
      const ref = this.activeKnowledgeReference(msg);
      const viewerMatchesActiveRef =
        !!ref &&
        this.knowledgeDocumentViewer.messageId === msg.id &&
        this.knowledgeDocumentViewer.citationIndex === this.activeKnowledgeCitation.citationIndex &&
        !!this.knowledgeDocumentViewer.content;

      if (ref && !viewerMatchesActiveRef) {
        await this.loadKnowledgeDocument(
          ref,
          msg.id,
          this.activeKnowledgeCitation.citationIndex,
          nextExpanded
        );
        return;
      }

      this.knowledgeDocumentViewer = {
        ...this.knowledgeDocumentViewer,
        messageId: msg.id,
        expanded: nextExpanded,
      };

      if (!nextExpanded) return;

      await this.$nextTick();
      if (ref) {
        this.focusKnowledgeDocumentMatch(ref);
      }
    },
    handleKnowledgeAnswerClick(event, msg) {
      const target = event?.target instanceof Element
        ? event.target.closest('.knowledge-citation[data-ref-index]')
        : null;
      if (!target) return;
      event.preventDefault();
      const citationIndex = Number(target.getAttribute('data-ref-index'));
      if (!Number.isInteger(citationIndex) || !Array.isArray(msg?.references) || !msg.references[citationIndex]) {
        return;
      }
      this.openKnowledgeCitation(msg, citationIndex);
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
      const buildingText = building ? building.name : '未选择建筑';
      const rangeText = this.isCompleteRange(this.filters.range) ? `${this.filters.range[0]} ~ ${this.filters.range[1]}` : '全量时间范围';
      return `${this.currentMetricLabel()} | ${buildingText} | ${rangeText}`;
    },
    overviewSeveritySummary(level) {
      return this.anomalyRows.filter((item) => item.severity === level).length;
    },
    anomalyCountByStatus(status) {
      return this.anomalyRows.filter((item) => item.status === status).length;
    },
    recentAnomalyRows(limit = 5) {
      return this.anomalyRows.slice(0, limit);
    },
    reviewableAnomalyRows(limit = 6) {
      return this.anomalyRows.filter((item) => item.status === 'resolved' || item.status === 'acknowledged').slice(0, limit);
    },
    topSavingImpactItems(limit = 3) {
      return (this.analysisInsights.saving_opportunities || [])
        .slice(0, limit)
        .map((item) => `${item.title}：影响估算 ${this.formatInsightKwh(item.estimated_loss_kwh)}`);
    },
    activateSubmodule(pageKey, submoduleKey) {
      const currentKey = this.activeSubmoduleMap[pageKey];
      this.activeSubmoduleMap = { ...this.activeSubmoduleMap, [pageKey]: submoduleKey };
      if (pageKey === 'assistant') {
        const changed = this.assistantSubmodule !== submoduleKey;
        this.assistantSubmodule = submoduleKey;
        if (changed) {
          this.resetUnifiedConversation({ silent: true });
          if (submoduleKey !== 'diagnosis') {
            this.selectedAnomaly = null;
          }
        }
      }
      if (this.activePage !== pageKey) {
        this.switchPage(pageKey);
        return;
      }
      if (currentKey !== submoduleKey) {
        this.$nextTick(() => {
          this.syncVisibleCharts();
          this.scrollAssistantToBottom();
        });
      }
    },
    navigateToAssistantSubmodule(submoduleKey, { question = '', send = false } = {}) {
      this.activateSubmodule('assistant', submoduleKey);
      if (question) this.unifiedChat.input = question;
      if (send) {
        this.$nextTick(() => this.sendUnifiedMessage(question || undefined));
      }
    },
    handleUnifiedInputKeydown(event) {
      if (event.key === 'Enter' && !event.shiftKey) {
        event.preventDefault();
        this.sendUnifiedMessage();
      }
    },
    switchAssistantMode(mode) {
      this.assistantMode = mode;
      if (mode === 'knowledge' && !String(this.assistantKnowledgeInput || '').trim()) {
        this.assistantKnowledgeInput = this.buildKnowledgeQuestion(this.selectedAnomaly ? 'anomaly' : 'analysis');
      }
    },
    // ── Unified chat interface ────────────────────────────────────────────────
    unifiedQuickStarters() {
      if (this.assistantSubmodule === 'diagnosis') {
        const starters = [];
        if (this.selectedAnomaly) {
          starters.push({
            key: 'diagnose_current',
            label: `诊断 ${this.selectedAnomaly.anomaly_name}`,
            question: `请诊断 ${this.selectedAnomaly.building_name} 在 ${this.formatCompactDateTime(this.selectedAnomaly.timestamp)} 出现的${this.selectedAnomaly.anomaly_name}。`,
          });
        }
        starters.push(
          { key: 'spike', label: '排查能耗突增', question: '请结合当前范围，给出能耗突增的可能原因、排查步骤和立即动作。' },
          { key: 'night', label: '排查夜间高负荷', question: '夜间基线负荷偏高时，应优先排查哪些设备和控制策略？' },
          { key: 'offline', label: '排查设备离线', question: '如果出现设备离线或低负荷异常，应如何判断是传感器问题还是设备问题？' },
        );
        return starters.slice(0, 4);
      }
      if (this.assistantSubmodule === 'saving') {
        return [
          { key: 'peak', label: '优化峰时负荷', question: '请针对当前建筑峰时负荷给出节能优化建议，并说明优先动作。' },
          { key: 'night', label: '降低夜间基线', question: '围绕夜间基线负荷偏高，给出可落地的节能动作和预估收益。' },
          { key: 'schedule', label: '优化时段策略', question: '请结合当前时间范围，给出工作时段与非工作时段的节能策略。' },
          { key: 'ops', label: '运维协同建议', question: '请把当前节能机会整理成运维可执行的优先动作清单。' },
        ];
      }
      if (this.assistantSubmodule === 'interpretation') {
        return [
          { key: 'trend', label: '解读趋势变化', question: '请解读当前时段的能耗趋势变化，说明主要发现和可能原因。' },
          { key: 'structure', label: '解释结构特征', question: '请解释当前时段的昼夜与周内结构特征，并指出关注点。' },
          { key: 'compare', label: '说明同类偏差', question: '请解释当前建筑与同类样本的差距，并给出管理建议。' },
          { key: 'brief', label: '生成汇报结论', question: '请把当前分析结果整理成适合汇报的主要发现、原因和运维建议。' },
        ];
      }
      return [
        { key: 'knowledge_standard', label: '查运维规范', question: '建筑能耗运维中，出现高负荷或突增时通常应优先排查哪些系统？' },
        { key: 'knowledge_hvac', label: '查空调常识', question: '空调系统供回水温差异常通常意味着什么？运维上如何判断？' },
        { key: 'knowledge_light', label: '查照明策略', question: '照明系统在教学楼或办公楼中，常见的节能控制策略有哪些？' },
        { key: 'knowledge_process', label: '查处置流程', question: '建筑能源异常从发现到复盘，一般应包括哪些标准化处理步骤？' },
      ];
    },
    routeUnifiedMessage(question) {
      const diagKw = ['诊断', '故障', '排查', '原因', '异常', '为什么', '突增', '高负荷', '离线', '排查步骤', '不正常'];
      const analysisKw = ['分析', '解读', '结论', '节能建议', '节能优化', '优化建议', '运维建议', '主要发现', '能耗报告'];
      if (diagKw.some(k => question.includes(k))) return 'diagnosis';
      if (analysisKw.some(k => question.includes(k))) return 'analysis';
      return 'knowledge';
    },
    async sendUnifiedMessage(questionOverride) {
      const question = String(questionOverride || this.unifiedChat.input || '').trim();
      if (!question) { this.$message.warning('请先输入问题'); return; }
      if (this.unifiedChat.loading) return;
      this.unifiedChat.input = '';
      this.unifiedChat.messages.push({ id: `${Date.now()}-user`, role: 'user', content: question });
      if (this.assistantSubmodule === 'diagnosis') {
        await this.sendStreamingDiagnosis({ question });
      } else if (this.assistantSubmodule === 'saving') {
        await this.sendUnifiedAnalysisMessage(question, 'saving');
      } else if (this.assistantSubmodule === 'interpretation') {
        await this.sendUnifiedAnalysisMessage(question, 'interpretation');
      } else {
        await this.sendUnifiedKnowledgeMessage(question);
      }
      await this.$nextTick();
      this.scrollAssistantToBottom();
    },
    async sendStreamingDiagnosis({ question } = {}) {
      this.unifiedChat.loading = true;
      this.unifiedChat.streaming = true;
      this.unifiedChat.streamBuffer = '';
      const msgId = `${Date.now()}-assistant`;
      const baseData = {
        conclusion: '正在检索数据和知识库...',
        causes: [], steps: [], recommended_actions: [], prevention: [], evidence: [],
        anomaly_name: this.selectedAnomaly?.anomaly_name || '能源异常诊断',
      };
      this.unifiedChat.messages.push({ id: msgId, role: 'assistant', type: 'diagnosis', pending: true, data: baseData });
      await this.$nextTick();
      this.scrollAssistantToBottom();

      const payload = {
        message: question || '请基于当前异常事件给出结构化诊断。',
        provider: this.aiProvider,
        building_id: this.selectedAnomaly?.building_id || this.filters.buildingId || null,
        anomaly_id: this.selectedAnomaly?.anomaly_id || null,
        timestamp: this.selectedAnomaly?.timestamp || null,
        anomaly_type: this.selectedAnomaly?.anomaly_type || null,
        start_time: this.isCompleteRange(this.filters.range) ? this.filters.range[0] : null,
        end_time: this.isCompleteRange(this.filters.range) ? this.filters.range[1] : null,
      };

      try {
        const response = await fetch(`${API_BASE}/api/ai/stream`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buf = '';
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buf += decoder.decode(value, { stream: true });
          const events = buf.split('\n\n');
          buf = events.pop();
          for (const block of events) {
            const evName = block.match(/^event: (.+)/m)?.[1];
            const dataStr = block.match(/^data: (.+)/m)?.[1];
            if (!dataStr) continue;
            let evData;
            try { evData = JSON.parse(dataStr); } catch { continue; }
            const idx = this.unifiedChat.messages.findIndex(m => m.id === msgId);
            if (idx < 0) break;
            if (evName === 'template') {
              evData.causes = evData.causes || evData.possible_causes || [];
              evData.steps = evData.steps || [];
              evData.prevention = evData.prevention || [];
              evData.recommended_actions = evData.recommended_actions || [];
              evData.evidence = evData.evidence || [];
              this.unifiedChat.messages.splice(idx, 1, { id: msgId, role: 'assistant', type: 'diagnosis', pending: true, data: evData });
              this.diagnosis = evData;
            } else if (evName === 'token') {
              this.unifiedChat.streamBuffer += evData.text;
              const cur = this.unifiedChat.messages[idx];
              this.unifiedChat.messages.splice(idx, 1, { ...cur, data: { ...cur.data, conclusion: this.unifiedChat.streamBuffer } });
            } else if (evName === 'done') {
              const cur = this.unifiedChat.messages[idx];
              const finalConclusion = this.unifiedChat.streamBuffer || cur.data.conclusion;
              const finalData = { ...cur.data, ...evData, conclusion: finalConclusion };
              this.unifiedChat.messages.splice(idx, 1, { ...cur, pending: false, data: finalData });
              this.diagnosis = finalData;
            } else if (evName === 'fallback' || evName === 'error') {
              const cur = this.unifiedChat.messages[idx];
              this.unifiedChat.messages.splice(idx, 1, { ...cur, pending: false });
            }
            await this.$nextTick();
            this.scrollAssistantToBottom();
          }
        }
      } catch (err) {
        const idx = this.unifiedChat.messages.findIndex(m => m.id === msgId);
        if (idx >= 0) {
          const cur = this.unifiedChat.messages[idx];
          this.unifiedChat.messages.splice(idx, 1, { ...cur, pending: false, data: { ...cur.data, conclusion: '诊断请求失败，请稍后重试。' } });
        }
        this.$message.error('诊断失败：' + (err.message || '未知错误'));
      } finally {
        this.unifiedChat.loading = false;
        this.unifiedChat.streaming = false;
      }
    },
    async sendUnifiedKnowledgeMessage(question) {
      this.unifiedChat.loading = true;
      const msgId = `${Date.now()}-assistant`;
      this.unifiedChat.messages.push({
        id: msgId,
        role: 'assistant',
        type: 'knowledge',
        pending: true,
        content: '正在检索知识库...',
        references: [],
      });
      await this.$nextTick();
      this.scrollAssistantToBottom();
      try {
        const response = await fetch(`${API_BASE}/api/ragflow/chat/stream`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ question, session_id: this.unifiedChat.sessionId || null }),
        });
        if (!response.ok) throw new Error(response.status + ' ' + response.statusText);
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buf = '', accumulatedRaw = '';
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buf += decoder.decode(value, { stream: true });
          const parts = buf.split('\n\n');
          buf = parts.pop();
          for (const part of parts) {
            const em = part.match(/event: (\w+)/);
            const dm = part.match(/data: (.+)/);
            if (!em || !dm) continue;
            let ed; try { ed = JSON.parse(dm[1]); } catch(e) { continue; }
            if (em[1] === 'token') {
              accumulatedRaw += ed.text || '';
              const displayContent = this.formatKnowledgeAnswerText(accumulatedRaw) || '正在检索知识库...';
              const idx = this.unifiedChat.messages.findIndex(m => m.id === msgId);
              if (idx >= 0) {
                const c = this.unifiedChat.messages[idx];
                this.unifiedChat.messages.splice(idx, 1, {
                  ...c,
                  pending: false,
                  content: displayContent,
                });
                this.scrollAssistantToBottom();
              }
            } else if (em[1] === 'done') {
              const idx = this.unifiedChat.messages.findIndex(m => m.id === msgId);
              const finalContent = this.formatKnowledgeAnswerText(
                ed.answer !== undefined && ed.answer !== null && String(ed.answer) !== '' ? ed.answer : accumulatedRaw
              ) || 'RAGFlow 已返回，但当前没有可展示的文本结果。';
              if (idx >= 0) {
                const c = this.unifiedChat.messages[idx];
                this.unifiedChat.messages.splice(idx, 1, {
                  ...c,
                  pending: false,
                  content: finalContent,
                  references: this.normalizeUnifiedKnowledgeReferences(ed.references),
                });
              }
              this.unifiedChat.sessionId = ed.session_id || this.unifiedChat.sessionId;
            } else if (em[1] === 'error') { throw new Error(ed.message || '知识库返回错误'); }
          }
        }
      } catch (err) {
        const idx = this.unifiedChat.messages.findIndex(m => m.id === msgId);
        if (idx >= 0) {
          this.unifiedChat.messages.splice(idx, 1, { id: msgId, role: 'assistant', type: 'knowledge', pending: false, content: '知识问答失败，请稍后重试。', references: [] });
        }
        this.$message.error('知识问答失败：' + (err.message || '未知错误'));
      } finally {
        this.unifiedChat.loading = false;
      }
    },
    async sendUnifiedAnalysisMessage(question, mode = 'interpretation') {
      this.unifiedChat.loading = true;
      const msgId = `${Date.now()}-assistant`;
      const messageType = mode === 'saving' ? 'saving' : 'analysis';
      this.unifiedChat.messages.push({ id: msgId, role: 'assistant', type: messageType, pending: true, data: null });
      await this.$nextTick();
      this.scrollAssistantToBottom();
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
          message: question || (mode === 'saving'
            ? `${this.currentAnalysisScopeText()}，请输出节能结论、优先动作和收益影响。`
            : `${this.currentAnalysisScopeText()}，请输出结构化分析结论。`),
        };
        const data = await this.fetchJson(`${API_BASE}/api/ai/analyze`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        this.analysisInsight = data.analysis || null;
        this.analysisFeedbackLabel = '';
        const idx = this.unifiedChat.messages.findIndex((item) => item.id === msgId);
        if (idx >= 0) {
          this.unifiedChat.messages.splice(idx, 1, {
            id: msgId,
            role: 'assistant',
            type: messageType,
            pending: false,
            data: this.analysisInsight,
          });
        }
        await this.loadAiStats();
        await this.loadAiEvaluate();
      } catch (err) {
        const idx = this.unifiedChat.messages.findIndex((item) => item.id === msgId);
        if (idx >= 0) {
          this.unifiedChat.messages.splice(idx, 1, {
            id: msgId,
            role: 'assistant',
            type: messageType,
            pending: false,
            data: {
              summary: mode === 'saving'
                ? `节能建议生成失败：${err.message || '请稍后重试。'}`
                : `分析结论生成失败：${err.message || '请稍后重试。'}`,
              findings: [],
              possible_causes: [],
              energy_saving_suggestions: [],
              operations_suggestions: [],
              provider: 'error',
              requested_provider: this.aiProvider,
              latency_ms: 0,
              fallback_used: false,
            },
          });
        }
        this.$message.error(`分析结论生成失败：${err.message || '未知错误'}`);
      } finally {
        this.unifiedChat.loading = false;
      }
    },
    resetUnifiedConversation(options = {}) {
      const { silent = false } = options;
      this.unifiedChat.messages = [];
      this.unifiedChat.input = '';
      this.unifiedChat.sessionId = '';
      this.unifiedChat.loading = false;
      this.unifiedChat.streaming = false;
      this.unifiedChat.streamBuffer = '';
      this.resetKnowledgeReferenceState();
      if (!silent) this.$message.success(`已切换到新的${this.assistantSubmoduleConfig().label}会话`);
    },
    scrollAssistantToBottom() {
      const el = this.$refs.assistantThreadScroll || this.$refs.assistantBody;
      if (el) el.scrollTop = el.scrollHeight;
    },
    // ── End unified chat ──────────────────────────────────────────────────────
    buildKnowledgeQuestion(kind = 'analysis') {
      if (kind === 'anomaly' && this.selectedAnomaly) {
        return `针对${this.selectedAnomaly.building_type || '当前'}建筑出现“${this.selectedAnomaly.anomaly_name}”，从运维常识看优先排查哪些设备系统？为什么这种现象会发生？`;
      }
      const scope = this.analysisInsights.scope_summary || {};
      return `结合${scope.building_name || '当前建筑'}在${this.currentAnalysisScopeText()}下的能耗表现，解释夜间基线、非工作时段负荷和节能优化上最该关注什么。`;
    },
    fillKnowledgeQuestion(kind = 'analysis') {
      this.assistantKnowledgeInput = this.buildKnowledgeQuestion(kind);
    },
    assistantTaskTitle() {
      return this.assistantPromptKind === 'anomaly' ? '异常诊断结果' : '当前分析结论';
    },
    assistantTaskHint() {
      if (this.assistantPromptKind === 'anomaly') {
        return '这里展示 A8 基于结构化异常事件、窗口数据、同类对照、天气条件和知识证据生成的诊断结果。';
      }
      return '这里展示 A8 基于当前分析范围、趋势、结构、同类对比和知识证据生成的分析结论。';
    },
    diagnosisDataEvidence() {
      return Array.isArray(this.diagnosis?.data_evidence) ? this.diagnosis.data_evidence : [];
    },
    diagnosisKnowledgeEvidence() {
      return Array.isArray(this.diagnosis?.evidence) ? this.diagnosis.evidence : [];
    },
    analysisBulletText(items, emptyText) {
      if (!Array.isArray(items) || !items.length) return emptyText;
      return items.slice(0, 3).map((item) => `- ${item.title}：${item.detail}`).join('\n');
    },
    buildAnalysisAssistantPrompt() {
      const scope = this.analysisInsights.scope_summary || {};
      const peer = this.analysisCompare.peer_group || {};
      const opportunities = (this.analysisInsights.saving_opportunities || []).slice(0, 3).map((item) => `- ${item.title}：${item.detail}`);
      const anomalyWindows = (this.analysisInsights.anomaly_windows || []).slice(0, 3).map((item) => `- ${this.formatCompactDateTime(item.timestamp)} ${item.anomaly_name}，偏差 ${this.formatNumber(item.deviation_pct, 1)}%，影响估算 ${this.formatInsightKwh(item.estimated_loss_kwh)}`);
      return [
        '请作为建筑能源与运维专家，结合下面的项目分析上下文，输出“结论-证据-动作”三段式中文回答。',
        '如果存在明显异常，请同时补充风险提示和优先检查顺序。',
        '',
        `分析对象：${scope.building_name || '未选择建筑'}`,
        `分析指标：${this.currentMetricLabel()}（${this.analysisSummary.unit || 'kWh'}）`,
        `筛选时间：${this.formatCompactDateTime(scope.selected_start_time)} ~ ${this.formatCompactDateTime(scope.selected_end_time)}`,
        `有效数据：${this.formatCompactDateTime(scope.data_start_time)} ~ ${this.formatCompactDateTime(scope.data_end_time)}，共 ${scope.point_count || 0} 个点位`,
        `总量：${this.formatNumber(this.analysisSummary.total_value, 1)} ${this.analysisSummary.unit || 'kWh'}`,
        `均值：${this.formatNumber(this.analysisSummary.avg_value, 1)} ${this.analysisSummary.unit || 'kWh'}`,
        `峰值：${this.formatNumber(this.analysisSummary.peak_value, 1)} ${this.analysisSummary.unit || 'kWh'}`,
        `波动率：${this.formatNumber(this.analysisSummary.volatility_pct, 1)}%`,
        `窗口变化：${this.formatNumber(this.analysisTrend.summary?.window_change_pct, 1)}%`,
        `温度相关：${this.formatNumber(this.analysisTrend.summary?.temperature_correlation, 2)}`,
        peer.peer_count ? `同类对比：样本 ${peer.peer_count} 栋，偏离同类均值 ${this.formatNumber(peer.gap_pct, 1)}%，同类百分位 ${this.formatNumber(peer.peer_percentile, 1)}` : '同类对比：当前为全集或缺少单体样本，请谨慎使用同类结论',
        '',
        '趋势结论：',
        this.analysisBulletText(this.analysisInsights.trend_findings, '- 当前没有额外趋势结论'),
        '',
        '天气联动：',
        this.analysisBulletText(this.analysisInsights.weather_findings, '- 当前没有温度联动结论'),
        '',
        '节能机会：',
        opportunities.length ? opportunities.join('\n') : '- 当前时间范围暂无明显节能机会点',
        '',
        '异常窗口：',
        anomalyWindows.length ? anomalyWindows.join('\n') : '- 当前筛选范围内没有识别到异常窗口',
      ].join('\n');
    },
    buildAnomalyAssistantPrompt(row = this.selectedAnomaly) {
      if (!row) {
        return this.buildAnalysisAssistantPrompt();
      }
      return [
        '请作为建筑运维诊断助手，基于下面的异常上下文输出：原因判断、排查步骤、立即动作、预防建议。',
        '请优先给出现场可执行的检查顺序，并尽量结合建筑场景与设备运行特征。',
        '',
        `建筑：${row.building_name || '-'}`,
        `异常类型：${row.anomaly_name}`,
        `异常时间：${this.formatCompactDateTime(row.timestamp)}`,
        `偏差比例：${this.formatNumber(row.deviation_pct, 1)}%`,
        `当前状态：${row.status || 'new'}`,
        `当前分析范围：${this.currentAnalysisScopeText()}`,
        `补充背景：${this.analysisBulletText(this.analysisInsights.trend_findings, '- 当前没有额外背景分析')}`,
      ].join('\n');
    },
    prepareAssistantPrompt(kind = 'analysis', row = null) {
      this.assistantPromptKind = kind;
      const prompt = kind === 'anomaly' ? this.buildAnomalyAssistantPrompt(row) : this.buildAnalysisAssistantPrompt();
      this.assistantPromptDraft = prompt;
      if (!String(this.assistantKnowledgeInput || '').trim()) {
        this.assistantKnowledgeInput = this.buildKnowledgeQuestion(kind);
      }
      return prompt;
    },
    newAssistantMessage(role, content = '') {
      return {
        id: `${role}-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
        role,
        content,
        pending: false,
        references: [],
        knowledgeSource: role === 'assistant' ? 'RAGFlow' : '',
      };
    },
    normalizeAssistantReferences(reference = {}) {
      const chunks = Array.isArray(reference?.chunks) ? reference.chunks : [];
      const docAggs = Array.isArray(reference?.doc_aggs) ? reference.doc_aggs : [];
      return chunks.slice(0, 6).map((item, index) => {
        const docAgg = docAggs.find((doc) => doc.doc_id === item.document_id);
        return {
          id: item.id || `${item.document_id || 'doc'}-${index}`,
          title: item.document_name || docAgg?.doc_name || '知识片段',
          excerpt: String(item.content || '').replace(/\s+/g, ' ').trim(),
          similarity: Number(item.similarity || 0),
          sourceType: item.source_type || 'ragflow',
        };
      });
    },
    resetAssistantConversation(options = {}) {
      const { keepDraft = true, silent = false } = options;
      this.assistantChat.sessionId = '';
      this.assistantChat.error = '';
      this.assistantChat.lastLatencyMs = 0;
      this.assistantChat.messages = [];
      if (!keepDraft) this.assistantPromptDraft = '';
      if (!silent) this.$message.success('已创建新的知识问答会话');
    },
    async sendAssistantPrompt(options = {}) {
      const { silent = false } = options;
      const question = String(this.assistantKnowledgeInput || '').trim();
      if (!question) {
        if (!silent) this.$message.warning('请先输入知识问答问题');
        return false;
      }
      if (this.assistantChat.loading) return false;

      this.assistantChat.loading = true;
      this.assistantChat.error = '';
      const userMessage = this.newAssistantMessage('user', question);
      const assistantMessage = this.newAssistantMessage('assistant', '');
      assistantMessage.pending = true;
      assistantMessage.content = 'RAGFlow 正在检索并生成，请稍候...';
      this.assistantChat.messages.push(userMessage, assistantMessage);

      const startedAt = performance.now();
      try {
        const data = await this.fetchJson(`${API_BASE}/api/ragflow/chat`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            question,
            session_id: this.assistantChat.sessionId || null,
          }),
        });
        assistantMessage.pending = false;
        assistantMessage.content = data.answer || 'RAGFlow 已返回，但当前没有可展示的文本结果。';
        assistantMessage.references = Array.isArray(data.references) ? data.references : [];
        assistantMessage.knowledgeSource = 'RAGFlow';
        this.assistantChat.sessionId = data.session_id || this.assistantChat.sessionId;
        this.assistantChat.lastLatencyMs = Number(data.latency_ms || Math.round(performance.now() - startedAt));
        return true;
      } catch (err) {
        console.error(err);
        assistantMessage.pending = false;
        assistantMessage.content = 'RAGFlow 当前未能完成回答，请稍后重试。若持续失败，请先检查本地知识库和会话配置。';
        assistantMessage.references = [];
        assistantMessage.knowledgeSource = 'error';
        this.assistantChat.error = `发送失败：${err.message || '未知错误'}`;
        if (!silent) this.$message.error(this.assistantChat.error);
        return false;
      } finally {
        this.assistantChat.loading = false;
      }
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
          building_name: this.selectedBuildingMeta()?.name || '未选择建筑',
          building_type: this.selectedBuildingMeta()?.type || '',
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
      if (this.assistantPromptKind === 'analysis') this.assistantPromptDraft = '';
      this.ensureRangeWithinScope({ applyDefaultIfEmpty: true, silent: true });
      this.refreshCurrentPage();
    },
    onAnalysisMetricChange() {
      this.analysisInsight = null;
      this.analysisFeedbackLabel = '';
      if (this.assistantPromptKind === 'analysis') this.assistantPromptDraft = '';
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
          if (this.assistantPromptKind === 'analysis') this.assistantPromptDraft = '';
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
          name: x.display_name || x.building_name,
          type: x.display_category || x.building_type,
          peerCategory: x.peer_category || '',
          startTime: x.start_time,
          endTime: x.end_time,
          recordCount: x.record_count,
        }));
        if (!this.buildings.some((item) => item.id === this.filters.buildingId)) {
          this.filters.buildingId = this.buildings[0]?.id || '';
        }
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
          building_name: meta?.name || '未选择建筑',
          building_type: meta?.type || '',
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
        this.health.ragflowConfigured = !!data.ragflow?.configured;
        this.health.ragflowEnabled = !!data.ragflow?.enabled;
        this.health.ragflowDatasetCount = data.ragflow?.scene_dataset_count ?? data.ragflow?.dataset_count ?? 0;
        this.health.ragflowStandardDatasetCount = data.ragflow?.standard_dataset_count ?? 0;
        this.health.ragflowStandardConfigured = !!data.ragflow?.standard_configured;
        this.health.ragflowChatReady = !!(data.ragflow?.chat_ready ?? data.ragflow?.assistant_ready);
        this.health.ragflowChatId = data.ragflow?.chat_id || '';
      } catch (err) {
        console.error(err);
        this.health.status = 'unknown';
        this.health.regression = 'unknown';
        this.health.aiConfigured = false;
        this.health.aiModelReady = false;
        this.health.ragflowConfigured = false;
        this.health.ragflowEnabled = false;
        this.health.ragflowDatasetCount = 0;
        this.health.ragflowStandardDatasetCount = 0;
        this.health.ragflowStandardConfigured = false;
        this.health.ragflowChatReady = false;
        this.health.ragflowChatId = '';
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
      await this.loadSystemHealth();
    },
    async switchPage(key) {
      this.activePage = key;
      if (key === 'assistant') {
        this.assistantSubmodule = this.activeSubmoduleMap.assistant || this.assistantSubmodule;
      }
      await this.refreshCurrentPage();
      await this.$nextTick();
      this.syncVisibleCharts();
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
    async diagnoseFromAnomaly(row) {
      this.selectedAnomaly = row;
      this.prepareAssistantPrompt('anomaly', row);
      this.assistantMode = 'diagnosis';
      this.diagnosis = null;
      this.activeSubmoduleMap = { ...this.activeSubmoduleMap, assistant: 'diagnosis' };
      this.assistantSubmodule = 'diagnosis';
      this.resetUnifiedConversation({ silent: true });
      await this.switchPage('assistant');
      await nextTick();
      const userQuestion = `请诊断 ${row.building_name} 在 ${this.formatCompactDateTime(row.timestamp)} 出现的${row.anomaly_name}（偏差 ${this.formatNumber(row.deviation_pct, 1)}%）。`;
      this.unifiedChat.messages.push({ id: `${Date.now()}-user`, role: 'user', content: userQuestion });
      await this.sendStreamingDiagnosis({ question: userQuestion });
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
      if (!this.selectedAnomaly && !this.chatInput.trim()) {
        this.$message.warning('请先输入问题或从异常列表触发诊断');
        return;
      }
      this.loading.ai = true;
      try {
        const payload = {
          message: this.selectedAnomaly ? '请基于当前异常事件给出结构化诊断。' : this.chatInput,
          provider: this.aiProvider,
          building_id: this.selectedAnomaly?.building_id || this.filters.buildingId || null,
          anomaly_id: this.selectedAnomaly?.anomaly_id || null,
          timestamp: this.selectedAnomaly?.timestamp || null,
          anomaly_type: this.selectedAnomaly?.anomaly_type || null,
          start_time: this.isCompleteRange(this.filters.range) ? this.filters.range[0] : null,
          end_time: this.isCompleteRange(this.filters.range) ? this.filters.range[1] : null,
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
    launchAnalysisAssistant() {
      if (this.activeSubmoduleMap.analysis === 'opportunity') {
        this.navigateToAssistantSubmodule('saving', {
          question: `请基于${this.currentAnalysisScopeText()}输出节能结论、优先动作和收益影响。`,
          send: true,
        });
        return;
      }
      this.runAnalysisAssistant();
    },
    sendSavingOpportunityToAssistant(item) {
      const title = item?.title || '当前节能机会';
      const detail = item?.detail || '';
      this.navigateToAssistantSubmodule('saving', {
        question: `请围绕“${title}”给出节能建议、优先动作和收益影响。补充信息：${detail}`,
        send: true,
      });
    },
    openKnowledgeAssistant(question = '') {
      this.navigateToAssistantSubmodule('knowledge', { question });
    },
    async runAnalysisAssistant() {
      this.selectedAnomaly = null;
      this.prepareAssistantPrompt('analysis');
      this.assistantMode = 'diagnosis';
      this.analysisInsight = null;
      this.activeSubmoduleMap = { ...this.activeSubmoduleMap, assistant: 'interpretation' };
      this.assistantSubmodule = 'interpretation';
      this.resetUnifiedConversation({ silent: true });
      await this.switchPage('assistant');
      await nextTick();
      const question = `请分析${this.currentAnalysisScopeText()}的能耗数据，生成可用于汇报的分析结论。`;
      this.unifiedChat.messages.push({ id: `${Date.now()}-user`, role: 'user', content: question });
      const pendingId = `${Date.now()}-assistant`;
      this.unifiedChat.messages.push({ id: pendingId, role: 'assistant', type: 'analysis', pending: true, data: null });
      await this.submitAnalysisReport();
      const idx = this.unifiedChat.messages.findIndex(m => m.id === pendingId);
      if (idx >= 0 && this.analysisInsight) {
        this.unifiedChat.messages.splice(idx, 1, { id: pendingId, role: 'assistant', type: 'analysis', pending: false, data: this.analysisInsight });
      } else if (idx >= 0) {
        this.unifiedChat.messages.splice(idx, 1);
      }
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
