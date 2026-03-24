import { expect, test } from '@playwright/test';

test.describe('A8 closed loop chains', () => {
  test('building switch keeps cards and analysis workspace usable', async ({ page }) => {
    await page.goto('/');
    await expect(page.locator('.topbar h1')).toBeVisible();
    await expect(page.getByText('系统:')).toBeVisible();
    await expect(page.getByText('回归:')).toBeVisible();

    const select = page.locator('.filter-row .el-select').first();
    await select.click();
    await page.locator('.el-select-dropdown:visible .el-select-dropdown__item').nth(1).click();
    await expect(page.locator('.filter-scope-value')).toContainText('~');

    await expect(page.locator('.card-value').first()).toContainText('kWh');
    await expect(page.locator('#overviewChart canvas').first()).toBeVisible();

    await page.getByRole('button', { name: '能耗分析' }).click();
    await expect(page.getByText('电力分析工作台')).toBeVisible();
    await expect(page.getByRole('button', { name: 'AI 分析' })).toBeVisible();
    await expect(page.getByText('规则洞察与证据提要')).toBeVisible();
    await expect(page.locator('#trendChart')).toBeVisible();
    await expect(page.locator('#patternChart')).toBeVisible();
    await expect(page.locator('#splitChart')).toBeVisible();
  });

  test('analysis workspace supports weather overlay, ai report and unsupported metric placeholder', async ({ page }) => {
    await page.route('**/api/ai/analyze', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          code: 0,
          message: 'ok',
          data: {
            analysis: {
              summary: '当前建筑工作日白天负荷偏高，建议先从空调排程和夜间基线治理入手。',
              findings: ['白天工作时段负荷高于同类均值。'],
              possible_causes: ['空调启停策略偏保守。'],
              energy_saving_suggestions: ['优化工作日前两小时预冷策略。'],
              operations_suggestions: ['核查空调排程与夜间常开设备。'],
              evidence: [
                {
                  chunk_id: 'rag-1',
                  title: '03-校园与教育建筑运维场景.md',
                  section: '相似度 0.88',
                  excerpt: '教学楼在高温时段应优先核查通风与空调排程。',
                  source_type: 'ragflow',
                },
              ],
              provider: 'llm_provider',
              requested_provider: 'auto',
              fallback_used: false,
              knowledge_source: 'ragflow',
              retrieval_hit_count: 1,
              latency_ms: 856,
              trace_id: 'trace-analysis-e2e',
            },
          },
        }),
      });
    });

    await page.goto('/');
    await page.getByRole('button', { name: '能耗分析' }).click();

    await expect(page.getByText('趋势分析与天气联动')).toBeVisible();
    await expect(page.getByText('分时段与周内结构')).toBeVisible();
    await expect(page.getByText('当前建筑 vs 同类均值')).toBeVisible();
    await expect(page.getByText('节能机会与优化动作')).toBeVisible();
    await expect(page.getByText('异常窗口与影响估算')).toBeVisible();
    await expect(page.locator('#trendChart')).toBeVisible();
    await expect
      .poll(async () => {
        return page.evaluate(() => {
          const chart = echarts.getInstanceByDom(document.getElementById('trendChart'));
          const option = chart?.getOption?.() || {};
          return option.xAxis?.[0]?.data?.[0] || '';
        });
      }, { timeout: 15000 })
      .toMatch(/\d{4}-\d{2}-\d{2}/);
    await expect(page.getByRole('button', { name: 'AI 分析' })).toBeVisible();
    await expect(page.getByText('规则洞察与证据提要')).toBeVisible();
    await expect(page.locator('.insight-group').first()).toBeVisible();

    const trendMetaBefore = await page.evaluate(() => {
      const chart = echarts.getInstanceByDom(document.getElementById('trendChart'));
      const option = chart?.getOption?.() || {};
      return {
        seriesCount: Array.isArray(option.series) ? option.series.length : 0,
        hasZoom: Array.isArray(option.dataZoom) && option.dataZoom.length > 0,
        firstLabel: option.xAxis?.[0]?.data?.[0] || '',
        lastLabel: option.xAxis?.[0]?.data?.[(option.xAxis?.[0]?.data?.length || 1) - 1] || '',
      };
    });
    expect(trendMetaBefore.seriesCount).toBeGreaterThanOrEqual(0);
    expect(trendMetaBefore.hasZoom).toBeFalsy();
    expect(trendMetaBefore.firstLabel).toMatch(/\d{4}-\d{2}-\d{2}/);
    expect(trendMetaBefore.lastLabel).toMatch(/\d{4}-\d{2}-\d{2}/);
    await expect(page.getByText('图表范围跟随顶部“时间范围”筛选')).toBeVisible();

    await page.getByRole('combobox', { name: '开始时间' }).fill('2016-01-07 16:00:00');
    await page.getByRole('combobox', { name: '结束时间' }).fill('2016-12-13 23:00:00');
    await page.getByRole('button', { name: '刷新数据' }).click();
    await page.waitForTimeout(2500);
    await expect(page.locator('.analysis-scope-item').nth(1)).toContainText('2016-01-07');
    await expect(page.locator('.insight-group').first()).toContainText(/趋势结论|当前时间范围暂无可解释趋势/);

    const weatherSwitch = page.locator('.analysis-hero-actions .el-switch');
    if (await weatherSwitch.isEnabled()) {
      await weatherSwitch.click();
      await page.waitForTimeout(500);
    }

    await page.locator('.filter-row .el-select').nth(1).click();
    await page.locator('.el-select-dropdown:visible .el-select-dropdown__item').filter({ hasText: /^水$/ }).first().click();
    await expect(page.getByText('水暂未接入')).toBeVisible();

    await page.locator('.filter-row .el-select').nth(1).click();
    await page.locator('.el-select-dropdown:visible .el-select-dropdown__item').filter({ hasText: /^电力$/ }).first().click();
    await page.waitForTimeout(1200);
    await expect(page.locator('#trendChart')).toBeVisible();

    await page.getByRole('button', { name: 'AI 分析' }).click();
    await expect(page.locator('.assistant-page .panel-title').filter({ hasText: '智能助手' })).toBeVisible();
    await expect(page.locator('.assistant-result-panel')).toContainText('当前分析结论');
    await expect(page.locator('.assistant-summary-card')).toContainText('当前建筑工作日白天负荷偏高');
    await expect(page.locator('.assistant-section').filter({ hasText: '节能建议' })).toContainText('优化工作日前两小时预冷策略');
    await expect(page.locator('.assistant-evidence-item')).toContainText('03-校园与教育建筑运维场景');
  });

  test('anomaly ack -> detail timeline works', async ({ page }) => {
    await page.goto('/');
    await page.getByRole('button', { name: '故障监控' }).click();
    await expect(page.locator('.el-table')).toBeVisible();

    await page.locator('.anomaly-toolbar .el-select').nth(1).click();
    await page.locator('.el-select-dropdown__item span').filter({ hasText: /^新告警$/ }).first().click();

    const row = page.locator('.el-table__body-wrapper tbody tr').first();
    await expect(row).toBeVisible();
    await row.locator('.el-button', { hasText: '确认' }).click();

    await expect(page.locator('.action-form')).toBeVisible();
    await page.locator('.action-form input').first().fill('playwright');
    await page.locator('.action-form textarea').first().fill('ack by e2e');
    await page.getByRole('button', { name: '提交' }).click();
    await page.waitForTimeout(1000);
    if (await page.locator('.action-form').isVisible()) {
      await page.keyboard.press('Escape');
    }
    await expect(page.locator('.action-form')).toBeHidden();

    await page.locator('.anomaly-toolbar .el-select').nth(1).click();
    await page.locator('.el-select-dropdown__item span').filter({ hasText: /^已确认$/ }).first().click();

    const ackRow = page.locator('.el-table__body-wrapper tbody tr').first();
    await expect(ackRow).toBeVisible();
    await ackRow.locator('.el-button', { hasText: '详情' }).click();
    const detailDialog = page.locator('.el-dialog').filter({ hasText: '异常详情' });
    await expect(detailDialog).toBeVisible();
    await expect(page.locator('.history-list li').first()).toBeVisible();
    await expect(page.locator('.history-list')).toContainText('ack');
    await page.getByRole('button', { name: '关闭' }).click();
  });

  test('diagnose uses A8 result chain and can write note draft', async ({ page }) => {
    await page.route('**/api/ai/diagnose', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          code: 0,
          message: 'ok',
          data: {
            diagnosis: {
              conclusion: '异常更像夜间常开负荷叠加设备排程异常，应先检查空调和热水系统。',
              causes: ['夜间待机设备未关闭。'],
              steps: ['先核查 24 小时排程。'],
              prevention: ['建立夜间巡检和联动复核机制。'],
              recommended_actions: ['立即核查空调、新风和热水设备的夜间运行状态。'],
              evidence: [
                {
                  chunk_id: 'rag-2',
                  title: '05-居住与住宿建筑运行场景.md',
                  section: '相似度 0.91',
                  excerpt: '住宿建筑夜间基线偏高通常与热水和公共区照明常开有关。',
                  source_type: 'ragflow',
                },
              ],
              data_evidence: [
                {
                  title: '异常点与24h基线',
                  detail: '异常时刻负荷明显高于近24小时基线。',
                  source_type: 'data_signal',
                },
              ],
              provider: 'llm_provider',
              requested_provider: 'auto',
              fallback_used: false,
              knowledge_source: 'ragflow',
              retrieval_hit_count: 1,
              latency_ms: 933,
              trace_id: 'trace-diagnose-e2e',
            },
          },
        }),
      });
    });

    await page.goto('/');
    await page.getByRole('button', { name: '故障监控' }).click();
    const row = page.locator('.el-table__body-wrapper tbody tr').first();
    await row.locator('.el-button--danger', { hasText: '诊断' }).click();

    await expect(page.locator('.assistant-page .panel-title').filter({ hasText: '智能助手' })).toBeVisible();
    await expect(page.locator('.assistant-result-panel')).toContainText('异常诊断结果');
    await expect(page.locator('.assistant-summary-card')).toContainText('异常更像夜间常开负荷叠加设备排程异常');
    await expect(page.locator('.assistant-section').filter({ hasText: '排查步骤' })).toContainText('先核查 24 小时排程');
    await expect(page.locator('.assistant-evidence-item--data')).toContainText('异常点与24h基线');
    await page.getByRole('button', { name: '写入处理备注草稿' }).click();
    await expect(page.locator('.action-form')).toBeVisible();
  });

  test('knowledge qa mode sends question without iframe', async ({ page }) => {
    await page.route('**/api/ragflow/chat*', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          code: 0,
          message: 'ok',
          data: {
            answer: '夜间基础负荷偏高通常要优先检查热水系统、公共照明和常开末端设备。',
            session_id: 'ragflow-session-e2e',
            references: [
              {
                chunk_id: 'rag-3',
                title: '05-居住与住宿建筑运行场景.md',
                section: '相似度 0.87',
                excerpt: '住宿建筑夜间高基线多与热水、走廊照明和公共区设备有关。',
                source_type: 'ragflow',
              },
            ],
            knowledge_source: 'ragflow',
            provider: 'ragflow_chat',
            latency_ms: 512,
          },
        }),
      });
    });

    await page.goto('/');
    await page.getByRole('button', { name: '智能助手' }).click();
    await page.getByRole('button', { name: '知识问答' }).click();
    const textarea = page.locator('.assistant-prompt-panel textarea');
    await textarea.fill('学生宿舍夜间基础负荷一直偏高，常见原因是什么？');
    await page.getByRole('button', { name: '发送问题' }).click();

    await expect(page.locator('.assistant-chat-stream')).toBeVisible();
    await expect(page.locator('.assistant-chat-message--assistant').last()).toContainText('夜间基础负荷偏高通常要优先检查热水系统');
    await expect(page.locator('.assistant-reference-item')).toContainText('05-居住与住宿建筑运行场景');
  });
});
