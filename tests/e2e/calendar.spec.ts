import { expect, test } from '@playwright/test';

async function openAnomalyEvents(page) {
  await page.getByRole('button', { name: '运维处置' }).click();
  await page.getByRole('button', { name: '事件列表' }).click();
  await expect(page.locator('.anomaly-table')).toBeVisible();
}

async function chooseAnomalyStatus(page, label: string) {
  await page.locator('[data-testid="anomaly-status-filter"]').click();
  await page.locator('.el-select-dropdown:visible .el-select-dropdown__item').filter({ hasText: new RegExp(`^${label}$`) }).first().click();
}

test.describe('A8 closed loop chains', () => {
  test('building switch keeps cards and analysis workspace usable', async ({ page }) => {
    await page.goto('/');
    await expect(page.locator('.topbar h1')).toBeVisible();
    await expect(page.locator('.status-inline-label').filter({ hasText: '系统' })).toBeVisible();
    await expect(page.locator('.status-inline-label').filter({ hasText: '回归' })).toBeVisible();

    const select = page.locator('.filter-row .el-select').first();
    await select.click();
    await page.locator('.el-select-dropdown:visible .el-select-dropdown__item').nth(1).click();
    await expect(page.locator('.filter-scope-value')).toContainText('~');

    await expect(page.locator('.card-value').first()).toContainText('kWh');
    await expect(page.locator('#overviewChart')).toBeVisible();

    await page.getByRole('button', { name: '能耗分析' }).click();
    await expect(page.getByText('趋势分析与天气联动')).toBeVisible();
    await expect(page.locator('#trendChart')).toBeVisible();

    await page.getByRole('button', { name: '结构分析与对标' }).click();
    await expect(page.getByText('分时段与周内结构')).toBeVisible();
    await expect(page.locator('#patternChart')).toBeVisible();
    await expect(page.locator('#splitChart')).toBeVisible();

    await page.getByRole('button', { name: '节能机会' }).click();
    await expect(page.getByText('节能机会与优化动作')).toBeVisible();
    await expect(page.getByText('异常窗口与影响估算')).toBeVisible();
  });

  test('analysis workspace supports weather overlay and ai report', async ({ page }) => {
    await page.goto('/');
    await page.getByRole('button', { name: '能耗分析' }).click();

    await expect(page.getByText('趋势分析与天气联动')).toBeVisible();
    await expect(page.locator('#trendChart')).toBeVisible();
    await expect(
      page.getByText(/图表范围跟随顶部.*时间范围.*筛选/)
    ).toBeVisible();

    await expect
      .poll(async () => {
        return page.evaluate(() => {
          const chart = echarts.getInstanceByDom(document.getElementById('trendChart'));
          const option = chart?.getOption?.() || {};
          return option.xAxis?.[0]?.data?.[0] || '';
        });
      }, { timeout: 15000 })
      .toMatch(/\d{4}-\d{2}-\d{2}/);

    const weatherSwitch = page.locator('.analysis-hero-actions .el-switch');
    if (await weatherSwitch.isEnabled()) {
      await weatherSwitch.click();
      await page.waitForTimeout(500);
    }

    await page.getByRole('button', { name: '使用有效时间' }).click();
    await page.waitForTimeout(2500);
    await expect(page.locator('.analysis-scope-item').nth(1)).not.toContainText('- ~ -');
    await expect(page.locator('.insight-group').first()).toContainText(/趋势结论|当前时间范围暂无可解释趋势/);

    await page.getByRole('button', { name: '结构分析与对标' }).click();
    await expect(page.getByText('分时段与周内结构')).toBeVisible();
    await expect(page.getByText('当前建筑 vs 同类均值')).toBeVisible();

    await page.getByRole('button', { name: '趋势分析' }).click();
    await page.getByRole('button', { name: '进入分析解读' }).click();
    await expect(page.locator('.assistant-page')).toBeVisible();
    await expect(page.getByText('正在生成分析结论')).toBeVisible();
    await expect(page.getByText('正在生成分析结论')).toBeHidden({ timeout: 30000 });
    await expect(page.locator('.assistant-summary-card')).toBeVisible();
    await expect(page.locator('.assistant-section').filter({ hasText: '主要发现' })).toBeVisible();
    await expect(page.locator('.assistant-section').filter({ hasText: '运维建议' })).toBeVisible();
  });

  test('anomaly ack -> detail timeline works', async ({ page }) => {
    await page.goto('/');
    await openAnomalyEvents(page);

    await chooseAnomalyStatus(page, '新告警');

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

    await chooseAnomalyStatus(page, '已确认');

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
    await page.goto('/');
    await openAnomalyEvents(page);
    const row = page.locator('.el-table__body-wrapper tbody tr').first();
    await row.locator('.el-button--danger', { hasText: '诊断' }).click();

    await expect(page.locator('.assistant-page')).toBeVisible();
    await expect(page.locator('.assistant-summary-card')).not.toContainText('正在检索数据和知识库');
    await expect(page.locator('.assistant-section').filter({ hasText: '排查步骤' })).toBeVisible();
    await page.getByRole('button', { name: '写入备注' }).click();
    await expect(page.locator('.action-form')).toBeVisible();
  });

  test('knowledge qa mode sends question without iframe', async ({ page }) => {
    await page.goto('/');
    await page.getByRole('button', { name: '智能助手' }).click();
    await page.getByRole('button', { name: '知识问答' }).click();
    await expect(page.getByText('知识问答工作台')).toBeVisible();

    const textarea = page.locator('textarea').last();
    await textarea.fill('学生宿舍夜间基础负荷一直偏高，常见原因是什么？');
    await page.getByRole('button', { name: '发送' }).click();

    await expect(page.locator('.unified-knowledge-answer')).toBeVisible();
    await expect(page.locator('.unified-knowledge-answer')).toContainText('夜间基础负荷偏高');
    await expect(page.getByRole('button', { name: '新会话' })).toBeVisible();
  });
});
