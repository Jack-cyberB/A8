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
    await expect(page.locator('#trendChart canvas').first()).toBeVisible();
    await expect(page.locator('#patternChart canvas').first()).toBeVisible();
    await expect(page.locator('#splitChart canvas').first()).toBeVisible();
  });

  test('analysis workspace supports weather overlay, ai report and unsupported metric placeholder', async ({ page }) => {
    await page.goto('/');
    await page.getByRole('button', { name: '能耗分析' }).click();

    await expect(page.getByText('趋势分析与天气联动')).toBeVisible();
    await expect(page.getByText('分时段与周内结构')).toBeVisible();
    await expect(page.getByText('当前建筑 vs 同类均值')).toBeVisible();
    await expect(page.getByText('节能机会与优化动作')).toBeVisible();
    await expect(page.getByText('异常窗口与影响估算')).toBeVisible();
    await expect(page.locator('#trendChart')).toBeVisible();
    await page.waitForFunction(() => {
      const chart = echarts.getInstanceByDom(document.getElementById('trendChart'));
      return !!chart?.getOption?.()?.series?.length;
    }, null, { timeout: 30000 });
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
    expect(trendMetaBefore.seriesCount).toBeGreaterThanOrEqual(1);
    expect(trendMetaBefore.hasZoom).toBeFalsy();
    expect(trendMetaBefore.firstLabel).toMatch(/\d{4}-\d{2}-\d{2}/);
    expect(trendMetaBefore.lastLabel).toMatch(/\d{4}-\d{2}-\d{2}/);
    await expect(page.getByText('图表范围跟随顶部“时间范围”筛选')).toBeVisible();

    await page.getByRole('combobox', { name: '开始时间' }).fill('2016-01-07 16:00:00');
    await page.getByRole('combobox', { name: '结束时间' }).fill('2016-12-13 23:00:00');
    await page.getByRole('button', { name: '刷新数据' }).click();
    await page.waitForFunction(() => {
      const chart = echarts.getInstanceByDom(document.getElementById('trendChart'));
      const option = chart?.getOption?.() || {};
      const data = option.xAxis?.[0]?.data || [];
      return String(data[0] || '').startsWith('2016-01-07') && String(data[data.length - 1] || '').startsWith('2016-12-13');
    });
    await expect(page.locator('.analysis-scope-item').nth(1)).toContainText('2016-01-07');
    await expect(page.locator('.insight-group').first()).toContainText(/趋势结论|当前时间范围暂无可解释趋势/);
    const trendMetaAfterRange = await page.evaluate(() => {
      const chart = echarts.getInstanceByDom(document.getElementById('trendChart'));
      const option = chart?.getOption?.() || {};
      const data = option.xAxis?.[0]?.data || [];
      return {
        firstLabel: data[0] || '',
        lastLabel: data[data.length - 1] || '',
        hasZoom: Array.isArray(option.dataZoom) && option.dataZoom.length > 0,
      };
    });
    expect(trendMetaAfterRange.firstLabel.startsWith('2016-01-07')).toBeTruthy();
    expect(trendMetaAfterRange.lastLabel.startsWith('2016-12-13')).toBeTruthy();
    expect(trendMetaAfterRange.hasZoom).toBeFalsy();

    const weatherSwitch = page.locator('.analysis-hero-actions .el-switch');
    if (await weatherSwitch.isEnabled()) {
      await weatherSwitch.click();
      await page.waitForTimeout(400);
      const trendMetaAfterToggle = await page.evaluate(() => {
        const chart = echarts.getInstanceByDom(document.getElementById('trendChart'));
        const option = chart?.getOption?.() || {};
        return {
          seriesCount: Array.isArray(option.series) ? option.series.length : 0,
        };
      });
      expect(trendMetaAfterToggle.seriesCount).toBeGreaterThanOrEqual(2);
    }

    await page.locator('.filter-row .el-select').nth(1).click();
    await page.locator('.el-select-dropdown:visible .el-select-dropdown__item').filter({ hasText: /^水$/ }).first().click();
    await expect(page.getByText('水暂未接入')).toBeVisible();

    await page.locator('.filter-row .el-select').nth(1).click();
    await page.locator('.el-select-dropdown:visible .el-select-dropdown__item').filter({ hasText: /^电力$/ }).first().click();
    await page.waitForTimeout(1200);
    await expect(page.locator('#trendChart canvas').first()).toBeVisible();

    await page.getByRole('button', { name: 'AI 分析' }).click();
    await expect(page.getByRole('button', { name: '智能助手' })).toBeVisible();
    await expect(page.locator('.diagnosis--analysis')).toBeVisible({ timeout: 60000 });
    await expect(page.locator('.diagnosis--analysis').getByText(/来源：DeepSeek|来源：模板兜底/).first()).toBeVisible();
  });

  test('anomaly ack -> detail timeline works', async ({ page }) => {
    await page.goto('/');
    await page.getByRole('button', { name: '故障监控' }).click();
    await expect(page.locator('.el-table')).toBeVisible();

    // filter to new status
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

    // switch to acknowledged and inspect first detail history
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

  test('diagnose and write note draft to action dialog', async ({ page }) => {
    await page.goto('/');
    await page.getByRole('button', { name: '故障监控' }).click();
    const row = page.locator('.el-table__body-wrapper tbody tr').first();
    await row.locator('.el-button--danger', { hasText: '诊断' }).click();

    await expect(page.getByRole('button', { name: '智能助手' })).toBeVisible();
    await expect(page.getByText('近24小时 AI 运行统计')).toBeVisible();
    await expect(page.getByText('近24小时 诊断质量评估')).toBeVisible();
    await expect(page.locator('.diagnosis')).toBeVisible({ timeout: 60000 });
    await expect(page.locator('.diagnosis').getByText(/来源：DeepSeek|来源：模板兜底/).first()).toBeVisible();

    await page.getByRole('button', { name: '写入处理备注草稿' }).click();
    await expect(page.locator('.action-form')).toBeVisible();
    const noteValue = await page.locator('.action-form textarea').first().inputValue();
    expect(noteValue).toContain('诊断结论');
  });
});
