import { expect, test } from '@playwright/test';

test.describe('A8 closed loop chains', () => {
  test('building switch keeps cards and chart usable', async ({ page }) => {
    await page.goto('/');
    await expect(page.locator('.topbar h1')).toBeVisible();
    await expect(page.getByText('系统:')).toBeVisible();
    await expect(page.getByText('回归:')).toBeVisible();

    const select = page.locator('.filter-row .el-select').first();
    await select.click();
    await page.locator('.el-select-dropdown__item').nth(1).click();

    await expect(page.locator('.card-value').first()).toContainText('kWh');
    await expect(page.locator('#overviewChart canvas').first()).toBeVisible();

    await page.getByRole('button', { name: '能耗分析' }).click();
    await expect(page.locator('#trendChart canvas').first()).toBeVisible();
    await expect(page.locator('#rankChart canvas').first()).toBeVisible();
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
      await page.getByRole('button', { name: '取消' }).click();
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
    await expect(page.locator('.diagnosis')).toBeVisible();

    await page.getByRole('button', { name: '写入处理备注草稿' }).click();
    await expect(page.locator('.action-form')).toBeVisible();
    const noteValue = await page.locator('.action-form textarea').first().inputValue();
    expect(noteValue).toContain('诊断结论');
  });
});
