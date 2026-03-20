import { expect, test } from '@playwright/test';

test.describe('A8 key chains', () => {
  test('building switch updates cards and chart', async ({ page }) => {
    await page.goto('/');
    await expect(page.locator('.topbar h1')).toBeVisible();

    const select = page.locator('.filter-row .el-select').first();
    await select.click();
    await page.locator('.el-select-dropdown__item').nth(1).click();

    await expect(page.locator('.card-value').first()).toContainText('kWh');
    await expect(page.locator('#overviewChart canvas').first()).toBeVisible();
  });

  test('anomaly paging detail and diagnose works', async ({ page }) => {
    await page.goto('/');
    await page.getByRole('button', { name: '故障监控' }).click();
    await expect(page.locator('.el-table')).toBeVisible();

    await page.locator('.anomaly-toolbar .el-select').first().click();
    await page.locator('.el-select-dropdown__item span').filter({ hasText: /^高$/ }).first().click();

    const detailBtn = page.locator('.el-table__body-wrapper .el-button', { hasText: '详情' }).first();
    await detailBtn.click();
    await expect(page.locator('.el-dialog')).toContainText('影响估算');
    await page.getByRole('button', { name: '关闭' }).click();

    const diagnoseBtn = page.locator('.el-table__body-wrapper .el-button--danger', { hasText: '诊断' }).first();
    await diagnoseBtn.click();
    await expect(page.locator('.diagnosis')).toContainText('置信度');
    await expect(page.locator('.diagnosis')).toContainText('知识证据');
  });

  test('same diagnose input returns stable structure twice', async ({ page }) => {
    await page.goto('/');
    await page.getByRole('button', { name: '智能助手' }).click();

    const input = page.locator('.assistant-box textarea');
    await input.fill('请分析建筑突增异常并给出建议');

    const api1 = page.waitForResponse((r) => r.url().includes('/api/ai/diagnose') && r.request().method() === 'POST');
    await page.getByRole('button', { name: '生成诊断' }).click();
    const res1 = await api1;
    const body1 = await res1.json();

    const api2 = page.waitForResponse((r) => r.url().includes('/api/ai/diagnose') && r.request().method() === 'POST');
    await page.getByRole('button', { name: '生成诊断' }).click();
    const res2 = await api2;
    const body2 = await res2.json();

    const d1 = body1?.data?.diagnosis || {};
    const d2 = body2?.data?.diagnosis || {};

    expect(typeof d1.conclusion).toBe('string');
    expect(typeof d2.conclusion).toBe('string');
    expect(Array.isArray(d1.steps)).toBeTruthy();
    expect(Array.isArray(d2.steps)).toBeTruthy();
    expect(d1.anomaly_type).toBe(d2.anomaly_type);
  });
});
