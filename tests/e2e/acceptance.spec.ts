import { expect, test } from '@playwright/test';

test.describe('A8 acceptance chains', () => {
  test('ack -> diagnose -> postmortem note -> export csv', async ({ page, request }) => {
    await page.goto('/');
    await page.getByRole('button', { name: '故障监控' }).click();
    await expect(page.locator('.el-table')).toBeVisible();

    await page.locator('.anomaly-toolbar .el-select').nth(1).click();
    await page.locator('.el-select-dropdown__item span').filter({ hasText: /^新告警$/ }).first().click();

    let row = page.locator('.el-table__body-wrapper tbody tr').first();
    if (!(await row.isVisible())) {
      await page.locator('.anomaly-toolbar .el-select').nth(1).click();
      await page.locator('.el-select-dropdown__item span').filter({ hasText: /^已确认$/ }).first().click();
      row = page.locator('.el-table__body-wrapper tbody tr').first();
    }
    await expect(row).toBeVisible();

    await row.locator('.el-button', { hasText: '确认' }).click();
    if (await page.locator('.action-form').isVisible()) {
      await page.locator('.action-form input').first().fill('acceptance');
      await page.locator('.action-form textarea').first().fill('ack by acceptance');
      await page.getByRole('button', { name: '提交' }).click();
      await page.waitForTimeout(1000);
      if (await page.locator('.action-form').isVisible()) {
        await page.keyboard.press('Escape');
      }
      await expect(page.locator('.action-form')).toBeHidden();
    }

    await row.locator('.el-button--danger', { hasText: '诊断' }).click();
    await expect(page.getByRole('button', { name: '智能助手' })).toBeVisible();
    await expect(page.locator('.diagnosis')).toBeVisible({ timeout: 60000 });
    await expect(page.locator('.diagnosis').getByText(/来源：DeepSeek|来源：模板兜底/).first()).toBeVisible();

    await page.getByRole('button', { name: '故障监控' }).click();
    const detailBtn = page.locator('.el-table__body-wrapper tbody tr').first().locator('.el-button', { hasText: '详情' });
    await detailBtn.click();
    const dialog = page.locator('.el-dialog').filter({ hasText: '异常详情' });
    await expect(dialog).toBeVisible();

    const notes = dialog.locator('.postmortem-form');
    await notes.locator('textarea').nth(0).fill('确认原因为测试异常');
    await notes.locator('textarea').nth(1).fill('完成巡检并复位');
    await notes.locator('textarea').nth(2).fill('已恢复且持续稳定');
    await page.getByPlaceholder('复盘人').fill('acceptance');
    await dialog.getByRole('button', { name: '保存复盘' }).click();

    const exportResp = await request.get('/api/anomaly/export?status=acknowledged');
    expect(exportResp.ok()).toBeTruthy();
    const csv = await exportResp.text();
    expect(csv).toContain('anomaly_id,building_id,building_name');
  });
});
