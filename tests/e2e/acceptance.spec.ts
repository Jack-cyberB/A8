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

test.describe('A8 acceptance chains', () => {
  test('ack -> diagnose -> postmortem note -> export csv', async ({ page, request }) => {
    await page.goto('/');
    await openAnomalyEvents(page);

    await chooseAnomalyStatus(page, '新告警');

    let row = page.locator('.el-table__body-wrapper tbody tr').first();
    if (!(await row.isVisible())) {
      await chooseAnomalyStatus(page, '已确认');
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
    await expect(page.locator('.assistant-page')).toBeVisible();
    await expect(page.locator('.assistant-summary-card')).not.toContainText('正在检索数据和知识库');
    await expect(page.locator('.assistant-section').filter({ hasText: '立即动作' })).toBeVisible();
    await openAnomalyEvents(page);
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
