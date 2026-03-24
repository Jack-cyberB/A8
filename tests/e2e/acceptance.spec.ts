import { expect, test } from '@playwright/test';

test.describe('A8 acceptance chains', () => {
  test('ack -> diagnose -> postmortem note -> export csv', async ({ page, request }) => {
    await page.route('**/api/ai/diagnose', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          code: 0,
          message: 'ok',
          data: {
            diagnosis: {
              conclusion: '当前异常更接近夜间常开负荷与排程未收敛叠加造成的能耗突增。',
              causes: ['夜间设备未及时关闭。'],
              steps: ['优先核查 BMS 排程和夜间常开设备。'],
              prevention: ['建立夜间巡检和排程复核机制。'],
              recommended_actions: ['立即排查空调、新风、热水与公共照明夜间运行状态。'],
              evidence: [
                {
                  chunk_id: 'rag-accept-1',
                  title: '05-居住与住宿建筑运行场景.md',
                  section: '相似度 0.90',
                  excerpt: '住宿建筑夜间高负荷常与热水系统和公共区照明常开有关。',
                  source_type: 'ragflow',
                },
              ],
              data_evidence: [
                {
                  title: '异常点与24h基线',
                  detail: '异常时刻较近24小时基线明显抬升。',
                  source_type: 'data_signal',
                },
              ],
              provider: 'llm_provider',
              requested_provider: 'auto',
              fallback_used: false,
              knowledge_source: 'ragflow',
              retrieval_hit_count: 1,
              latency_ms: 1012,
              trace_id: 'trace-acceptance-diagnose',
            },
          },
        }),
      });
    });

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
    await expect(page.locator('.assistant-page .panel-title').filter({ hasText: '智能助手' })).toBeVisible();
    await expect(page.locator('.assistant-summary-card')).toContainText('当前异常更接近夜间常开负荷与排程未收敛叠加造成的能耗突增');
    await expect(page.locator('.assistant-section').filter({ hasText: '立即动作' })).toContainText('立即排查空调、新风、热水与公共照明夜间运行状态');
    await expect(page.locator('.assistant-evidence-item').filter({ hasText: 'RAGFlow' })).toContainText('05-居住与住宿建筑运行场景');

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
