const { chromium } = require('playwright');
const fs = require('fs');

const BASE = 'http://127.0.0.1:8000';
const TESTS = [
  { type: 'knowledge', question: '建筑节能设计有哪些规范要求？' },
  { type: 'anomaly',   question: '当前设备能耗突然升高50%，可能是什么原因？' },
  { type: 'saving',    question: '针对空调系统，有哪些节能优化建议？' },
  { type: 'fault',     question: '空调制冷效果差，如何排查问题？' },
];

function checkIssues(text) {
  const issues = [];
  if (/\*\*[^*]+\*\*/.test(text)) issues.push('markdown未渲染(**符号)');
  if (/#{1,3}\s/.test(text)) issues.push('markdown未渲染(###符号)');
  if (/相似度\s*0\.0%/.test(text)) issues.push('相似度显示0.0%');
  if (text.length < 20 && !/正在/.test(text)) issues.push('回答过短可能截断');
  return issues;
}

(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  await page.setViewportSize({ width: 1400, height: 900 });
  const report = [];

  await page.goto(BASE, { waitUntil: 'networkidle' });
  // Navigate to assistant tab
  await page.click('button.nav-btn >> text=智能助手');
  await page.waitForTimeout(800);

  for (const test of TESTS) {
    console.log(`Testing: ${test.type} - ${test.question}`);

    // Record current card count before sending
    const prevCount = await page.evaluate(() => document.querySelectorAll('.unified-assistant-card').length);

    // Clear input and type question
    const input = await page.waitForSelector('textarea', { timeout: 5000 });
    await input.click();
    await input.fill(test.question);
    await page.keyboard.press('Control+Enter');

    // Wait for new card to appear
    await page.waitForFunction((prev) => {
      return document.querySelectorAll('.unified-assistant-card').length > prev;
    }, prevCount, { timeout: 15000 }).catch(() => console.log('timeout waiting for new card'));

    // Wait for content to stabilize (no change for 2 consecutive checks 1s apart)
    const PLACEHOLDERS = ['正在检索知识库', '正在检索数据和知识库', 'DeepSeek 正在生成'];
    let stable = false;
    let lastText = '';
    const deadline = Date.now() + 90000;
    while (Date.now() < deadline) {
      await page.waitForTimeout(1500);
      const curText = await page.evaluate((prev) => {
        const cards = document.querySelectorAll('.unified-assistant-card');
        if (cards.length <= prev) return '';
        return cards[cards.length - 1].innerText || '';
      }, prevCount);
      const isPlaceholder = PLACEHOLDERS.some(p => curText.includes(p));
      if (!isPlaceholder && curText.length > 30 && curText === lastText) {
        stable = true;
        break;
      }
      lastText = curText;
    }
    if (!stable) console.log('content did not stabilize, using last captured text');

    await page.screenshot({ path: `docs/screenshots/test-${test.type}.png`, fullPage: false });

    const cardText = lastText;
    const issues = checkIssues(cardText);
    const entry = `[${test.type}] ${test.question}\n  issues: ${issues.length ? issues.join(', ') : 'none'}\n  length: ${cardText.length}\n  preview: ${cardText.slice(0, 200).replace(/\n/g, ' ')}\n`;
    report.push(entry);
    console.log(entry);
  }

  fs.writeFileSync('D:/Project/2026/A8/test-report.txt', report.join('\n'));
  console.log('Report saved to test-report.txt');
  await browser.close();
})();
