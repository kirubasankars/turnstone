// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

const { test, expect } = require('@playwright/test');
const { showMonitorTab } = require('./helpers');

var viewports = [
  { name: 'mobile', width: 375, height: 667 },
  { name: 'mobile-landscape', width: 667, height: 375 },
  { name: 'tablet', width: 768, height: 1024 },
  { name: 'laptop', width: 1280, height: 800 },
  { name: 'desktop', width: 1440, height: 900 },
  { name: 'wide', width: 1920, height: 1080 },
  { name: 'ultrawide', width: 2560, height: 1440 },
];

for (var i = 0; i < viewports.length; i++) {
  var vp = viewports[i];

  test('layout fits without horizontal scroll on ' + vp.name, async function ({ page }) {
    await page.setViewportSize({ width: vp.width, height: vp.height });
    await page.goto('/');

    var scrollWidth = await page.evaluate(function () {
      return document.documentElement.scrollWidth;
    });
    var clientWidth = await page.evaluate(function () {
      return document.documentElement.clientWidth;
    });
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1);

    await expect(page.getByTestId('app-header')).toBeVisible();
    await expect(page.getByTestId('summary-bar')).toBeVisible();
    await expect(page.getByTestId('panel-keys')).toBeVisible();
    await expect(page.getByTestId('panel-editor')).toBeVisible();
    await expect(page.getByTestId('btn-set')).toBeVisible();
  });

  test('touch targets meet minimum size on ' + vp.name, async function ({ page }) {
    await page.setViewportSize({ width: vp.width, height: vp.height });
    await page.goto('/');

    var buttons = ['btn-refresh', 'btn-get', 'btn-set', 'btn-delete', 'btn-search-keys'];
    for (var b = 0; b < buttons.length; b++) {
      var box = await page.getByTestId(buttons[b]).boundingBox();
      expect(box).not.toBeNull();
      expect(box.height).toBeGreaterThanOrEqual(40);
    }
  });
}

test('mobile tabs switch between data and monitor views', async function ({ page }) {
  await page.setViewportSize({ width: 375, height: 667 });
  await page.goto('/');

  await expect(page.getByTestId('panel-keys')).toBeVisible();
  await expect(page.getByTestId('panel-stats')).not.toBeVisible();

  await page.getByTestId('tab-monitor').click();
  await expect(page.getByTestId('panel-stats')).toBeVisible();
  await expect(page.getByTestId('panel-keys')).not.toBeVisible();

  await page.getByTestId('tab-data').click();
  await expect(page.getByTestId('panel-keys')).toBeVisible();
});

test('editor and key browser stack cleanly on narrow screens', async function ({ page }) {
  await page.setViewportSize({ width: 360, height: 640 });
  await page.goto('/');

  var keysBox = await page.getByTestId('panel-keys').boundingBox();
  var editorBox = await page.getByTestId('panel-editor').boundingBox();
  expect(keysBox).not.toBeNull();
  expect(editorBox).not.toBeNull();
  expect(editorBox.y).toBeGreaterThanOrEqual(keysBox.y);
});

test('wide layout shows keys and editor side by side', async function ({ page }) {
  await page.setViewportSize({ width: 1600, height: 900 });
  await page.goto('/');

  var keysBox = await page.getByTestId('panel-keys').boundingBox();
  var editorBox = await page.getByTestId('panel-editor').boundingBox();
  expect(keysBox).not.toBeNull();
  expect(editorBox).not.toBeNull();

  var rowDelta = Math.abs(keysBox.y - editorBox.y);
  expect(rowDelta).toBeLessThan(80);
});

test('monitor panels visible on desktop without tab switch', async function ({ page }) {
  await page.setViewportSize({ width: 1280, height: 800 });
  await page.goto('/');

  await expect(page.getByTestId('panel-stats')).toBeVisible();
  await expect(page.getByTestId('panel-server-metrics')).toBeVisible();
});
