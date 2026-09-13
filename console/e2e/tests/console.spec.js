// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

const { test, expect } = require('@playwright/test');
const { showMonitorTab } = require('./helpers');

test.describe('Turnstone Console', function () {
  test.beforeEach(async function ({ page }) {
    await page.goto('/');
    await expect(page.getByTestId('app-header')).toBeVisible();
    await expect(page.getByTestId('db-select')).toBeVisible();
  });

  test('loads overview and metrics panels', async function ({ page }) {
    await expect(page.getByRole('heading', { name: 'Turnstone Console' })).toBeVisible();
    await expect(page.getByTestId('db-state')).toContainText('PRIMARY');
    await expect(page.getByTestId('panel-stats')).toBeVisible();
    await expect(page.getByTestId('panel-stats')).toContainText('Database');
    await expect(page.getByTestId('panel-server-metrics')).toBeVisible();
    await expect(page.getByTestId('panel-server-metrics')).toContainText('Server');
    await expect(page.getByTestId('panel-db-metrics')).toContainText('Prometheus');
    await expect(page.getByTestId('db-stats')).not.toContainText('Loading');
    await expect(page.getByTestId('panel-gc')).toBeVisible();
    await expect(page.getByTestId('panel-gc')).toContainText('GC');
    await expect(page.getByTestId('gc-stats')).toContainText('Index live');
    await expect(page.getByTestId('gc-stats')).toContainText('WAL segments');
    await expect(page.getByTestId('gc-stats')).toContainText('Hash segments');
  });

  test('formats key count as a number not a byte size', async function ({ page }) {
    await page.route('**/api/databases/*/stats', async function (route) {
      var response = await route.fetch();
      var stats = await response.json();
      stats.key_count = 5000670;
      stats.log_bytes = 1400000000;
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(stats)
      });
    });
    await showMonitorTab(page);
    await page.getByTestId('btn-refresh').click();
    await expect(page.getByTestId('overview-keys')).toHaveText('5M');
    await expect(page.getByTestId('overview-keys')).not.toContainText('MB');
    await expect(page.getByTestId('key-count')).toHaveText('5M keys');
    await expect(page.getByTestId('db-stats')).toContainText('5M');
    await expect(page.getByTestId('db-stats')).toContainText('1.4 GB');
  });

  test('shows wal segment gc and live bytes', async function ({ page }) {
    await page.route('**/api/databases/*/stats', async function (route) {
      var response = await route.fetch();
      var stats = await response.json();
      stats.index_arena_bytes = 4000;
      stats.index_live_bytes = 1000;
      stats.wal_live_bytes = 256;
      stats.wal_garbage_bytes = 768;
      stats.wal_segments = 12;
      stats.hash_shards = 7;
      stats.hash_shards_compacted = 3;
      stats.hash_compact_bytes_reclaimed = 2048;
      stats.hash_compact_unix = Math.floor(Date.now() / 1000) - 12;
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(stats)
      });
    });
    await showMonitorTab(page);
    await page.getByTestId('btn-refresh').click();
    await expect(page.getByTestId('gc-stats')).toContainText('4 KB');
    await expect(page.getByTestId('gc-stats')).toContainText('WAL live');
    await expect(page.getByTestId('gc-stats')).toContainText('WAL segments');
    await expect(page.getByTestId('gc-stats')).toContainText('12');
    await expect(page.getByTestId('gc-stats')).toContainText('Hash segments');
    await expect(page.getByTestId('gc-stats')).toContainText('Hash compacted');
    await expect(page.getByTestId('gc-stats')).toContainText('3');
    await expect(page.getByTestId('gc-stats')).toContainText('Last compact');
    await expect(page.getByTestId('gc-stats')).toContainText('ago');
  });

  test('refresh reloads data', async function ({ page }) {
    await showMonitorTab(page);
    await page.getByTestId('btn-refresh').click();
    await expect(page.getByTestId('db-stats')).toBeVisible();
    await expect(page.getByTestId('server-metrics')).toBeVisible();
    await expect(page.getByTestId('overview-updated')).not.toHaveText('—');
  });

  test('switching database updates stats panel', async function ({ page }) {
    var options = await page.getByTestId('db-select').locator('option').count();
    if (options < 2) {
      test.skip();
      return;
    }
    await showMonitorTab(page);
    await page.getByTestId('db-select').selectOption({ index: 1 });
    await expect(page.getByTestId('db-stats')).toBeVisible();
  });

  test('metrics tab groups database charts', async function ({ page }) {
    await page.getByTestId('tab-prometheus').click();
    await expect(page.getByTestId('panel-prometheus')).toBeVisible();
    await expect(page.getByTestId('metrics-group-server')).toBeVisible();
    await expect(page.getByTestId('metrics-group-database')).toBeVisible();
    await expect(page.getByTestId('metrics-group-overview')).toBeVisible();
    await expect(page.getByTestId('metrics-group-wal')).toBeVisible();
    await expect(page.getByTestId('metrics-group-hash-index')).toBeVisible();
    await expect(page.getByTestId('metrics-group-overview')).toContainText('key count');
    await expect(page.getByTestId('metrics-group-overview')).toContainText('replicas');
    await expect(page.getByTestId('metrics-group-wal')).toContainText('wal segments');
    await expect(page.getByTestId('metrics-group-hash-index')).toContainText('hash shards');
    await expect(page.getByTestId('metrics-group-hash-index')).toContainText('index allocated');
  });
});
