// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

const { test, expect } = require('@playwright/test');
const { showMonitorTab } = require('./helpers');

test.describe('Turnstone Devtool', function () {
  test.beforeEach(async function ({ page }) {
    await page.goto('/');
    await expect(page.getByTestId('app-header')).toBeVisible();
    await expect(page.getByTestId('db-select')).toBeVisible();
  });

  test('loads overview and metrics panels', async function ({ page }) {
    await expect(page.getByRole('heading', { name: 'Turnstone Devtool' })).toBeVisible();
    await expect(page.getByTestId('summary-bar')).toBeVisible();
    await expect(page.getByTestId('db-state')).toContainText('PRIMARY');
    await expect(page.getByTestId('panel-keys')).toBeVisible();
    await expect(page.getByTestId('panel-editor')).toBeVisible();

    await showMonitorTab(page);
    await expect(page.getByTestId('panel-stats')).toBeVisible();
    await expect(page.getByTestId('panel-server-metrics')).toBeVisible();
    await expect(page.getByTestId('db-stats')).not.toContainText('Loading');
  });

  test('set, get, browse, and delete a key', async function ({ page }) {
    var key = 'e2e-test-key';
    var value = 'hello from e2e';

    await page.getByTestId('edit-key').fill(key);
    await page.getByTestId('edit-value').fill(value);
    await page.getByTestId('btn-set').click();
    await expect(page.getByTestId('editor-status')).toHaveText('Saved');

    await page.getByTestId('btn-search-keys').click();
    await expect(page.getByTestId('key-list').getByTestId('key-item').filter({ hasText: key })).toBeVisible();

    await page.getByTestId('btn-clear').click();
    await page.getByTestId('btn-get').click();
    await expect(page.getByTestId('editor-status')).toHaveText('Enter a key name');

    await page.getByTestId('edit-key').fill(key);
    await page.getByTestId('btn-get').click();
    await expect(page.getByTestId('edit-value')).toHaveValue(value);

    await page.getByTestId('btn-delete').click();
    await expect(page.getByTestId('confirm-dialog')).toBeVisible();
    await page.getByTestId('confirm-ok').click();
    await expect(page.getByTestId('editor-status')).toHaveText('Deleted');
    await expect(page.getByTestId('key-list')).toContainText('No keys found');
  });

  test('prefix filter narrows key list', async function ({ page }) {
    await page.getByTestId('edit-key').fill('alpha-one');
    await page.getByTestId('edit-value').fill('1');
    await page.getByTestId('btn-set').click();
    await expect(page.getByTestId('editor-status')).toHaveText('Saved');

    await page.getByTestId('edit-key').fill('beta-two');
    await page.getByTestId('edit-value').fill('2');
    await page.getByTestId('btn-set').click();
    await expect(page.getByTestId('editor-status')).toHaveText('Saved');

    await page.getByTestId('prefix-filter').fill('alpha');
    await page.getByTestId('btn-search-keys').click();
    await expect(page.getByTestId('key-list').getByTestId('key-item')).toHaveCount(1);
    await expect(page.getByTestId('key-list')).toContainText('alpha-one');
    await expect(page.getByTestId('key-list')).not.toContainText('beta-two');
  });

  test('refresh reloads data', async function ({ page }) {
    await showMonitorTab(page);
    await page.getByTestId('btn-refresh').click();
    await expect(page.getByTestId('db-stats')).toBeVisible();
    await expect(page.getByTestId('server-metrics')).toBeVisible();
    await expect(page.getByTestId('last-updated')).not.toHaveText('—');
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

  test('copy value to clipboard', async function ({ page, context }) {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);
    await page.getByTestId('edit-value').fill('copy me');
    await page.getByTestId('btn-copy').click();
    await expect(page.getByTestId('toast')).toHaveText('Copied to clipboard');
  });
});
