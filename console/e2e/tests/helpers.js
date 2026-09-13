// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

async function showMonitorTab(page) {
  var tab = page.getByTestId('tab-monitor');
  if (await tab.isVisible()) {
    await tab.click();
  }
}

module.exports = { showMonitorTab };
