// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

const fs = require('fs');
const path = require('path');

const PID_FILE = path.join(__dirname, '.server-pid');
const HOME = path.join(__dirname, '.testdata');

module.exports = async function globalTeardown() {
  if (fs.existsSync(PID_FILE)) {
    var pid = parseInt(fs.readFileSync(PID_FILE, 'utf8'), 10);
    try {
      process.kill(pid, 'SIGTERM');
    } catch (_) {
      // process may already be gone
    }
    fs.unlinkSync(PID_FILE);
  }
  if (fs.existsSync(HOME)) {
    fs.rmSync(HOME, { recursive: true, force: true });
  }
};
