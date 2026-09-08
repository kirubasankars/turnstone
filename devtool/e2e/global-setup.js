// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

const { spawn, execSync } = require('child_process');
const fs = require('fs');
const http = require('http');
const path = require('path');

const ROOT = path.join(__dirname, '..', '..');
const HOME = path.join(__dirname, '.testdata');
const BIN = path.join(ROOT, 'bin', 'turnstone');
const DEVTOOL_URL = 'http://127.0.0.1:18080';
const PID_FILE = path.join(__dirname, '.server-pid');

function waitFor(url, attempts) {
  attempts = attempts || 60;
  return new Promise(function (resolve, reject) {
    var tries = 0;
    function poll() {
      http.get(url + '/api/databases', function (res) {
        res.resume();
        if (res.statusCode === 200) {
          resolve();
        } else if (++tries >= attempts) {
          reject(new Error('server returned ' + res.statusCode));
        } else {
          setTimeout(poll, 250);
        }
      }).on('error', function () {
        if (++tries >= attempts) {
          reject(new Error('server did not start at ' + url));
        } else {
          setTimeout(poll, 250);
        }
      });
    }
    poll();
  });
}

module.exports = async function globalSetup() {
  execSync('go build -o bin/turnstone ./cmd/turnstone', { cwd: ROOT, stdio: 'inherit' });

  if (fs.existsSync(HOME)) {
    fs.rmSync(HOME, { recursive: true, force: true });
  }
  execSync(BIN + ' init --home ' + HOME, { cwd: ROOT, stdio: 'inherit' });

  var configPath = path.join(HOME, 'turnstone.json');
  var cfg = JSON.parse(fs.readFileSync(configPath, 'utf8'));
  cfg.port = ':16379';
  cfg.metrics_addr = ':19090';
  fs.writeFileSync(configPath, JSON.stringify(cfg, null, 2));

  if (fs.existsSync(PID_FILE)) {
    try {
      process.kill(parseInt(fs.readFileSync(PID_FILE, 'utf8'), 10), 'SIGTERM');
    } catch (_) {}
    fs.unlinkSync(PID_FILE);
  }

  var logFile = fs.openSync(path.join(__dirname, 'server.log'), 'w');
  var proc = spawn(BIN, [
    'server', '--home', HOME, '--dev',
    '--devtool-addr', '127.0.0.1:18080',
  ], {
    cwd: ROOT,
    stdio: ['ignore', logFile, logFile],
    detached: true,
  });
  proc.unref();

  fs.writeFileSync(PID_FILE, String(proc.pid));
  process.env.DEVTOOL_URL = DEVTOOL_URL;

  await waitFor(DEVTOOL_URL);
};
