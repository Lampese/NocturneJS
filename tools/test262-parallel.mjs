#!/usr/bin/env node
'use strict';

import { spawn, spawnSync } from 'child_process';
import { cpus } from 'os';
import { createWriteStream, existsSync, mkdirSync, writeFileSync } from 'fs';
import { dirname, resolve } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const moduleRoot = resolve(__dirname, '..');
const repoRoot = resolve(moduleRoot, '..');
const logsDir = resolve(repoRoot, 'tmp', 'test262-shards');

function popFlag(args, name, fallback = null) {
  const idx = args.indexOf(name);
  if (idx < 0) return fallback;
  const value = args[idx + 1];
  args.splice(idx, 2);
  return value ?? fallback;
}

function popBool(args, name) {
  const idx = args.indexOf(name);
  if (idx < 0) return false;
  args.splice(idx, 1);
  return true;
}

function stripRunnerArgs(args, { stripTests = false } = {}) {
  const valueFlags = new Set([
    '-c',
    '--config',
    '--gc-threshold',
    '--start',
    '--count',
    '--verbose',
    '--tests-file',
    '-f',
    '-d',
  ]);
  const stripped = [];
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if (arg === '--list-tests' || arg === '--print-count') {
      continue;
    }
    if (valueFlags.has(arg)) {
      if (
        stripTests &&
        (arg === '--start' || arg === '--count' || arg === '--tests-file' || arg === '-f' || arg === '-d')
      ) {
        i += 1;
        continue;
      }
      stripped.push(arg);
      if (i + 1 < args.length) {
        stripped.push(args[i + 1]);
        i += 1;
      }
      continue;
    }
    if (stripTests) {
      if (arg === '-f' || arg === '-d') {
        i += 1;
        continue;
      }
      if (!arg.startsWith('-')) {
        continue;
      }
    }
    stripped.push(arg);
  }
  return stripped;
}

const argv = process.argv.slice(2);
const sep = argv.indexOf('--');
const passThrough = sep >= 0 ? argv.slice(sep + 1) : [];
const opts = sep >= 0 ? argv.slice(0, sep) : argv;

const config =
  popFlag(opts, '--config', null) ||
  popFlag(opts, '-c', null) ||
  'test262-config/test262.conf';
const passThroughRunner = stripRunnerArgs(passThrough);
const passThroughSingle = stripRunnerArgs(passThrough, { stripTests: true });
const enableTui = popBool(opts, '--tui') && process.stdout.isTTY;
const mergeLog =
  popFlag(opts, '--merge-log', null) ||
  resolve(repoRoot, 'tmp', 'test262-merged.log');
const exePath =
  popFlag(opts, '--exe', null) ||
  resolve(
    moduleRoot,
    '_build',
    'native',
    'release',
    'build',
    'cmd',
    'test262',
    'test262.exe',
  );
const workersRaw = popFlag(opts, '--workers', null) || popFlag(opts, '-j', null);
const chunkRaw = popFlag(opts, '--chunk', null);
const quiet = popBool(opts, '--quiet');
const testsFile =
  popFlag(opts, '--tests-file', null) ||
  resolve(logsDir, 'tests.list');
const runnerVerbose = popBool(opts, '--runner-verbose');
const capWorkersRaw = popFlag(opts, '--cap-workers', null);
const noCapWorkers = popBool(opts, '--no-cap-workers');
const memLogPath = popFlag(opts, '--mem-log', null);
const memEveryRaw = popFlag(opts, '--mem-every', null);
const memTopRaw = popFlag(opts, '--mem-top', null);
const memPeak = popBool(opts, '--mem-peak');
const distributeRaw = popFlag(opts, '--distribute', null);

const cpuCount = cpus().length;
const defaultCap = Math.max(1, Math.min(cpuCount, 4));
let workers = Number.parseInt(workersRaw ?? '', 10);
if (!Number.isFinite(workers) || workers <= 0) {
  workers = defaultCap;
}
let chunkSize = Number.parseInt(chunkRaw ?? '', 10);
if (!Number.isFinite(chunkSize) || chunkSize <= 0) {
  chunkSize = -1;
}
let memEvery = Number.parseInt(memEveryRaw ?? '', 10);
if (!Number.isFinite(memEvery) || memEvery <= 0) {
  memEvery = memLogPath ? 1 : 0;
}
if (memPeak) {
  memEvery = 0;
}
const memSampleEnabled = memEvery > 0;
const memLogEnabled = memPeak || memSampleEnabled;
const memLog = memLogEnabled ? (memLogPath || resolve(logsDir, 'test262-mem.log')) : null;
let memTop = Number.parseInt(memTopRaw ?? '', 10);
if (memTopRaw != null) {
  if (!Number.isFinite(memTop) || memTop <= 0) {
    memTop = 0;
  }
} else {
  memTop = memLogEnabled ? 20 : 0;
}
let capWorkers = null;
let capWorkersExplicit = false;
if (!noCapWorkers) {
  capWorkers = Number.parseInt(capWorkersRaw ?? '', 10);
  if (Number.isFinite(capWorkers) && capWorkers > 0) {
    capWorkersExplicit = true;
  } else {
    capWorkers = defaultCap;
  }
}
const distributeMode = normalizeDistributeMode(distributeRaw);

const baseArgs = ['-c', config];
const resultRe = /^Result: (\d+)\/(\d+) errors, (\d+) skipped, (\d+) excluded/;
const progressRe = /^(RUN|SKIP|EXCLUDE|ERROR) (.+)$/;
const needsRunnerVerbose = memSampleEnabled;
const hasRunnerVerbose =
  passThroughRunner.includes('--verbose') || passThroughRunner.includes('--quiet');
const runnerArgs = [];
if (!runnerVerbose && !hasRunnerVerbose && !needsRunnerVerbose) {
  runnerArgs.push('--quiet');
}
if (needsRunnerVerbose && !hasRunnerVerbose && !runnerVerbose) {
  runnerArgs.push('--verbose', 'yes');
}

function parseTestList(text) {
  return text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && line.endsWith('.js'));
}

function normalizeDistributeMode(value) {
  if (!value) return 'contiguous';
  const mode = value.trim().toLowerCase();
  if (mode == 'contiguous' || mode == 'default') {
    return 'contiguous';
  }
  if (mode == 'interleave' || mode == 'round-robin' || mode == 'rr' || mode == 'stripe') {
    return 'interleave';
  }
  throw new Error(`invalid --distribute ${value}`);
}

function interleaveTests(tests, workersCount, chunkSizeValue) {
  if (workersCount <= 1 || chunkSizeValue <= 1) {
    return tests.slice();
  }
  const total = tests.length;
  const ordered = [];
  for (let offset = 0; offset < chunkSizeValue; offset += 1) {
    for (let i = 0; i < workersCount; i += 1) {
      const idx = i * chunkSizeValue + offset;
      if (idx < total) {
        ordered.push(tests[idx]);
      }
    }
  }
  return ordered;
}

function detectTimeTool() {
  const cmd = '/usr/bin/time';
  if (!existsSync(cmd)) {
    throw new Error(`time tool not found at ${cmd}`);
  }
  if (process.platform === 'darwin') {
    return { cmd, args: ['-l'], kind: 'darwin' };
  }
  return { cmd, args: ['-v'], kind: 'gnu' };
}

function parseMaxRssKb(output, kind) {
  if (kind === 'darwin') {
    const match = output.match(/^\s*(\d+)\s+maximum resident set size/m);
    if (!match) return null;
    const bytes = Number.parseInt(match[1], 10);
    if (!Number.isFinite(bytes)) return null;
    return Math.ceil(bytes / 1024);
  }
  const match = output.match(/Maximum resident set size.*:\s*(\d+)/i);
  if (!match) return null;
  const kb = Number.parseInt(match[1], 10);
  if (!Number.isFinite(kb)) return null;
  return kb;
}

function parseResultLine(output) {
  const lines = output.split(/\r?\n/);
  for (const line of lines) {
    const match = resultRe.exec(line);
    if (!match) continue;
    return {
      failed: Number.parseInt(match[1], 10),
      total: Number.parseInt(match[2], 10),
      skipped: Number.parseInt(match[3], 10),
      excluded: Number.parseInt(match[4], 10),
    };
  }
  return null;
}

function sampleRssKb(pid) {
  if (!pid) return null;
  const result = spawnSync('ps', ['-o', 'rss=', '-p', String(pid)], {
    encoding: 'utf8',
  });
  if (result.status !== 0) {
    return null;
  }
  const text = (result.stdout || '').trim();
  const value = Number.parseInt(text, 10);
  if (!Number.isFinite(value)) return null;
  return value;
}

function findChildPid(ppid) {
  const ppidText = String(ppid);
  const pgrep = spawnSync('pgrep', ['-P', ppidText], { encoding: 'utf8' });
  if (!pgrep.error && pgrep.status === 0) {
    const text = (pgrep.stdout || '').trim();
    if (text.length > 0) {
      const first = text.split(/\s+/)[0];
      const pid = Number.parseInt(first, 10);
      if (Number.isFinite(pid)) {
        return pid;
      }
    }
  }
  const ps = spawnSync('ps', ['-o', 'pid=,ppid=,comm='], { encoding: 'utf8' });
  if (ps.status !== 0) {
    return null;
  }
  const lines = (ps.stdout || '').split(/\r?\n/);
  let fallback = null;
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    const parts = trimmed.split(/\s+/, 3);
    if (parts.length < 2) continue;
    const pid = Number.parseInt(parts[0], 10);
    const parent = Number.parseInt(parts[1], 10);
    const comm = parts[2] || '';
    if (!Number.isFinite(pid) || !Number.isFinite(parent)) continue;
    if (String(parent) !== ppidText) continue;
    if (comm.includes('test262')) {
      return pid;
    }
    if (fallback == null) {
      fallback = pid;
    }
  }
  return fallback;
}

function openMemLog(path) {
  mkdirSync(dirname(path), { recursive: true });
  return createWriteStream(path, { flags: 'w' });
}

function writeMemLine(output, tag, pid, kind, path, count, rssKb, durationMs) {
  if (!output) return;
  const ts = Date.now();
  const duration = durationMs != null ? ` duration_ms=${durationMs}` : '';
  output.write(
    `${ts} [${tag}] rss_kb=${rssKb} pid=${pid} count=${count} kind=${kind}${duration} ${path}\n`,
  );
}

function recordTopSample(memState, entry) {
  if (!memState || memState.topLimit <= 0) return;
  const top = memState.top;
  if (top.length < memState.topLimit) {
    top.push(entry);
  } else {
    let minIdx = 0;
    let minVal = top[0].rssKb;
    for (let i = 1; i < top.length; i += 1) {
      if (top[i].rssKb < minVal) {
        minVal = top[i].rssKb;
        minIdx = i;
      }
    }
    if (entry.rssKb <= minVal) {
      return;
    }
    top[minIdx] = entry;
  }
}

function workerTag(index) {
  return `worker-${String(index).padStart(2, '0')}`;
}

function runSingleTestPeak(testPath, timeTool, count, state) {
  return new Promise((resolve) => {
    const startedAt = Date.now();
    const args = timeTool.args
      .concat([exePath])
      .concat(baseArgs)
      .concat(runnerArgs)
      .concat(['-f', testPath])
      .concat(passThroughSingle);
    const proc = spawn(timeTool.cmd, args, { cwd: moduleRoot });
    if (state) {
      state.status = 'running';
      state.current = testPath;
      state.startedAt = startedAt;
      state.procPid = proc.pid;
      state.childPid = null;
      state.lastRssKb = null;
    }
    let stdout = '';
    let stderr = '';
    proc.stdout.on('data', (chunk) => {
      stdout += chunk.toString();
    });
    proc.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
    });
    proc.on('close', (code) => {
      const durationMs = Date.now() - startedAt;
      const output = `${stdout}\n${stderr}`;
      const rssKb = parseMaxRssKb(output, timeTool.kind);
      const result = parseResultLine(output);
      resolve({
        testPath,
        rssKb,
        result,
        exitCode: code,
        pid: proc.pid,
        count,
        durationMs,
      });
    });
  });
}

async function runMemPeak(tests, workersCount, memState, render, states) {
  const timeTool = detectTimeTool();
  let cursor = 0;
  let failed = 0;
  let totalRun = 0;
  let skipped = 0;
  let excluded = 0;
  const workerCount = Math.max(1, Math.min(workersCount, tests.length));
  const runWorker = async (workerIndex) => {
    const state = states ? states[workerIndex] : null;
    while (true) {
      const index = cursor;
      cursor += 1;
      if (index >= tests.length) break;
      const testPath = tests[index];
      const promise = runSingleTestPeak(testPath, timeTool, index + 1, state);
      if (render) render();
      const result = await promise;
      if (state) {
        state.status = 'idle';
        state.current = '';
        state.startedAt = 0;
        state.procPid = null;
        state.childPid = null;
        state.lastDurationMs = result.durationMs;
        state.done += 1;
        if (render) render();
      }
      if (memState && result.rssKb != null) {
        if (result.rssKb > memState.peak) {
          memState.peak = result.rssKb;
        }
        memState.samples += 1;
        recordTopSample(memState, {
          rssKb: result.rssKb,
          tag: workerTag(workerIndex),
          pid: result.pid,
          count: result.count,
          kind: 'PEAK',
          durationMs: result.durationMs,
          path: result.testPath,
          ts: Date.now(),
        });
        writeMemLine(
          memState.log,
          workerTag(workerIndex),
          result.pid,
          'PEAK',
          result.testPath,
          result.count,
          result.rssKb,
          result.durationMs,
        );
      } else if (memState) {
        memState.failed = true;
      }
      if (result.result) {
        failed += result.result.failed;
        totalRun += result.result.total;
        skipped += result.result.skipped;
        excluded += result.result.excluded;
      }
    }
    if (state) {
      state.status = 'done';
      if (render) render();
    }
  };
  const pool = [];
  for (let i = 0; i < workerCount; i += 1) {
    pool.push(runWorker(i));
  }
  await Promise.all(pool);
  return { failed, totalRun, skipped, excluded };
}

function runForList() {
  return new Promise((resolveList, reject) => {
    const args = baseArgs
      .concat(runnerArgs)
      .concat(['--list-tests'])
      .concat(passThroughRunner);
    const proc = spawn(exePath, args, { cwd: moduleRoot });
    let buffer = '';
    proc.stdout.on('data', (chunk) => {
      buffer += chunk.toString();
    });
    proc.stderr.on('data', (chunk) => {
      buffer += chunk.toString();
    });
    proc.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(`list failed (exit ${code})\n${buffer}`));
        return;
      }
      resolveList(parseTestList(buffer));
    });
  });
}

function shardTag(index) {
  return `shard-${String(index).padStart(2, '0')}`;
}

function openMergedLog() {
  mkdirSync(dirname(mergeLog), { recursive: true });
  return createWriteStream(mergeLog, { flags: 'w' });
}

function writeMergedLine(output, tag, line) {
  if (!output) return;
  if (resultRe.test(line)) return;
  const prefix = `[${tag}]`;
  if (line.length === 0) {
    output.write(`${prefix}\n`);
  } else {
    output.write(`${prefix} ${line}\n`);
  }
}

function writeMergedMarker(output, tag, label) {
  if (!output) return;
  output.write(`[${tag}] ${label}\n`);
}

function runShard(shard, state, render, merged, memState) {
  return new Promise((resolveShard, reject) => {
    const args = baseArgs
      .concat(runnerArgs)
      .concat(['--tests-file', testsFile])
      .concat(['--start', String(shard.start), '--count', String(shard.count)])
      .concat(passThroughRunner);
    const logPath = resolve(logsDir, `shard-${String(shard.index).padStart(2, '0')}.log`);
    const log = createWriteStream(logPath, { flags: 'w' });
    const proc = spawn(exePath, args, { cwd: moduleRoot });
    const tag = shardTag(shard.index);
    let lineBuf = '';
    let result = null;
    writeMergedMarker(merged, tag, '>>> start');
    let memCount = 0;
    const handleLine = (line) => {
      writeMergedLine(merged, tag, line);
      const runMatch = progressRe.exec(line);
      if (runMatch && state) {
        const kind = runMatch[1];
        const current = runMatch[2];
        state.seen = Math.min(state.seen + 1, state.total);
        state.current = kind === 'RUN' ? current : `${kind} ${current}`;
        state.status = 'running';
        if (render) render();
        if (memState && kind == 'RUN') {
          memCount += 1;
          if (memCount % memState.every == 0) {
            const rssKb = sampleRssKb(proc.pid);
            if (rssKb != null) {
              if (rssKb > memState.peak) {
                memState.peak = rssKb;
              }
              memState.samples += 1;
              recordTopSample(memState, {
                rssKb,
                tag,
                pid: proc.pid,
                count: memCount,
                kind,
                path: current,
                ts: Date.now(),
              });
              writeMemLine(memState.log, tag, proc.pid, kind, current, memCount, rssKb, null);
            } else {
              memState.failed = true;
            }
          }
        }
      }
      const match = resultRe.exec(line);
      if (match) {
        result = {
          failed: Number.parseInt(match[1], 10),
          total: Number.parseInt(match[2], 10),
          skipped: Number.parseInt(match[3], 10),
          excluded: Number.parseInt(match[4], 10),
        };
      }
    };
    const onData = (chunk) => {
      const text = chunk.toString();
      log.write(text);
      lineBuf += text;
      let idx = lineBuf.indexOf('\n');
      while (idx >= 0) {
        const line = lineBuf.slice(0, idx);
        lineBuf = lineBuf.slice(idx + 1);
        handleLine(line);
        idx = lineBuf.indexOf('\n');
      }
    };
    proc.stdout.on('data', onData);
    proc.stderr.on('data', onData);
    proc.on('close', (code) => {
      if (lineBuf.length > 0) {
        handleLine(lineBuf);
        lineBuf = '';
      }
      log.end();
      writeMergedMarker(merged, tag, `<<< end (exit ${code})`);
      if (code !== 0) {
        if (state) {
          state.status = 'failed';
          if (render) render();
        }
        reject(new Error(`shard ${shard.index} failed (exit ${code})`));
        return;
      }
      if (state) {
        state.seen = state.total;
        state.status = 'done';
        if (render) render();
      }
      resolveShard({ shard, result });
    });
  });
}

function writeTestsFile(path, tests) {
  mkdirSync(dirname(path), { recursive: true });
  const text = tests.length > 0 ? `${tests.join('\n')}\n` : '';
  writeFileSync(path, text);
}

function renderTui(states, startedAt) {
  if (!enableTui) return;
  const lines = [];
  const elapsedMs = Date.now() - startedAt;
  const elapsedSec = Math.floor(elapsedMs / 1000);
  const totalDone = states.reduce((sum, state) => sum + state.seen, 0);
  const totalCount = states.reduce((sum, state) => sum + state.total, 0);
  lines.push(
    `test262 shards: ${states.length} | ${totalDone}/${totalCount} done | elapsed ${elapsedSec}s`,
  );
  for (const state of states) {
    const status = state.status.padEnd(7, ' ');
    const progress = `${state.seen}/${state.total}`.padEnd(9, ' ');
    const current = state.current || '';
    lines.push(
      `[${String(state.index).padStart(2, '0')}] ${status} ${progress} ${current}`,
    );
  }
  process.stdout.write('\x1b[2J\x1b[H' + lines.join('\n') + '\n');
}

function sampleMemPeakState(state) {
  if (!state || !state.procPid) return;
  let childPid = state.childPid;
  if (!childPid) {
    childPid = findChildPid(state.procPid);
    if (childPid) {
      state.childPid = childPid;
    }
  }
  const pidToSample = childPid || state.procPid;
  const rssKb = sampleRssKb(pidToSample);
  if (rssKb != null) {
    state.lastRssKb = rssKb;
  }
}

function renderMemPeakTui(states, startedAt, total) {
  if (!enableTui) return;
  const lines = [];
  const elapsedMs = Date.now() - startedAt;
  const elapsedSec = Math.floor(elapsedMs / 1000);
  const done = states.reduce((sum, state) => sum + state.done, 0);
  lines.push(
    `mem-peak workers: ${states.length} | ${done}/${total} done | elapsed ${elapsedSec}s`,
  );
  for (const state of states) {
    const status = state.status.padEnd(7, ' ');
    const rss = state.lastRssKb != null ? `${state.lastRssKb}kb` : '--';
    const runtime =
      state.status === 'running'
        ? `${Math.floor((Date.now() - state.startedAt) / 1000)}s`
        : '--';
    const current = state.current || '';
    lines.push(
      `[${String(state.index).padStart(2, '0')}] ${status} rss ${rss} time ${runtime} ${current}`,
    );
  }
  process.stdout.write('\x1b[2J\x1b[H' + lines.join('\n') + '\n');
}

async function main() {
  mkdirSync(logsDir, { recursive: true });
  if (!quiet) {
    console.log(`test262 runner: ${exePath}`);
  }
  let tests = await runForList();
  if (tests.length === 0) {
    throw new Error('no tests found');
  }
  const total = tests.length;
  if (!quiet) {
    console.log(`total tests: ${total}`);
  }
  const memLogStream = memLogEnabled ? openMemLog(memLog) : null;
  const memState = memLogStream
    ? {
        log: memLogStream,
        every: memEvery,
        peak: 0,
        samples: 0,
        failed: false,
        topLimit: memTop,
        top: [],
      }
    : null;
  const closeMem = () => {
    if (memLogStream) {
      memLogStream.end();
    }
  };
  if (memLogStream && !quiet) {
    console.log(`memory log: ${memLog}`);
  }
  if (memPeak) {
    let workerCount = workers;
    if (capWorkers != null && workerCount > capWorkers) {
      workerCount = capWorkers;
    }
    workerCount = Math.max(1, Math.min(workerCount, tests.length));
    if (distributeMode === 'interleave') {
      const peakChunkSize = Math.ceil(total / workerCount);
      tests = interleaveTests(tests, workerCount, peakChunkSize);
    }
    const startedAt = Date.now();
    const memStates = enableTui
      ? Array.from({ length: workerCount }, (_, index) => ({
          index,
          status: 'idle',
          current: '',
          startedAt: 0,
          procPid: null,
          childPid: null,
          lastRssKb: null,
          lastDurationMs: 0,
          done: 0,
        }))
      : null;
    const render = memStates ? () => renderMemPeakTui(memStates, startedAt, total) : null;
    let tuiTimer = null;
    if (memStates) {
      render();
      tuiTimer = setInterval(() => {
        for (const state of memStates) {
          sampleMemPeakState(state);
        }
        render();
      }, 1000);
    }
    if (!quiet) {
      console.log(`mem-peak workers: ${workerCount}`);
    }
    const summary = await runMemPeak(tests, workerCount, memState, render, memStates);
    if (tuiTimer) {
      clearInterval(tuiTimer);
      tuiTimer = null;
      if (render) render();
    }
    if (memState && memState.log) {
      if (memState.failed) {
        memState.log.write(`[merge] warning=mem-sample-failed\n`);
      }
      memState.log.write(
        `[merge] samples=${memState.samples} peak_rss_kb=${memState.peak}\n`,
      );
      if (memState.topLimit > 0 && memState.top.length > 0) {
        const entries = memState.top
          .slice()
          .sort((a, b) => b.rssKb - a.rssKb || a.ts - b.ts);
        for (const entry of entries) {
          const duration =
            entry.durationMs != null ? ` duration_ms=${entry.durationMs}` : '';
          memState.log.write(
            `[merge] top rss_kb=${entry.rssKb} tag=${entry.tag} count=${entry.count}${duration} path=${entry.path}\n`,
          );
        }
      }
      closeMem();
    }
    const summaryLine = `Result: ${summary.failed}/${summary.totalRun} errors, ${summary.skipped} skipped, ${summary.excluded} excluded`;
    console.log(summaryLine);
    return;
  }
  const mergedLog = openMergedLog();
  let mergedClosed = false;
  const closeMerged = () => {
    if (!mergedClosed) {
      mergedLog.end();
      mergedClosed = true;
    }
  };
  const chunkSpecified = chunkSize > 0;
  let capApplied = false;
  if (chunkSpecified) {
    workers = Math.ceil(total / chunkSize);
    if (capWorkersExplicit && capWorkers != null && workers > capWorkers) {
      workers = capWorkers;
      chunkSize = Math.ceil(total / workers);
      capApplied = true;
    }
  } else {
    if (capWorkers != null && workers > capWorkers) {
      workers = capWorkers;
      capApplied = true;
    }
    chunkSize = Math.ceil(total / workers);
  }
  if (capApplied && !quiet) {
    console.log(`capping workers to ${workers} (use --no-cap-workers to override)`);
  }
  if (distributeMode === 'interleave') {
    tests = interleaveTests(tests, workers, chunkSize);
  }
  writeTestsFile(testsFile, tests);
  const shards = [];
  for (let i = 0; i < workers; i += 1) {
    const start = i * chunkSize;
    const count = Math.min(chunkSize, total - start);
    if (count <= 0) break;
    shards.push({ index: i, start, count });
  }
  if (!quiet) {
    console.log(`workers: ${shards.length}, chunk size: ${chunkSize}`);
  }
  const startedAt = Date.now();
  const states = shards.map((shard) => ({
    index: shard.index,
    status: 'queued',
    current: '',
    total: shard.count,
    seen: 0,
  }));
  const render = () => renderTui(states, startedAt);
  render();
  let results;
  try {
    results = await Promise.all(
      shards.map((shard, idx) =>
        runShard(shard, states[idx], render, mergedLog, memState),
      ),
    );
  } catch (err) {
    writeMergedMarker(mergedLog, 'merge', `<<< error: ${err?.message ?? err}`);
    closeMerged();
    closeMem();
    throw err;
  }
  let failed = 0;
  let totalRun = 0;
  let skipped = 0;
  let excluded = 0;
  for (const entry of results) {
    if (!entry.result) continue;
    failed += entry.result.failed;
    totalRun += entry.result.total;
    skipped += entry.result.skipped;
    excluded += entry.result.excluded;
  }
  const summaryLine = `Result: ${failed}/${totalRun} errors, ${skipped} skipped, ${excluded} excluded`;
  mergedLog.write(`[merge] ${summaryLine}\n`);
  closeMerged();
  if (memState && memState.log) {
    if (memState.failed) {
      memState.log.write(`[merge] warning=mem-sample-failed\n`);
    }
    memState.log.write(`[merge] samples=${memState.samples} peak_rss_kb=${memState.peak}\n`);
    if (memState.topLimit > 0 && memState.top.length > 0) {
      const entries = memState.top
        .slice()
        .sort((a, b) => b.rssKb - a.rssKb || a.ts - b.ts);
      for (const entry of entries) {
        const duration =
          entry.durationMs != null ? ` duration_ms=${entry.durationMs}` : '';
        memState.log.write(
          `[merge] top rss_kb=${entry.rssKb} tag=${entry.tag} count=${entry.count}${duration} path=${entry.path}\n`,
        );
      }
    }
    closeMem();
  }
  console.log(summaryLine);
}

main().catch((err) => {
  console.error(err?.stack || String(err));
  process.exit(1);
});
