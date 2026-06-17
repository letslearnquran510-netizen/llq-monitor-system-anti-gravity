/**
 * OMNIWATCH QC-V4 — Recording Storage Module
 * Saves agent screenshots to date-organized directories and provides
 * retrieval / housekeeping utilities.
 *
 * Directory layout:
 *   ./recordings/YYYY-MM-DD/ROOM-XXX/HH-MM-SS.jpg
 */

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const RECORDINGS_ROOT = path.join(__dirname, 'recordings');

// ─── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Return today's date string (YYYY-MM-DD) in local time.
 * @param {Date} [d]
 * @returns {string}
 */
function dateStr(d = new Date()) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

/**
 * Return current time string (HH-MM-SS) in local time.
 * @param {Date} [d]
 * @returns {string}
 */
function timeStr(d = new Date()) {
  const h = String(d.getHours()).padStart(2, '0');
  const m = String(d.getMinutes()).padStart(2, '0');
  const s = String(d.getSeconds()).padStart(2, '0');
  return `${h}-${m}-${s}`;
}

/**
 * Recursively ensure a directory exists (sync — only called on infrequent writes).
 * @param {string} dir
 */
function ensureDir(dir) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

/**
 * Walk a directory tree and return all file paths.
 * @param {string} dir
 * @returns {string[]}
 */
function walkDir(dir) {
  const results = [];
  if (!fs.existsSync(dir)) return results;

  let entries;
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch {
    return results;
  }

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...walkDir(fullPath));
    } else if (entry.isFile()) {
      results.push(fullPath);
    }
  }
  return results;
}

// ─── Public API ───────────────────────────────────────────────────────────────

/**
 * Save a base64-encoded screenshot to disk.
 *
 * @param {string} agentId   e.g. "ROOM-131"
 * @param {string} base64Data  Raw base64 string (no data-uri prefix)
 * @returns {string|null}  Absolute path of saved file, or null on failure
 */
function saveScreenshot(agentId, base64Data) {
  try {
    if (!agentId || typeof base64Data !== 'string' || base64Data.length === 0) {
      return null;
    }

    // Strip optional data-URI prefix
    const raw = base64Data.replace(/^data:image\/\w+;base64,/, '');

    const now = new Date();
    const dir = path.join(RECORDINGS_ROOT, dateStr(now), agentId);
    ensureDir(dir);

    const filename = `${timeStr(now)}.jpg`;
    const filePath = path.join(dir, filename);

    fs.writeFileSync(filePath, Buffer.from(raw, 'base64'));
    return filePath;
  } catch (err) {
    if (err.code === 'ENOSPC') {
      console.error('[STORAGE] Disk full — cannot save screenshot');
    } else if (err.code === 'EACCES' || err.code === 'EPERM') {
      console.error('[STORAGE] Permission denied —', err.message);
    } else {
      console.error('[STORAGE] Failed to save screenshot:', err.message);
    }
    return null;
  }
}

/**
 * List recording file paths for a given date and (optionally) a specific agent.
 *
 * @param {string} date     e.g. "2026-06-12"
 * @param {string} [agentId]  e.g. "ROOM-131" — omit to list all agents for that date
 * @returns {string[]}
 */
function getRecordings(date, agentId) {
  try {
    if (!date || typeof date !== 'string') return [];

    const base = agentId
      ? path.join(RECORDINGS_ROOT, date, agentId)
      : path.join(RECORDINGS_ROOT, date);

    return walkDir(base).filter((f) => f.endsWith('.jpg'));
  } catch (err) {
    console.error('[STORAGE] getRecordings error:', err.message);
    return [];
  }
}

/**
 * Return storage statistics for the recordings directory.
 *
 * @returns {{ totalSize: number, usedSize: number, freeSize: number, fileCount: number }}
 */
function getStorageStats() {
  try {
    const files = walkDir(RECORDINGS_ROOT);
    let usedSize = 0;

    for (const f of files) {
      try {
        usedSize += fs.statSync(f).size;
      } catch {
        // file may have been deleted between listing and stat
      }
    }

    // Attempt to get volume free space via Node 18 statfs (available on most platforms)
    let totalSize = 0;
    let freeSize = 0;
    try {
      const stat = fs.statfsSync(RECORDINGS_ROOT);
      totalSize = stat.bsize * stat.blocks;
      freeSize = stat.bsize * stat.bavail;
    } catch {
      // statfsSync may not exist on all builds; degrade gracefully
      totalSize = -1;
      freeSize = -1;
    }

    return {
      totalSize,
      usedSize,
      freeSize,
      fileCount: files.length,
    };
  } catch (err) {
    console.error('[STORAGE] getStorageStats error:', err.message);
    return { totalSize: -1, usedSize: 0, freeSize: -1, fileCount: 0 };
  }
}

/**
 * Delete recordings older than `daysToKeep` days.
 *
 * @param {number} [daysToKeep=30]
 * @returns {number} Number of files deleted
 */
function cleanOldRecordings(daysToKeep = 30) {
  let deleted = 0;

  try {
    if (!fs.existsSync(RECORDINGS_ROOT)) return 0;

    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - daysToKeep);
    const cutoffStr = dateStr(cutoff);

    const dateDirs = fs.readdirSync(RECORDINGS_ROOT, { withFileTypes: true });

    for (const entry of dateDirs) {
      if (!entry.isDirectory()) continue;

      // Date dirs are named YYYY-MM-DD; lexicographic compare works.
      if (entry.name < cutoffStr) {
        const dirPath = path.join(RECORDINGS_ROOT, entry.name);
        const files = walkDir(dirPath);
        deleted += files.length;

        fs.rmSync(dirPath, { recursive: true, force: true });
      }
    }
  } catch (err) {
    console.error('[STORAGE] cleanOldRecordings error:', err.message);
  }

  return deleted;
}

export {
  RECORDINGS_ROOT,
  saveScreenshot,
  getRecordings,
  getStorageStats,
  cleanOldRecordings,
};
