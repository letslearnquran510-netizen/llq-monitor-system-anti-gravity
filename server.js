/**
 * ╔═══════════════════════════════════════════════════════════════════════╗
 * ║                  OMNIWATCH  QC-V4  RELAY  SERVER                    ║
 * ║  WebSocket relay bridging desktop agents → browser dashboards       ║
 * ╚═══════════════════════════════════════════════════════════════════════╝
 *
 * Port  : 9600
 * Paths : /agent      — desktop agent connections
 *         /dashboard  — dashboard client connections
 *         /           — dashboard fallback
 *
 * REST  : /api/agents, /api/agent/:id/screenshot, /api/health, /api/storage
 */

import http from 'node:http';
import https from 'node:https';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { WebSocketServer, WebSocket } from 'ws';

import {
  validateAgentToken,
  validateDashboardToken,
  createSession,
  removeSession,
} from './auth.js';

import {
  saveScreenshot,
  getRecordings,
  getStorageStats,
  cleanOldRecordings,
  RECORDINGS_ROOT,
} from './storage.js';

// ─── Constants & Paths ────────────────────────────────────────────────────────

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = parseInt(process.env.PORT || '9600', 10);
const HEARTBEAT_INTERVAL_MS = 30_000;
const CLEANUP_INTERVAL_MS = 30_000;
const FEED_RATE_LIMIT_MS = 40; // Allow ~25 fps max per agent
const MAX_AGENTS = 100;
const MAX_DASHBOARDS = 10;
const DASHBOARD_HTML_PATH = path.resolve(__dirname, 'omniwatch-dashboard.html');

let knownCloudHost = null;

const startTime = Date.now();

// ─── Registries ───────────────────────────────────────────────────────────────

/**
 * @typedef {Object} AgentRecord
 * @property {import('ws').WebSocket} ws
 * @property {string} agentId
 * @property {string} teacherName
 * @property {number} roomNumber
 * @property {number} connectedAt
 * @property {number} lastHeartbeat
 * @property {object|null} lastFeed
 * @property {'active'|'idle'|'disconnected'} status
 * @property {string} sessionId
 * @property {number} lastFeedTime  — epoch ms of last accepted feed (rate limit)
 * @property {boolean} alive        — ping/pong liveness flag
 */

/** @type {Map<string, AgentRecord>} */
const agents = new Map();

/**
 * @typedef {Object} DashboardRecord
 * @property {import('ws').WebSocket} ws
 * @property {string} sessionId
 * @property {number} connectedAt
 * @property {boolean} alive
 */

/** @type {Set<DashboardRecord>} */
const dashboards = new Set();

// ─── Console Helpers ──────────────────────────────────────────────────────────

const C = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  red: '\x1b[31m',
  cyan: '\x1b[36m',
  yellow: '\x1b[33m',
  bold: '\x1b[1m',
  dim: '\x1b[2m',
};

function ts() {
  return new Date().toISOString().replace('T', ' ').slice(0, 19);
}

function logConnect(msg) {
  console.log(`${C.green}[${ts()}] ✓ ${msg}${C.reset}`);
}
function logDisconnect(msg) {
  console.log(`${C.red}[${ts()}] ✗ ${msg}${C.reset}`);
}
function logInfo(msg) {
  console.log(`${C.cyan}[${ts()}] ℹ ${msg}${C.reset}`);
}
function logWarn(msg) {
  console.log(`${C.yellow}[${ts()}] ⚠ ${msg}${C.reset}`);
}

// ─── HTTP Server + REST API ──────────────────────────────────────────────────

function setCorsHeaders(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

function sendJson(res, statusCode, data) {
  setCorsHeaders(res);
  res.writeHead(statusCode, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(data));
}

function sendError(res, statusCode, message) {
  sendJson(res, statusCode, { error: message });
}

/**
 * Serve a static file from disk.
 * @param {http.ServerResponse} res
 * @param {string} filePath
 * @param {string} contentType
 */
function serveFile(res, filePath, contentType) {
  try {
    if (!fs.existsSync(filePath)) {
      sendError(res, 404, 'Not found');
      return;
    }
    const data = fs.readFileSync(filePath);
    setCorsHeaders(res);
    res.writeHead(200, { 'Content-Type': contentType });
    res.end(data);
  } catch (err) {
    sendError(res, 500, 'Internal server error');
  }
}

/**
 * Build a plain object summarising one agent (safe to serialise).
 * @param {AgentRecord} a
 */
function agentSummary(a) {
  return {
    agentId: a.agentId,
    teacherName: a.teacherName,
    roomNumber: a.roomNumber,
    status: a.status,
    connectedAt: a.connectedAt,
    lastHeartbeat: a.lastHeartbeat,
    hasScreenshot: !!(a.lastFeed && a.lastFeed.screenshot),
    lastFeedTimestamp: a.lastFeed ? a.lastFeed.timestamp : null,
  };
}

function handleHttpRequest(req, res) {
  try {
    const urlObj = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    const pathname = urlObj.pathname;

    if (req.headers.host && !req.headers.host.includes('localhost') && !req.headers.host.startsWith('192.168.')) {
      knownCloudHost = req.headers.host;
    }

    // Pre-flight
    if (req.method === 'OPTIONS') {
      setCorsHeaders(res);
      res.writeHead(204);
      res.end();
      return;
    }

    // ── REST Endpoints ──────────────────────────────────────────────────

    if (req.method === 'GET' && pathname === '/api/health') {
      sendJson(res, 200, {
        status: 'ok',
        uptime: Math.floor((Date.now() - startTime) / 1000),
        agents: agents.size,
        dashboards: dashboards.size,
        timestamp: Date.now(),
      });
      return;
    }

    if (req.method === 'GET' && pathname === '/api/agents') {
      const list = [];
      for (const a of agents.values()) {
        list.push(agentSummary(a));
      }
      sendJson(res, 200, { agents: list });
      return;
    }

    if (req.method === 'GET' && pathname === '/api/storage') {
      sendJson(res, 200, getStorageStats());
      return;
    }

    // /api/agent/:id/screenshot
    const screenshotMatch = pathname.match(/^\/api\/agent\/([^/]+)\/screenshot$/);
    if (req.method === 'GET' && screenshotMatch) {
      const id = decodeURIComponent(screenshotMatch[1]);
      const agent = agents.get(id);
      if (!agent || !agent.lastFeed || !agent.lastFeed.screenshot) {
        sendError(res, 404, 'No screenshot available');
        return;
      }
      try {
        const raw = agent.lastFeed.screenshot.replace(/^data:image\/\w+;base64,/, '');
        const imgBuf = Buffer.from(raw, 'base64');
        setCorsHeaders(res);
        res.writeHead(200, {
          'Content-Type': 'image/jpeg',
          'Content-Length': imgBuf.length,
          'Cache-Control': 'no-cache',
        });
        res.end(imgBuf);
      } catch {
        sendError(res, 500, 'Failed to decode screenshot');
      }
      return;
    }

    // ── Static file serving: /recordings/ ───────────────────────────────

    if (req.method === 'GET' && pathname.startsWith('/recordings/')) {
      const relative = pathname.slice('/recordings/'.length);
      // Prevent directory traversal
      const safe = path.normalize(relative).replace(/^(\.\.[/\\])+/, '');
      const filePath = path.join(RECORDINGS_ROOT, safe);
      if (!filePath.startsWith(RECORDINGS_ROOT)) {
        sendError(res, 403, 'Forbidden');
        return;
      }
      const ext = path.extname(filePath).toLowerCase();
      const mime = ext === '.jpg' || ext === '.jpeg' ? 'image/jpeg' : 'application/octet-stream';
      serveFile(res, filePath, mime);
      return;
    }

    // ── Dashboard HTML fallback ─────────────────────────────────────────

    if (req.method === 'GET' && (pathname === '/' || pathname === '/index.html')) {
      // Only serve the dashboard HTML for plain HTTP requests, not WebSocket upgrades
      if (fs.existsSync(DASHBOARD_HTML_PATH)) {
        serveFile(res, DASHBOARD_HTML_PATH, 'text/html; charset=utf-8');
      } else {
        sendJson(res, 200, {
          service: 'OMNIWATCH QC-V4 Relay',
          status: 'running',
          ws_agent: `ws://localhost:${PORT}/agent`,
          ws_dashboard: `ws://localhost:${PORT}/dashboard`,
        });
      }
      return;
    }

    sendError(res, 404, 'Not found');
  } catch (err) {
    console.error('[HTTP] Unhandled error:', err);
    try {
      sendError(res, 500, 'Internal server error');
    } catch { /* headers may already be sent */ }
  }
}

const httpServer = http.createServer(handleHttpRequest);

// ─── WebSocket Servers ────────────────────────────────────────────────────────

const wssAgent = new WebSocketServer({ noServer: true });
const wssDashboard = new WebSocketServer({ noServer: true });

// ── Upgrade routing ─────────────────────────────────────────────────────────

httpServer.on('upgrade', (req, socket, head) => {
  try {
    const urlObj = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    const pathname = urlObj.pathname;

    if (pathname === '/agent') {
      wssAgent.handleUpgrade(req, socket, head, (ws) => {
        wssAgent.emit('connection', ws, req);
      });
    } else if (pathname === '/dashboard' || pathname === '/') {
      wssDashboard.handleUpgrade(req, socket, head, (ws) => {
        wssDashboard.emit('connection', ws, req);
      });
    } else {
      socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
      socket.destroy();
    }
  } catch (err) {
    console.error('[UPGRADE] Error during WebSocket upgrade:', err);
    try { socket.destroy(); } catch { /* ignore */ }
  }
});

// ─── Broadcast Utilities ──────────────────────────────────────────────────────

/**
 * Send a JSON payload to every connected dashboard.
 * Silently skips closed sockets.
 */
function broadcastToDashboards(payload) {
  const raw = typeof payload === 'string' ? payload : JSON.stringify(payload);
  const isLargeUpdate = payload && payload.type === 'agent_update' && payload.screenshot;
  for (const d of dashboards) {
    try {
      if (d.ws.readyState === WebSocket.OPEN) {
        // Prevent massive lag: if dashboard internet is slow, drop the frame instead of queueing it for 5 minutes
        if (isLargeUpdate && d.ws.bufferedAmount > 100000) {
          continue; 
        }
        d.ws.send(raw);
      }
    } catch (err) {
      console.error('[BROADCAST] Dashboard send error:', err.message);
    }
  }
}

/**
 * Send a JSON payload to a specific agent.
 */
function sendToAgent(agentId, payload) {
  const agent = agents.get(agentId);
  if (!agent || agent.ws.readyState !== WebSocket.OPEN) return false;
  try {
    agent.ws.send(typeof payload === 'string' ? payload : JSON.stringify(payload));
    return true;
  } catch (err) {
    console.error(`[SEND] Error sending to agent ${agentId}:`, err.message);
    return false;
  }
}

// ─── Agent Connection Handler ─────────────────────────────────────────────────

wssAgent.on('connection', (ws, req) => {
  let agentId = 'UNKNOWN';

  try {
    const urlObj = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    // Accept token from query params (primary) or Authorization header (fallback)
    let token = urlObj.searchParams.get('token') || '';
    if (!token && req.headers.authorization) {
      const authHeader = req.headers.authorization;
      if (authHeader.startsWith('Bearer ')) {
        token = authHeader.slice(7);
      } else {
        token = authHeader;
      }
    }
    agentId = urlObj.searchParams.get('agentId') || req.headers['x-agent-id'] || '';
    const teacherName = urlObj.searchParams.get('teacherName') || req.headers['x-teacher-name'] || '';

    // Auth check
    if (!validateAgentToken(token)) {
      logWarn(`Agent auth failed (invalid token) from ${req.socket.remoteAddress}`);
      ws.close(4001, 'Invalid agent token');
      return;
    }

    if (!agentId) {
      ws.close(4002, 'Missing agentId');
      return;
    }

    // Capacity check
    if (agents.size >= MAX_AGENTS && !agents.has(agentId)) {
      ws.close(4003, 'Agent capacity reached');
      return;
    }

    // If an agent with the same ID is already connected, close the old one
    const existing = agents.get(agentId);
    if (existing && existing.ws.readyState === WebSocket.OPEN) {
      logWarn(`Agent ${agentId} reconnecting — closing stale socket`);
      try { existing.ws.close(4010, 'Replaced by new connection'); } catch { /* ok */ }
    }

    // Parse room number from agentId like "ROOM-131"
    const roomMatch = agentId.match(/(\d+)/);
    const roomNumber = roomMatch ? parseInt(roomMatch[1], 10) : 0;

    const session = createSession('agent', { agentId, teacherName });

    /** @type {AgentRecord} */
    const record = {
      ws,
      agentId,
      teacherName,
      roomNumber,
      connectedAt: Date.now(),
      lastHeartbeat: Date.now(),
      lastFeed: null,
      status: 'active',
      sessionId: session.sessionId,
      lastFeedTime: 0,
      alive: true,
    };

    agents.set(agentId, record);
    logConnect(`Agent ${agentId} connected (${teacherName}) [${session.sessionId}]`);

    // Notify dashboards
    broadcastToDashboards({
      type: 'agent_status',
      agentId,
      teacherName,
      roomNumber,
      status: 'active',
      connectedAt: record.connectedAt,
    });

    // Confirm to agent
    ws.send(JSON.stringify({
      type: 'auth_ok',
      sessionId: session.sessionId,
      serverTime: Date.now(),
    }));

    // ── Message handler ────────────────────────────────────────────────
    ws.on('message', (data, isBinary) => {
      try {
        if (isBinary) {
          // Prepend agentId + null byte to binary JPEG
          const idBuf = Buffer.from(agentId + '\0', 'utf8');
          const outBuf = Buffer.concat([idBuf, data]);

          for (const d of dashboards) {
            try {
              if (d.ws.readyState === WebSocket.OPEN) {
                if (d.ws.bufferedAmount > 2000000) continue; // Drop frame if dashboard is lagging
                d.ws.send(outBuf);
              }
            } catch (err) {
              console.error('[BROADCAST] Binary send error:', err.message);
            }
          }
          return;
        }

        const msg = JSON.parse(data.toString());

        switch (msg.type) {
          case 'agent_feed': {
            // Rate limiting
            const now = Date.now();
            if (now - record.lastFeedTime < FEED_RATE_LIMIT_MS) {
              break; // silently drop
            }
            record.lastFeedTime = now;
            record.lastHeartbeat = now;
            record.status = 'active';

            const feedPayload = {
              type: 'agent_update',
              agentId: record.agentId,
              teacherName: record.teacherName,
              roomNumber: record.roomNumber,
              timestamp: msg.timestamp || now,
              screenshot: msg.screenshot || null,
              idleSeconds: msg.idleSeconds ?? 0,
              activeWindow: msg.activeWindow || '',
              activeApp: msg.activeApp || '',
              keyboardActive: !!msg.keyboardActive,
              mouseActive: !!msg.mouseActive,
              cpuUsage: msg.cpuUsage ?? 0,
              memoryUsage: msg.memoryUsage ?? 0,
              status: 'active',
            };

            record.lastFeed = feedPayload;

            // Optionally save screenshot to disk - disabled for binary
            // if (msg.screenshot && msg.saveRecording) {
            //   saveScreenshot(agentId, msg.screenshot);
            // }

            broadcastToDashboards(feedPayload);
            break;
          }

          case 'heartbeat': {
            record.lastHeartbeat = Date.now();
            record.alive = true;
            ws.send(JSON.stringify({ type: 'heartbeat_ack', serverTime: Date.now() }));
            break;
          }

          default:
            // Forward unknown message types to dashboards as-is with agentId
            broadcastToDashboards({ ...msg, agentId });
            break;
        }
      } catch (err) {
        console.error(`[AGENT] Message parse error from ${agentId}:`, err.message);
      }
    });

    // ── Pong handler (response to our ping) ────────────────────────────
    ws.on('pong', () => {
      record.alive = true;
    });

    // ── Error handler ──────────────────────────────────────────────────
    ws.on('error', (err) => {
      console.error(`[AGENT] Socket error for ${agentId}:`, err.message);
    });

    // ── Close handler ──────────────────────────────────────────────────
    ws.on('close', (code, reason) => {
      logDisconnect(`Agent ${agentId} disconnected (code=${code})`);
      record.status = 'disconnected';
      removeSession(record.sessionId);

      // Keep the record for a while so dashboards can see last state
      // but mark it disconnected. It will be cleaned up later or replaced.
      broadcastToDashboards({
        type: 'agent_status',
        agentId,
        teacherName: record.teacherName,
        roomNumber: record.roomNumber,
        status: 'disconnected',
        disconnectedAt: Date.now(),
      });
    });
  } catch (err) {
    console.error(`[AGENT] Connection handler error for ${agentId}:`, err);
    try { ws.close(4000, 'Server error'); } catch { /* ignore */ }
  }
});

// ─── Dashboard Connection Handler ─────────────────────────────────────────────

wssDashboard.on('connection', (ws, req) => {
  let sessionId = '';

  try {
    const urlObj = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    const token = urlObj.searchParams.get('token') || '';

    // Auth check
    if (!validateDashboardToken(token)) {
      logWarn(`Dashboard auth failed from ${req.socket.remoteAddress}`);
      ws.close(4001, 'Invalid dashboard token');
      return;
    }

    // Capacity check
    if (dashboards.size >= MAX_DASHBOARDS) {
      ws.close(4003, 'Dashboard capacity reached');
      return;
    }

    const session = createSession('dashboard', {
      remoteAddress: req.socket.remoteAddress,
    });
    sessionId = session.sessionId;

    /** @type {DashboardRecord} */
    const record = {
      ws,
      sessionId,
      connectedAt: Date.now(),
      alive: true,
    };

    dashboards.add(record);
    logConnect(`Dashboard connected [${sessionId}] (total: ${dashboards.size})`);

    // Confirm
    ws.send(JSON.stringify({
      type: 'auth_ok',
      sessionId,
      serverTime: Date.now(),
    }));

    // ── Initial sync: send current state of ALL agents ─────────────────
    const initialAgents = [];
    for (const a of agents.values()) {
      if (a.lastFeed) {
        initialAgents.push(a.lastFeed);
      } else {
        initialAgents.push({
          type: 'agent_update',
          agentId: a.agentId,
          teacherName: a.teacherName,
          roomNumber: a.roomNumber,
          status: a.status,
          connectedAt: a.connectedAt,
          lastHeartbeat: a.lastHeartbeat,
          screenshot: null,
          idleSeconds: 0,
          activeWindow: '',
          activeApp: '',
          keyboardActive: false,
          mouseActive: false,
          cpuUsage: 0,
          memoryUsage: 0,
          timestamp: a.connectedAt,
        });
      }
    }

    ws.send(JSON.stringify({
      type: 'initial_sync',
      agents: initialAgents,
      serverTime: Date.now(),
    }));

    // ── Message handler ────────────────────────────────────────────────
    ws.on('message', (data) => {
      try {
        const msg = JSON.parse(data.toString());

        switch (msg.type) {
          case 'command': {
            // Forward a command to a specific agent
            const targetId = msg.agentId;
            if (!targetId) break;

            const sent = sendToAgent(targetId, {
              type: 'command',
              action: msg.action || '',
              payload: msg.payload || {},
              from: sessionId,
              timestamp: Date.now(),
            });

            ws.send(JSON.stringify({
              type: 'command_ack',
              agentId: targetId,
              action: msg.action,
              delivered: sent,
            }));
            break;
          }

          case 'request_all_screenshots': {
            // Ask every connected agent to send a screenshot immediately
            for (const a of agents.values()) {
              if (a.ws.readyState === WebSocket.OPEN) {
                try {
                  a.ws.send(JSON.stringify({
                    type: 'command',
                    action: 'send_screenshot',
                    timestamp: Date.now(),
                  }));
                } catch { /* skip dead sockets */ }
              }
            }
            ws.send(JSON.stringify({ type: 'screenshots_requested', count: agents.size }));
            break;
          }

          case 'get_recordings': {
            const recordings = getRecordings(msg.date || '', msg.agentId || '');
            ws.send(JSON.stringify({
              type: 'recordings_list',
              date: msg.date,
              agentId: msg.agentId || null,
              files: recordings,
            }));
            break;
          }

          default:
            break;
        }
      } catch (err) {
        console.error(`[DASHBOARD] Message parse error [${sessionId}]:`, err.message);
      }
    });

    // ── Pong handler ───────────────────────────────────────────────────
    ws.on('pong', () => {
      record.alive = true;
    });

    // ── Error handler ──────────────────────────────────────────────────
    ws.on('error', (err) => {
      console.error(`[DASHBOARD] Socket error [${sessionId}]:`, err.message);
    });

    // ── Close handler ──────────────────────────────────────────────────
    ws.on('close', () => {
      logDisconnect(`Dashboard disconnected [${sessionId}] (remaining: ${dashboards.size - 1})`);
      dashboards.delete(record);
      removeSession(sessionId);
    });
  } catch (err) {
    console.error(`[DASHBOARD] Connection handler error [${sessionId}]:`, err);
    try { ws.close(4000, 'Server error'); } catch { /* ignore */ }
  }
});

// ─── Heartbeat / Ping-Pong Interval ──────────────────────────────────────────

const heartbeatTimer = setInterval(() => {
  try {
    // Ping agents
    for (const [id, a] of agents) {
      if (a.ws.readyState !== WebSocket.OPEN) continue;

      if (!a.alive) {
        // Missed the last pong — terminate
        logWarn(`Agent ${id} failed heartbeat — terminating`);
        a.status = 'disconnected';
        try { a.ws.terminate(); } catch { /* ok */ }
        continue;
      }

      a.alive = false;
      try { a.ws.ping(); } catch { /* ok */ }
    }

    // Ping dashboards
    for (const d of dashboards) {
      if (d.ws.readyState !== WebSocket.OPEN) continue;

      if (!d.alive) {
        logWarn(`Dashboard [${d.sessionId}] failed heartbeat — terminating`);
        try { d.ws.terminate(); } catch { /* ok */ }
        dashboards.delete(d);
        continue;
      }

      d.alive = false;
      try { d.ws.ping(); } catch { /* ok */ }
    }
  } catch (err) {
    console.error('[HEARTBEAT] Error:', err.message);
  }
}, HEARTBEAT_INTERVAL_MS);

// ─── Stale Agent Cleanup Interval ────────────────────────────────────────────

const cleanupTimer = setInterval(() => {
  try {
    const now = Date.now();

    for (const [id, a] of agents) {
      // If disconnected for over 5 minutes, remove entirely
      if (a.status === 'disconnected' && now - a.lastHeartbeat > 300_000) {
        agents.delete(id);
        logInfo(`Removed stale agent record: ${id}`);
        continue;
      }

      // If no heartbeat for 45s and still marked active, mark as disconnected
      if (a.status === 'active' && now - a.lastHeartbeat > 45_000) {
        a.status = 'disconnected';
        logWarn(`Agent ${id} marked disconnected (no heartbeat for ${Math.round((now - a.lastHeartbeat) / 1000)}s)`);
        broadcastToDashboards({
          type: 'agent_status',
          agentId: id,
          teacherName: a.teacherName,
          roomNumber: a.roomNumber,
          status: 'disconnected',
        });
      }
    }
  } catch (err) {
    console.error('[CLEANUP] Error:', err.message);
  }
}, CLEANUP_INTERVAL_MS);

// Run recording cleanup once at startup and then every 24 hours
try {
  const deleted = cleanOldRecordings(30);
  if (deleted > 0) logInfo(`Cleaned ${deleted} old recording file(s)`);
} catch { /* non-critical */ }

const recordingCleanupTimer = setInterval(() => {
  try {
    const deleted = cleanOldRecordings(30);
    if (deleted > 0) logInfo(`Cleaned ${deleted} old recording file(s)`);
  } catch { /* non-critical */ }
}, 24 * 60 * 60 * 1000);

// ─── Graceful Shutdown ───────────────────────────────────────────────────────

let shuttingDown = false;

function gracefulShutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;

  console.log('');
  logInfo(`${signal} received — shutting down gracefully…`);

  clearInterval(heartbeatTimer);
  clearInterval(cleanupTimer);
  clearInterval(recordingCleanupTimer);

  // Close all agent connections
  for (const [id, a] of agents) {
    try {
      if (a.ws.readyState === WebSocket.OPEN) {
        a.ws.close(1001, 'Server shutting down');
      }
    } catch { /* ignore */ }
  }

  // Close all dashboard connections
  for (const d of dashboards) {
    try {
      if (d.ws.readyState === WebSocket.OPEN) {
        d.ws.close(1001, 'Server shutting down');
      }
    } catch { /* ignore */ }
  }

  // Close WebSocket servers
  wssAgent.close(() => {});
  wssDashboard.close(() => {});

  // Close HTTP server
  httpServer.close(() => {
    logInfo('HTTP server closed');
    logInfo('OMNIWATCH relay server stopped.');
    process.exit(0);
  });

  // Force exit after 5 seconds if graceful close hangs
  setTimeout(() => {
    console.error('[SHUTDOWN] Forced exit after timeout');
    process.exit(1);
  }, 5000).unref();
}

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

// Windows-specific: handle Ctrl+C on Windows
if (process.platform === 'win32') {
  process.on('SIGHUP', () => gracefulShutdown('SIGHUP'));
}

// ─── Cloud Keep-Alive (24/7 Uptime) ──────────────────────────────────────────
setInterval(() => {
  if (knownCloudHost) {
    const pingUrl = `https://${knownCloudHost}/api/health`;
    https.get(pingUrl, (resp) => {
      // Just consume the stream so it doesn't leak memory
      resp.on('data', () => {});
    }).on('error', () => {
      // Ignore errors (e.g. DNS issues or cert issues)
    });
  } else {
    // Fallback to local ping if host isn't known yet to at least keep the process ticking
    http.get(`http://localhost:${PORT}/api/health`, (resp) => {
      resp.on('data', () => {});
    }).on('error', () => {});
  }
}, 10 * 60 * 1000); // 10 minutes

// ─── Startup ──────────────────────────────────────────────────────────────────

httpServer.listen(PORT, '0.0.0.0', () => {
  console.log('');
  console.log(`${C.bold}${C.cyan}  ╔═══════════════════════════════════════════════════╗${C.reset}`);
  console.log(`${C.bold}${C.cyan}  ║                                                   ║${C.reset}`);
  console.log(`${C.bold}${C.cyan}  ║    ██████  ███    ███ ███    ██ ██                 ║${C.reset}`);
  console.log(`${C.bold}${C.cyan}  ║   ██    ██ ████  ████ ████   ██ ██                 ║${C.reset}`);
  console.log(`${C.bold}${C.cyan}  ║   ██    ██ ██ ████ ██ ██ ██  ██ ██                 ║${C.reset}`);
  console.log(`${C.bold}${C.cyan}  ║   ██    ██ ██  ██  ██ ██  ██ ██ ██                 ║${C.reset}`);
  console.log(`${C.bold}${C.cyan}  ║    ██████  ██      ██ ██   ████ ██                 ║${C.reset}`);
  console.log(`${C.bold}${C.cyan}  ║         W A T C H   —   Q C - V 4                 ║${C.reset}`);
  console.log(`${C.bold}${C.cyan}  ║                                                   ║${C.reset}`);
  console.log(`${C.bold}${C.cyan}  ╚═══════════════════════════════════════════════════╝${C.reset}`);
  console.log('');
  logInfo(`HTTP + WebSocket server listening on port ${PORT}`);
  logInfo(`Agent endpoint:     ws://localhost:${PORT}/agent`);
  logInfo(`Dashboard endpoint: ws://localhost:${PORT}/dashboard`);
  logInfo(`REST API:           http://localhost:${PORT}/api/health`);
  logInfo(`Dashboard HTML:     ${fs.existsSync(DASHBOARD_HTML_PATH) ? 'FOUND' : 'not found'} (${DASHBOARD_HTML_PATH})`);
  logInfo(`Recordings dir:     ${RECORDINGS_ROOT}`);
  logInfo(`Max agents: ${MAX_AGENTS} | Max dashboards: ${MAX_DASHBOARDS} | Feed rate limit: ${FEED_RATE_LIMIT_MS / 1000}s`);
  console.log('');
});

httpServer.on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    console.error(`${C.red}[FATAL] Port ${PORT} is already in use. Is another instance running?${C.reset}`);
    process.exit(1);
  }
  console.error(`${C.red}[FATAL] HTTP server error:${C.reset}`, err);
  process.exit(1);
});
