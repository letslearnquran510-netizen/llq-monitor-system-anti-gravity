/**
 * OMNIWATCH QC-V4 — Authentication Module
 * Handles token validation and session tracking for agents and dashboards.
 */

const VALID_AGENT_TOKEN = 'OMNIWATCH-AGENT-2026';
const VALID_DASHBOARD_TOKEN = 'OMNIWATCH-DASH-2026';

/** @type {Map<string, Session>} Active sessions keyed by sessionId */
const sessions = new Map();

/**
 * Validate an agent connection token.
 * @param {string} token
 * @returns {boolean}
 */
function validateAgentToken(token) {
  if (typeof token !== 'string') return false;
  return token === VALID_AGENT_TOKEN;
}

/**
 * Validate a dashboard connection token.
 * @param {string} token
 * @returns {boolean}
 */
function validateDashboardToken(token) {
  if (typeof token !== 'string') return false;
  return token === VALID_DASHBOARD_TOKEN;
}

/**
 * Generate a session ID in the format AUTH-NNN-XX
 * where NNN is a random 3-digit number and XX is two random uppercase letters.
 * @returns {string}
 */
function generateSessionId() {
  const digits = String(Math.floor(Math.random() * 900) + 100); // 100–999
  const letters = String.fromCharCode(
    65 + Math.floor(Math.random() * 26),
    65 + Math.floor(Math.random() * 26)
  );
  return `AUTH-${digits}-${letters}`;
}

/**
 * Create and register a new session.
 * @param {'agent'|'dashboard'} type
 * @param {object} clientInfo  Arbitrary metadata about the connecting client
 * @returns {{ sessionId: string, createdAt: number, type: string, clientInfo: object }}
 */
function createSession(type, clientInfo) {
  let sessionId = generateSessionId();
  // Avoid (astronomically unlikely) collisions
  while (sessions.has(sessionId)) {
    sessionId = generateSessionId();
  }

  const session = {
    sessionId,
    createdAt: Date.now(),
    type,
    clientInfo: clientInfo || {},
  };

  sessions.set(sessionId, session);
  return session;
}

/**
 * Remove a session by ID.
 * @param {string} sessionId
 */
function removeSession(sessionId) {
  sessions.delete(sessionId);
}

/**
 * Retrieve a session by ID.
 * @param {string} sessionId
 * @returns {object|undefined}
 */
function getSession(sessionId) {
  return sessions.get(sessionId);
}

export {
  VALID_AGENT_TOKEN,
  VALID_DASHBOARD_TOKEN,
  sessions,
  validateAgentToken,
  validateDashboardToken,
  generateSessionId,
  createSession,
  removeSession,
  getSession,
};
