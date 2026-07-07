/**
 * ═══════════════════════════════════════════════════════════════════════════════
 * DeviceLink — Real-Time Signaling Server
 * ═══════════════════════════════════════════════════════════════════════════════
 *
 * The central nervous system of DeviceLink. This server handles:
 *   • Room creation & management (6-digit codes, Agent + Commander pairing)
 *   • WebRTC signaling relay (offer / answer / ICE candidates)
 *   • Real-time data relay (location, stats, sensors, contacts, clipboard)
 *   • Chat messaging with typing indicators & read receipts
 *   • Remote actions (vibrate, sound, notify, camera switch, etc.)
 *   • Per-room activity logging
 *   • Automatic stale-room cleanup
 *
 * Roles:
 *   Agent     — the Android device being monitored (data source)
 *   Commander — the iOS device receiving & controlling (data sink)
 *
 * Every Socket.IO event handler includes validation, error handling,
 * and colourful console logging for easy debugging.
 */

// ─── Environment ─────────────────────────────────────────────────────────────
require('dotenv').config();

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const path = require('path');

// ─── Configuration ───────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const ROOM_CODE_LENGTH = 6;
const MAX_USERS_PER_ROOM = 2; // 1 Agent + 1 Commander
const ROOM_CLEANUP_INTERVAL_MS = 60 * 1000; // check every 60 s
const EMPTY_ROOM_TTL_MS = 5 * 60 * 1000; // remove empty rooms after 5 min
const HEARTBEAT_TIMEOUT_MS = 30 * 1000; // consider user offline after 30 s

// ─── Express & Socket.IO Setup ───────────────────────────────────────────────
const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST'],
  },
  pingInterval: 10000,
  pingTimeout: 20000,
  maxHttpBufferSize: 5e6, // 5 MB — generous for contacts payload
});

// ─── Middleware ───────────────────────────────────────────────────────────────
app.use(cors());
app.use(express.json());

// Serve public static files for test console
app.use(express.static(path.join(__dirname, 'public')));

// Serve mobile static files for development reference
app.use('/mobile', express.static(path.join(__dirname, '..', 'mobile')));

// ─── In-Memory Stores ────────────────────────────────────────────────────────

/**
 * rooms — Map<string, Room>
 *
 * Room shape:
 * {
 *   code:        string,         // 6-digit numeric room code
 *   id:          string,         // UUID
 *   createdAt:   number,         // Date.now()
 *   createdBy:   string,         // socket id of creator
 *   creatorRole: string,         // 'agent' | 'commander'
 *   creatorName: string,         // human-readable device name
 *   users:       Map<socketId, { role, deviceName, joinedAt, lastSeen }>,
 *   activityLog: Array<ActivityEntry>,
 * }
 *
 * ActivityEntry shape:
 * { id, type, description, timestamp, icon }
 */
const rooms = new Map();

/** Reverse lookup: socketId → room code */
const socketToRoom = new Map();

/** Global connection counter (ever-connected, not just current) */
let totalConnectionsServed = 0;

// ═══════════════════════════════════════════════════════════════════════════════
//  HELPERS
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Generate a random numeric code of the specified length.
 * Re-rolls if the code already exists.
 */
function generateRoomCode() {
  let code;
  do {
    code = '';
    for (let i = 0; i < ROOM_CODE_LENGTH; i++) {
      code += Math.floor(Math.random() * 10).toString();
    }
  } while (rooms.has(code));
  return code;
}

/** Colourised console helpers */
const log = {
  info: (emoji, msg) => console.log(`${emoji}  \x1b[36m[INFO]\x1b[0m  ${msg}`),
  success: (emoji, msg) => console.log(`${emoji}  \x1b[32m[OK]\x1b[0m    ${msg}`),
  warn: (emoji, msg) => console.log(`${emoji}  \x1b[33m[WARN]\x1b[0m  ${msg}`),
  error: (emoji, msg) => console.log(`${emoji}  \x1b[31m[ERROR]\x1b[0m ${msg}`),
  event: (emoji, msg) => console.log(`${emoji}  \x1b[35m[EVENT]\x1b[0m ${msg}`),
};

/**
 * Add an activity entry to a room's log and broadcast it.
 */
function addActivity(room, type, description, icon) {
  if (!room) return;
  const entry = {
    id: uuidv4(),
    type,
    description,
    timestamp: Date.now(),
    icon,
  };
  room.activityLog.push(entry);

  // Keep last 200 entries to avoid unbounded growth
  if (room.activityLog.length > 200) {
    room.activityLog = room.activityLog.slice(-200);
  }

  io.to(room.code).emit('activity-log-entry', entry);
  return entry;
}

/**
 * Get the socket of the peer in the same room (the other user).
 * Returns null if no peer is present.
 */
function getPeerSocket(socket) {
  const code = socketToRoom.get(socket.id);
  if (!code) return null;
  const room = rooms.get(code);
  if (!room) return null;

  for (const [sid] of room.users) {
    if (sid !== socket.id) {
      return io.sockets.sockets.get(sid) || null;
    }
  }
  return null;
}

/**
 * Get the room object the socket belongs to (or null).
 */
function getSocketRoom(socket) {
  const code = socketToRoom.get(socket.id);
  return code ? rooms.get(code) : null;
}

/**
 * Get user metadata from a room by socket id.
 */
function getUser(room, socketId) {
  return room ? room.users.get(socketId) : undefined;
}

/**
 * Build a concise snapshot of a room for API responses.
 */
function roomSnapshot(room) {
  const users = [];
  for (const [sid, u] of room.users) {
    users.push({
      socketId: sid,
      role: u.role,
      deviceName: u.deviceName,
      joinedAt: u.joinedAt,
      lastSeen: u.lastSeen,
    });
  }
  return {
    code: room.code,
    id: room.id,
    createdAt: room.createdAt,
    userCount: room.users.size,
    users,
    activityLog: room.activityLog.slice(-50), // last 50 entries
  };
}

/**
 * Safely relay an event from one socket to its peer.
 * Validates that the sender is in a room and has a peer.
 * Returns { room, peer } on success, or null on failure (after emitting error).
 */
function relayToPeer(socket, eventName, data, opts = {}) {
  const room = getSocketRoom(socket);
  if (!room) {
    socket.emit('error-message', { message: 'You are not in a room.' });
    return null;
  }
  const peer = getPeerSocket(socket);
  if (!peer) {
    if (!opts.silent) {
      socket.emit('error-message', { message: 'No peer connected to relay to.' });
    }
    return null;
  }
  peer.emit(eventName, data);
  return { room, peer };
}

// ═══════════════════════════════════════════════════════════════════════════════
//  REST API
// ═══════════════════════════════════════════════════════════════════════════════

/** Health check */
app.get('/health', (_req, res) => {
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    activeRooms: rooms.size,
    connectedSockets: io.engine.clientsCount,
    totalConnectionsServed,
    timestamp: Date.now(),
  });
});

/**
 * POST /api/rooms/create
 * Body: { role: 'agent' | 'commander', deviceName: string }
 * Returns: { code, roomId }
 */
app.post('/api/rooms/create', (req, res) => {
  try {
    const { role, deviceName } = req.body || {};

    // Validate role
    if (!role || !['agent', 'commander'].includes(role)) {
      return res.status(400).json({
        error: 'Invalid role. Must be "agent" or "commander".',
      });
    }

    // Validate deviceName
    if (!deviceName || typeof deviceName !== 'string' || deviceName.trim().length === 0) {
      return res.status(400).json({
        error: 'deviceName is required.',
      });
    }

    const code = generateRoomCode();
    const room = {
      code,
      id: uuidv4(),
      createdAt: Date.now(),
      createdBy: null, // will be set on socket join
      creatorRole: role,
      creatorName: deviceName.trim(),
      users: new Map(),
      activityLog: [],
    };

    rooms.set(code, room);

    log.success('🏠', `Room ${code} created by ${role} "${deviceName}"`);

    addActivity(room, 'room-created', `Room created by ${deviceName} (${role})`, '🏠');

    return res.status(201).json({
      code,
      roomId: room.id,
      createdAt: room.createdAt,
    });
  } catch (err) {
    log.error('💥', `POST /api/rooms/create failed: ${err.message}`);
    return res.status(500).json({ error: 'Internal server error.' });
  }
});

/**
 * POST /api/rooms/join
 * Body: { code: string, role: 'agent' | 'commander', deviceName: string }
 * Returns: room snapshot
 */
app.post('/api/rooms/join', (req, res) => {
  try {
    const { code, role, deviceName } = req.body || {};

    if (!code || typeof code !== 'string') {
      return res.status(400).json({ error: 'Room code is required.' });
    }

    if (!role || !['agent', 'commander'].includes(role)) {
      return res.status(400).json({ error: 'Invalid role. Must be "agent" or "commander".' });
    }

    if (!deviceName || typeof deviceName !== 'string' || deviceName.trim().length === 0) {
      return res.status(400).json({ error: 'deviceName is required.' });
    }

    const room = rooms.get(code.trim());

    if (!room) {
      return res.status(404).json({ error: 'Room not found. Check the code and try again.' });
    }

    if (room.users.size >= MAX_USERS_PER_ROOM) {
      return res.status(409).json({ error: 'Room is full. Maximum 2 users allowed.' });
    }

    // Prevent two users of the same role
    for (const [, u] of room.users) {
      if (u.role === role) {
        return res.status(409).json({
          error: `A ${role} is already in this room. Each room needs one Agent and one Commander.`,
        });
      }
    }

    log.info('🔗', `Room ${code} join validated for ${role} "${deviceName}"`);

    return res.json({
      message: 'Room is available. Connect via Socket.IO to complete join.',
      room: roomSnapshot(room),
    });
  } catch (err) {
    log.error('💥', `POST /api/rooms/join failed: ${err.message}`);
    return res.status(500).json({ error: 'Internal server error.' });
  }
});

/**
 * GET /api/rooms/:code
 * Returns: room status snapshot
 */
app.get('/api/rooms/:code', (req, res) => {
  try {
    const { code } = req.params;
    const room = rooms.get(code);

    if (!room) {
      return res.status(404).json({ error: 'Room not found.' });
    }

    return res.json({ room: roomSnapshot(room) });
  } catch (err) {
    log.error('💥', `GET /api/rooms/${req.params.code} failed: ${err.message}`);
    return res.status(500).json({ error: 'Internal server error.' });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
//  SOCKET.IO — CONNECTION HANDLER
// ═══════════════════════════════════════════════════════════════════════════════

io.on('connection', (socket) => {
  totalConnectionsServed++;
  log.info('⚡', `Socket connected: ${socket.id}  (total now: ${io.engine.clientsCount})`);

  // ─────────────────────────────────────────────────────────────────────────
  //  JOIN ROOM
  // ─────────────────────────────────────────────────────────────────────────
  socket.on('join-room', (data, ack) => {
    try {
      const { code, role, deviceName } = data || {};

      if (!code || !role || !deviceName) {
        const errMsg = 'Missing required fields: code, role, deviceName.';
        log.warn('⚠️', errMsg);
        if (typeof ack === 'function') ack({ success: false, error: errMsg });
        socket.emit('error-message', { message: errMsg });
        return;
      }

      if (!['agent', 'commander'].includes(role)) {
        const errMsg = 'Invalid role. Must be "agent" or "commander".';
        log.warn('⚠️', errMsg);
        if (typeof ack === 'function') ack({ success: false, error: errMsg });
        socket.emit('error-message', { message: errMsg });
        return;
      }

      const room = rooms.get(code);
      if (!room) {
        const errMsg = `Room ${code} does not exist.`;
        log.warn('⚠️', errMsg);
        if (typeof ack === 'function') ack({ success: false, error: errMsg });
        socket.emit('error-message', { message: errMsg });
        return;
      }

      // If this socket is already in a room, leave it first
      const prevCode = socketToRoom.get(socket.id);
      if (prevCode) {
        handleLeaveRoom(socket, 'switching rooms');
      }

      // Capacity check
      if (room.users.size >= MAX_USERS_PER_ROOM) {
        const errMsg = 'Room is full.';
        log.warn('⚠️', errMsg);
        if (typeof ack === 'function') ack({ success: false, error: errMsg });
        socket.emit('error-message', { message: errMsg });
        return;
      }

      // Role conflict check
      for (const [, u] of room.users) {
        if (u.role === role) {
          const errMsg = `A ${role} is already in this room.`;
          log.warn('⚠️', errMsg);
          if (typeof ack === 'function') ack({ success: false, error: errMsg });
          socket.emit('error-message', { message: errMsg });
          return;
        }
      }

      // ── All checks passed — join the room ──
      const now = Date.now();
      room.users.set(socket.id, {
        role,
        deviceName: deviceName.trim(),
        joinedAt: now,
        lastSeen: now,
      });

      // Track creator socket id
      if (room.users.size === 1 && !room.createdBy) {
        room.createdBy = socket.id;
      }

      socketToRoom.set(socket.id, code);
      socket.join(code);

      log.success('🚪', `${role} "${deviceName}" joined room ${code}  (users: ${room.users.size})`);

      addActivity(room, 'user-joined', `${deviceName} (${role}) joined the room`, '🚪');

      // Notify everyone in the room (including the joiner)
      io.to(code).emit('room-update', {
        type: 'user-joined',
        user: { socketId: socket.id, role, deviceName: deviceName.trim() },
        room: roomSnapshot(room),
      });

      // If both users are now present, notify that the pair is complete
      if (room.users.size === MAX_USERS_PER_ROOM) {
        log.success('🤝', `Room ${code} is now fully paired!`);
        addActivity(room, 'room-paired', 'Agent and Commander are connected!', '🤝');
        io.to(code).emit('room-paired', { room: roomSnapshot(room) });
      }

      if (typeof ack === 'function') {
        ack({ success: true, room: roomSnapshot(room) });
      }
    } catch (err) {
      log.error('💥', `join-room error: ${err.message}`);
      socket.emit('error-message', { message: 'Failed to join room.' });
      if (typeof ack === 'function') ack({ success: false, error: 'Server error.' });
    }
  });

  // ─────────────────────────────────────────────────────────────────────────
  //  CREATE ROOM (via Socket — used by the mobile app)
  // ─────────────────────────────────────────────────────────────────────────
  socket.on('create-room', (data, ack) => {
    try {
      const { deviceName, role } = data || {};

      if (!deviceName || typeof deviceName !== 'string' || deviceName.trim().length === 0) {
        const errMsg = 'deviceName is required.';
        log.warn('⚠️', errMsg);
        if (typeof ack === 'function') ack({ success: false, error: errMsg });
        return;
      }

      const actualRole = role || 'agent';

      // If this socket is already in a room, leave it first
      const prevCode = socketToRoom.get(socket.id);
      if (prevCode) {
        handleLeaveRoom(socket, 'creating new room');
      }

      const code = generateRoomCode();
      const now = Date.now();

      const room = {
        code,
        id: uuidv4(),
        createdAt: now,
        createdBy: socket.id,
        creatorRole: actualRole,
        creatorName: deviceName.trim(),
        users: new Map(),
        activityLog: [],
      };

      // Add the creator to the room immediately
      room.users.set(socket.id, {
        role: actualRole,
        deviceName: deviceName.trim(),
        joinedAt: now,
        lastSeen: now,
      });

      rooms.set(code, room);
      socketToRoom.set(socket.id, code);
      socket.join(code);

      log.success('🏠', `Room ${code} created & joined by ${actualRole} "${deviceName}" via socket`);
      addActivity(room, 'room-created', `Room created by ${deviceName} (${actualRole})`, '🏠');

      // Notify the room (just the creator for now)
      io.to(code).emit('room-update', {
        type: 'room-created',
        user: { socketId: socket.id, role: actualRole, deviceName: deviceName.trim() },
        room: roomSnapshot(room),
      });

      if (typeof ack === 'function') {
        ack({ success: true, roomCode: code, roomId: room.id });
      }
    } catch (err) {
      log.error('💥', `create-room error: ${err.message}`);
      if (typeof ack === 'function') ack({ success: false, error: 'Server error.' });
    }
  });

  // ─────────────────────────────────────────────────────────────────────────
  //  LEAVE ROOM
  // ─────────────────────────────────────────────────────────────────────────
  socket.on('leave-room', (data, ack) => {
    try {
      handleLeaveRoom(socket, 'left intentionally');
      if (typeof ack === 'function') ack({ success: true });
    } catch (err) {
      log.error('💥', `leave-room error: ${err.message}`);
      if (typeof ack === 'function') ack({ success: false, error: 'Server error.' });
    }
  });

  // ─────────────────────────────────────────────────────────────────────────
  //  DISCONNECT
  // ─────────────────────────────────────────────────────────────────────────
  socket.on('disconnect', (reason) => {
    log.warn('🔌', `Socket disconnected: ${socket.id}  reason: ${reason}`);
    handleLeaveRoom(socket, `disconnected (${reason})`);
  });

  // ─────────────────────────────────────────────────────────────────────────
  //  HEARTBEAT
  // ─────────────────────────────────────────────────────────────────────────
  socket.on('heartbeat', () => {
    try {
      const room = getSocketRoom(socket);
      if (!room) return;
      const user = getUser(room, socket.id);
      if (user) {
        user.lastSeen = Date.now();
      }
    } catch (err) {
      log.error('💥', `heartbeat error: ${err.message}`);
    }
  });

  // ═════════════════════════════════════════════════════════════════════════
  //  WebRTC SIGNALING
  // ═════════════════════════════════════════════════════════════════════════

  /**
   * webrtc-offer
   * Relay an SDP offer to the peer. `type` can be 'screen' or 'camera' to
   * distinguish multiple peer connections.
   */
  socket.on('webrtc-offer', (data) => {
    try {
      const { sdp, type } = data || {};
      if (!sdp) {
        socket.emit('error-message', { message: 'webrtc-offer: sdp is required.' });
        return;
      }

      const streamType = type || 'camera';
      const result = relayToPeer(socket, 'webrtc-offer', { sdp, type: streamType, from: socket.id });

      if (result) {
        const user = getUser(result.room, socket.id);
        log.event('📡', `WebRTC offer (${streamType}) from ${user?.deviceName || socket.id}`);
        addActivity(result.room, 'webrtc', `WebRTC ${streamType} offer sent`, '📡');
      }
    } catch (err) {
      log.error('💥', `webrtc-offer error: ${err.message}`);
    }
  });

  /**
   * webrtc-answer
   * Relay an SDP answer to the peer.
   */
  socket.on('webrtc-answer', (data) => {
    try {
      const { sdp, type } = data || {};
      if (!sdp) {
        socket.emit('error-message', { message: 'webrtc-answer: sdp is required.' });
        return;
      }

      const streamType = type || 'camera';
      const result = relayToPeer(socket, 'webrtc-answer', { sdp, type: streamType, from: socket.id });

      if (result) {
        const user = getUser(result.room, socket.id);
        log.event('📡', `WebRTC answer (${streamType}) from ${user?.deviceName || socket.id}`);
      }
    } catch (err) {
      log.error('💥', `webrtc-answer error: ${err.message}`);
    }
  });

  /**
   * webrtc-ice-candidate
   * Relay an ICE candidate to the peer.
   */
  socket.on('webrtc-ice-candidate', (data) => {
    try {
      const { candidate, type } = data || {};
      if (!candidate) {
        // Null candidate signals end-of-candidates — still relay it
        relayToPeer(socket, 'webrtc-ice-candidate', { candidate: null, type: type || 'camera', from: socket.id }, { silent: true });
        return;
      }

      const streamType = type || 'camera';
      relayToPeer(socket, 'webrtc-ice-candidate', { candidate, type: streamType, from: socket.id }, { silent: true });
    } catch (err) {
      log.error('💥', `webrtc-ice-candidate error: ${err.message}`);
    }
  });

  // ═════════════════════════════════════════════════════════════════════════
  //  DATA RELAY  (Agent → Commander)
  // ═════════════════════════════════════════════════════════════════════════

  /**
   * location-update
   * Relays GPS data from Agent to Commander.
   */
  socket.on('location-update', (data) => {
    try {
      const { lat, lng } = data || {};
      if (lat === undefined || lng === undefined) {
        socket.emit('error-message', { message: 'location-update: lat and lng are required.' });
        return;
      }

      const payload = {
        lat: Number(lat),
        lng: Number(lng),
        speed: data.speed ?? null,
        altitude: data.altitude ?? null,
        heading: data.heading ?? null,
        accuracy: data.accuracy ?? null,
        timestamp: data.timestamp || Date.now(),
        from: socket.id,
      };

      const result = relayToPeer(socket, 'location-update', payload, { silent: true });

      if (result) {
        // Log sparingly — location updates can be very frequent
        const user = getUser(result.room, socket.id);
        log.event('📍', `Location from ${user?.deviceName || socket.id}: ${lat.toFixed?.(5)}, ${lng.toFixed?.(5)}`);
        addActivity(result.room, 'location', `Location updated (${Number(lat).toFixed(4)}, ${Number(lng).toFixed(4)})`, '📍');
      }
    } catch (err) {
      log.error('💥', `location-update error: ${err.message}`);
    }
  });

  /**
   * device-stats
   * Battery, device info, network, storage, screen dimensions.
   */
  socket.on('device-stats', (data) => {
    try {
      if (!data || typeof data !== 'object') {
        socket.emit('error-message', { message: 'device-stats: payload must be an object.' });
        return;
      }

      const payload = {
        battery: data.battery || null,
        device: data.device || null,
        network: data.network || null,
        storage: data.storage || null,
        screen: data.screen || null,
        timestamp: data.timestamp || Date.now(),
        from: socket.id,
      };

      const result = relayToPeer(socket, 'device-stats', payload, { silent: true });

      if (result) {
        const user = getUser(result.room, socket.id);
        const batteryStr = payload.battery ? `${payload.battery.level}%${payload.battery.isCharging ? '⚡' : ''}` : '??';
        log.event('📊', `Device stats from ${user?.deviceName || socket.id} — battery: ${batteryStr}`);
      }
    } catch (err) {
      log.error('💥', `device-stats error: ${err.message}`);
    }
  });

  /**
   * sensor-data
   * Accelerometer & gyroscope readings.
   */
  socket.on('sensor-data', (data) => {
    try {
      if (!data || typeof data !== 'object') {
        socket.emit('error-message', { message: 'sensor-data: payload must be an object.' });
        return;
      }

      const payload = {
        accelerometer: data.accelerometer || null,
        gyroscope: data.gyroscope || null,
        timestamp: data.timestamp || Date.now(),
        from: socket.id,
      };

      relayToPeer(socket, 'sensor-data', payload, { silent: true });
    } catch (err) {
      log.error('💥', `sensor-data error: ${err.message}`);
    }
  });

  /**
   * contacts-data
   * Full or partial contacts list from Agent.
   */
  socket.on('contacts-data', (data) => {
    try {
      if (!data || !Array.isArray(data.contacts)) {
        socket.emit('error-message', { message: 'contacts-data: contacts array is required.' });
        return;
      }

      const payload = {
        contacts: data.contacts,
        total: data.total ?? data.contacts.length,
        timestamp: data.timestamp || Date.now(),
        from: socket.id,
      };

      const result = relayToPeer(socket, 'contacts-data', payload, { silent: true });

      if (result) {
        const user = getUser(result.room, socket.id);
        log.event('📇', `Contacts from ${user?.deviceName || socket.id}: ${payload.total} contacts`);
        addActivity(result.room, 'contacts', `Shared ${payload.total} contacts`, '📇');
      }
    } catch (err) {
      log.error('💥', `contacts-data error: ${err.message}`);
    }
  });

  /**
   * clipboard-update
   * Clipboard text from Agent.
   */
  socket.on('clipboard-update', (data) => {
    try {
      if (!data || typeof data.text !== 'string') {
        socket.emit('error-message', { message: 'clipboard-update: text string is required.' });
        return;
      }

      const payload = {
        text: data.text,
        timestamp: data.timestamp || Date.now(),
        from: socket.id,
      };

      const result = relayToPeer(socket, 'clipboard-update', payload, { silent: true });

      if (result) {
        const user = getUser(result.room, socket.id);
        const preview = data.text.length > 30 ? data.text.slice(0, 30) + '…' : data.text;
        log.event('📋', `Clipboard from ${user?.deviceName || socket.id}: "${preview}"`);
        addActivity(result.room, 'clipboard', 'Clipboard updated', '📋');
      }
    } catch (err) {
      log.error('💥', `clipboard-update error: ${err.message}`);
    }
  });

  // ═════════════════════════════════════════════════════════════════════════
  //  CHAT
  // ═════════════════════════════════════════════════════════════════════════

  /**
   * chat-message
   * Relay a chat message to peer and broadcast to the room.
   */
  socket.on('chat-message', (data) => {
    try {
      const { text } = data || {};
      if (!text || typeof text !== 'string' || text.trim().length === 0) {
        socket.emit('error-message', { message: 'chat-message: text is required.' });
        return;
      }

      const room = getSocketRoom(socket);
      if (!room) {
        socket.emit('error-message', { message: 'You are not in a room.' });
        return;
      }

      const user = getUser(room, socket.id);
      const message = {
        id: data.id || uuidv4(),
        text: text.trim(),
        senderId: socket.id,
        senderRole: user?.role || 'unknown',
        senderName: user?.deviceName || 'Unknown',
        timestamp: data.timestamp || Date.now(),
      };

      // Broadcast to everyone in the room (including sender for confirmation)
      io.to(room.code).emit('chat-message', message);

      log.event('💬', `Chat in room ${room.code}: [${message.senderName}] ${text.trim().slice(0, 50)}`);
      addActivity(room, 'chat', `${message.senderName}: "${text.trim().slice(0, 40)}"`, '💬');
    } catch (err) {
      log.error('💥', `chat-message error: ${err.message}`);
    }
  });

  /** typing-start — relay to peer */
  socket.on('typing-start', () => {
    try {
      const room = getSocketRoom(socket);
      if (!room) return;
      const user = getUser(room, socket.id);
      relayToPeer(socket, 'typing-start', {
        senderId: socket.id,
        senderName: user?.deviceName || 'Unknown',
      }, { silent: true });
    } catch (err) {
      log.error('💥', `typing-start error: ${err.message}`);
    }
  });

  /** typing-stop — relay to peer */
  socket.on('typing-stop', () => {
    try {
      const room = getSocketRoom(socket);
      if (!room) return;
      const user = getUser(room, socket.id);
      relayToPeer(socket, 'typing-stop', {
        senderId: socket.id,
        senderName: user?.deviceName || 'Unknown',
      }, { silent: true });
    } catch (err) {
      log.error('💥', `typing-stop error: ${err.message}`);
    }
  });

  /** message-read — relay to peer */
  socket.on('message-read', (data) => {
    try {
      const { messageId } = data || {};
      if (!messageId) {
        socket.emit('error-message', { message: 'message-read: messageId is required.' });
        return;
      }
      relayToPeer(socket, 'message-read', {
        messageId,
        readBy: socket.id,
        timestamp: Date.now(),
      }, { silent: true });
    } catch (err) {
      log.error('💥', `message-read error: ${err.message}`);
    }
  });

  // ═════════════════════════════════════════════════════════════════════════
  //  REMOTE ACTIONS  (Commander → Agent)
  // ═════════════════════════════════════════════════════════════════════════

  /** remote-vibrate — Commander tells Agent to vibrate */
  socket.on('remote-vibrate', () => {
    try {
      const result = relayToPeer(socket, 'remote-vibrate', { from: socket.id, timestamp: Date.now() });
      if (result) {
        const user = getUser(result.room, socket.id);
        log.event('📳', `Remote vibrate triggered by ${user?.deviceName || socket.id}`);
        addActivity(result.room, 'remote-action', 'Remote vibrate triggered', '📳');
      }
    } catch (err) {
      log.error('💥', `remote-vibrate error: ${err.message}`);
    }
  });

  /** remote-sound — Commander tells Agent to play a sound */
  socket.on('remote-sound', () => {
    try {
      const result = relayToPeer(socket, 'remote-sound', { from: socket.id, timestamp: Date.now() });
      if (result) {
        const user = getUser(result.room, socket.id);
        log.event('🔊', `Remote sound triggered by ${user?.deviceName || socket.id}`);
        addActivity(result.room, 'remote-action', 'Remote sound triggered', '🔊');
      }
    } catch (err) {
      log.error('💥', `remote-sound error: ${err.message}`);
    }
  });

  /** remote-notify — Commander sends a notification to Agent */
  socket.on('remote-notify', (data) => {
    try {
      const { title, body } = data || {};
      if (!title || !body) {
        socket.emit('error-message', { message: 'remote-notify: title and body are required.' });
        return;
      }

      const result = relayToPeer(socket, 'remote-notify', {
        title,
        body,
        from: socket.id,
        timestamp: Date.now(),
      });

      if (result) {
        const user = getUser(result.room, socket.id);
        log.event('🔔', `Remote notification from ${user?.deviceName || socket.id}: "${title}"`);
        addActivity(result.room, 'remote-action', `Notification sent: "${title}"`, '🔔');
      }
    } catch (err) {
      log.error('💥', `remote-notify error: ${err.message}`);
    }
  });

  /** remote-request — Commander requests Agent to perform an action */
  socket.on('remote-request', (data) => {
    try {
      const { action } = data || {};
      if (!action || typeof action !== 'string') {
        socket.emit('error-message', { message: 'remote-request: action string is required.' });
        return;
      }

      const allowedActions = [
        'switch-camera',
        'refresh-location',
        'toggle-flashlight',
        'capture-screenshot',
        'start-screen-share',
        'stop-screen-share',
        'start-camera',
        'stop-camera',
      ];

      if (!allowedActions.includes(action)) {
        log.warn('⚠️', `Unknown remote-request action: "${action}" — relaying anyway.`);
      }

      const result = relayToPeer(socket, 'remote-request', {
        action,
        from: socket.id,
        timestamp: Date.now(),
      });

      if (result) {
        const user = getUser(result.room, socket.id);
        log.event('🎮', `Remote request from ${user?.deviceName || socket.id}: ${action}`);
        addActivity(result.room, 'remote-action', `Remote request: ${action}`, '🎮');
      }
    } catch (err) {
      log.error('💥', `remote-request error: ${err.message}`);
    }
  });

  // ═════════════════════════════════════════════════════════════════════════
  //  ACTIVITY / STATUS
  // ═════════════════════════════════════════════════════════════════════════

  /**
   * feature-toggle
   * Logs and relays when a feature is enabled/disabled (e.g. 'location', 'camera', 'screen').
   */
  socket.on('feature-toggle', (data) => {
    try {
      const { feature, enabled } = data || {};
      if (!feature || typeof feature !== 'string') {
        socket.emit('error-message', { message: 'feature-toggle: feature name is required.' });
        return;
      }
      if (typeof enabled !== 'boolean') {
        socket.emit('error-message', { message: 'feature-toggle: enabled (boolean) is required.' });
        return;
      }

      const room = getSocketRoom(socket);
      if (!room) {
        socket.emit('error-message', { message: 'You are not in a room.' });
        return;
      }

      const user = getUser(room, socket.id);
      const stateStr = enabled ? 'enabled' : 'disabled';
      const icon = enabled ? '✅' : '❌';

      log.event('🔀', `${user?.deviceName || socket.id} ${stateStr} feature "${feature}"`);
      addActivity(room, 'feature-toggle', `${user?.deviceName || 'User'} ${stateStr} ${feature}`, icon);

      // Relay to peer
      relayToPeer(socket, 'feature-toggle', {
        feature,
        enabled,
        from: socket.id,
        senderName: user?.deviceName || 'Unknown',
        timestamp: Date.now(),
      }, { silent: true });
    } catch (err) {
      log.error('💥', `feature-toggle error: ${err.message}`);
    }
  });

  /**
   * activity-log
   * Client requests the full activity log for their room.
   */
  socket.on('activity-log', (data, ack) => {
    try {
      const room = getSocketRoom(socket);
      if (!room) {
        socket.emit('error-message', { message: 'You are not in a room.' });
        if (typeof ack === 'function') ack({ success: false, error: 'Not in a room.' });
        return;
      }

      const entries = room.activityLog.slice(-100); // last 100
      socket.emit('activity-log', entries);
      if (typeof ack === 'function') ack({ success: true, count: entries.length });
    } catch (err) {
      log.error('💥', `activity-log error: ${err.message}`);
      if (typeof ack === 'function') ack({ success: false, error: 'Server error.' });
    }
  });

  // ─────────────────────────────────────────────────────────────────────────
  //  GENERIC ERROR HANDLER (Socket.IO level)
  // ─────────────────────────────────────────────────────────────────────────
  socket.on('error', (err) => {
    log.error('🔥', `Socket ${socket.id} error: ${err.message}`);
  });
});

// ═══════════════════════════════════════════════════════════════════════════════
//  LEAVE / DISCONNECT HELPER
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Handles a socket leaving its current room — whether intentional or on disconnect.
 */
function handleLeaveRoom(socket, reason) {
  const code = socketToRoom.get(socket.id);
  if (!code) return;

  const room = rooms.get(code);
  if (!room) {
    socketToRoom.delete(socket.id);
    return;
  }

  const user = getUser(room, socket.id);
  const displayName = user?.deviceName || socket.id;
  const role = user?.role || 'unknown';

  // Remove user from the room
  room.users.delete(socket.id);
  socketToRoom.delete(socket.id);
  socket.leave(code);

  log.warn('👋', `${role} "${displayName}" left room ${code} (${reason}) — users remaining: ${room.users.size}`);
  addActivity(room, 'user-left', `${displayName} (${role}) left — ${reason}`, '👋');

  // Notify remaining users
  io.to(code).emit('room-update', {
    type: 'user-left',
    user: { socketId: socket.id, role, deviceName: displayName },
    reason,
    room: roomSnapshot(room),
  });

  // Mark empty room's "emptiedAt" for later cleanup
  if (room.users.size === 0) {
    room.emptiedAt = Date.now();
    log.info('🕳️', `Room ${code} is now empty. Will be cleaned up in ${EMPTY_ROOM_TTL_MS / 1000}s.`);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  PERIODIC CLEANUP
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Every ROOM_CLEANUP_INTERVAL_MS, scan rooms and remove those that have been
 * empty longer than EMPTY_ROOM_TTL_MS.
 */
setInterval(() => {
  const now = Date.now();
  let cleaned = 0;

  for (const [code, room] of rooms) {
    // Remove rooms that have been empty past the TTL
    if (room.users.size === 0 && room.emptiedAt && (now - room.emptiedAt) >= EMPTY_ROOM_TTL_MS) {
      rooms.delete(code);
      cleaned++;
    }
  }

  if (cleaned > 0) {
    log.info('🧹', `Room cleanup: removed ${cleaned} stale room(s). Active rooms: ${rooms.size}`);
  }
}, ROOM_CLEANUP_INTERVAL_MS);

// ═══════════════════════════════════════════════════════════════════════════════
//  START SERVER
// ═══════════════════════════════════════════════════════════════════════════════

server.listen(PORT, () => {
  console.log('');
  console.log('  ╔══════════════════════════════════════════════════════════╗');
  console.log('  ║                                                        ║');
  console.log('  ║   \x1b[36m⚡ DeviceLink Signaling Server\x1b[0m                       ║');
  console.log('  ║                                                        ║');
  console.log(`  ║   🌐  HTTP  → \x1b[32mhttp://localhost:${PORT}\x1b[0m${' '.repeat(21 - String(PORT).length)}║`);
  console.log(`  ║   🔌  WS    → \x1b[32mws://localhost:${PORT}\x1b[0m${' '.repeat(23 - String(PORT).length)}║`);
  console.log('  ║   💚  Health → \x1b[32m/health\x1b[0m                                ║');
  console.log('  ║                                                        ║');
  console.log('  ║   Roles: \x1b[33mAgent\x1b[0m (Android) ↔ \x1b[35mCommander\x1b[0m (iOS)         ║');
  console.log('  ║                                                        ║');
  console.log('  ╚══════════════════════════════════════════════════════════╝');
  console.log('');
});

// ─── Graceful Shutdown ───────────────────────────────────────────────────────
function gracefulShutdown(signal) {
  log.warn('🛑', `${signal} received — shutting down gracefully…`);

  // Close all socket connections
  io.disconnectSockets(true);

  server.close(() => {
    log.info('👋', 'Server closed. Goodbye!');
    process.exit(0);
  });

  // Force-kill if server hasn't closed within 5 seconds
  setTimeout(() => {
    log.error('💀', 'Forced shutdown after timeout.');
    process.exit(1);
  }, 5000);
}

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
