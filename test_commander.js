const { io } = require('socket.io-client');

const socket = io('http://localhost:3000', {
  transports: ['websocket'],
});

const ROOM_CODE = '464613'; // Current active room code from emulator

socket.on('connect', () => {
  console.log('Connected to server as Commander. Socket ID:', socket.id);

  // Join room
  socket.emit('join-room', {
    code: ROOM_CODE,
    role: 'commander',
    deviceName: 'Test Commander Script',
  }, (ack) => {
    console.log('Join room ACK:', ack);
    
    if (ack && ack.success) {
      console.log('Successfully joined room! Triggering tests...');
      
      // Test 1: Send remote-vibrate
      console.log('Sending remote-vibrate...');
      socket.emit('remote-vibrate', { duration: 1000 });
      
      // Test 2: Send remote-notify after 2 seconds
      setTimeout(() => {
        console.log('Sending remote-notify...');
        socket.emit('remote-notify', { title: 'Test Alert', body: 'Hello from Commander Script!' });
      }, 2000);
      
      // Test 3: Send remote-sound after 4 seconds
      setTimeout(() => {
        console.log('Sending remote-sound...');
        socket.emit('remote-sound');
      }, 4000);

      // Disconnect after 7 seconds
      setTimeout(() => {
        console.log('Test completed. Disconnecting...');
        socket.disconnect();
        process.exit(0);
      }, 7000);
    } else {
      console.error('Failed to join room. Exiting.');
      process.exit(1);
    }
  });
});

socket.on('room-update', (data) => {
  console.log('Room update received:', data);
});

socket.on('device-stats', (data) => {
  console.log('Received device-stats from Agent:', data);
});

socket.on('location-update', (data) => {
  console.log('Received location-update from Agent:', data);
});

socket.on('vibrate-ack', (data) => {
  console.log('Received vibrate-ack from Agent:', data);
});

socket.on('notification-ack', (data) => {
  console.log('Received notification-ack from Agent:', data);
});

socket.on('play-sound-ack', (data) => {
  console.log('Received play-sound-ack from Agent:', data);
});

socket.on('connect_error', (err) => {
  console.error('Connection error:', err);
});
