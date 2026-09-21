# NMX Messenger 5.0

Includes:
- responsive desktop/mobile UI
- realtime private chat
- profiles and avatars
- media/files/voice messages
- WebRTC audio/video calls
- Socket.IO signaling
- mute/camera/switch-camera/end-call controls
- incoming call UI
- stories/groups/channels/favorites/pinned toolbar UI

## Render
Build: `npm install`
Start: `npm start`

## Real-world WebRTC connectivity
Calls use WebRTC with Socket.IO signaling. A STUN server is included.
For reliable calls between different NATs/mobile networks, configure a TURN server through Render environment variables:
- `TURN_URL`
- `TURN_USERNAME`
- `TURN_CREDENTIAL`

The app exposes `/api/rtc-config` and automatically includes the TURN server when those variables are set.

Camera/microphone require HTTPS and user permission.
