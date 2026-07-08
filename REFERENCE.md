# 📖 MPV Relay Integration & Commands Reference

This document serves as a complete, formal specification of the communication protocols, command schemas, and payload structures for the MPV Relay Go Backend. Use this reference when building voice assistants, custom media remotes, smart-home hubs, or dashboard applications.

---

## 📡 MQTT Topic Map

The backend communicates bi-directionally over the following MQTT topics. Topic paths can be customized in the `.env` configuration file.

| Topic Path | Direction | Role | Description |
| :--- | :--- | :--- | :--- |
| **`mpv/command`** | **Incoming** | Command Receiver | Clients publish JSON command payloads here to control playback or trigger database queries. |
| **`mpv/status`** | **Outgoing** | status Broadcast | Backend publishes the unified player status payload on state changes, track changes, or seek actions. |
| **`device/waveshare/config/get`** | **Outgoing** | Config Query | Published by the backend to request the current hardware parameters from the Waveshare board. |
| **`device/waveshare/config/set`** | **Outgoing** | Config Mutator | Published by the backend to remotely push new configurations (SSID, broker details) to the Waveshare board. |
| **`device/waveshare/config/status`**| **Incoming** | Config status | Waveshare board publishes its current configuration here; backend forwards it to WebSocket clients. |
| **`device/waveshare/gemini/get`** | **Outgoing** | Gemini Query | Published by the backend to request the current Gemini parameters from the Waveshare board. |
| **`device/waveshare/gemini/set`** | **Outgoing** | Gemini Mutator | Published by the backend to remotely push new Gemini parameters (API key, system instructions). |
| **`device/waveshare/gemini/status`**| **Incoming** | Gemini status | Waveshare board publishes its current Gemini credentials and state; backend forwards it to WebSockets. |

---

## 🔌 WebSocket API

The backend exposes a WebSocket server at `ws://<host>:<WS_ADDR>/ws` (default port: `9000`).

- **Incoming Messages**: WebSocket clients can send raw text command strings in JSON format. They are routed directly through the backend's command dispatcher just like MQTT messages.
- **Outgoing Broadcasts**: The WebSocket server broadcasts all player status changes, search results, cached song lists, play history queries, and remote device configuration payloads to all connected clients.

---

## 🗂️ Command Catalog

All commands must be structured as JSON containing a `"cmd"` field, accompanied by any parameters.

### 1. Basic Media Control

#### `play`
Immediately plays a search query or a specific track, clearing any current manual play queue.
- **Payload**:
  ```json
  { "cmd": "play", "query": "<search query or title>", "download": true }
  ```
- **Parameters**:
  - `query` (string, required): Text search query or local cached filename.
  - `download` (boolean, optional, default `true`): If true, initiates background caching to disk during playback.

#### `queue`
Appends a track to the end of the manual play queue.
- **Payload**:
  ```json
  { "cmd": "queue", "query": "<search query or title>", "download": true }
  ```

#### `play_next`
Places a track at the front of the play queue so it plays immediately after the current song finishes.
- **Payload**:
  ```json
  { "cmd": "play_next", "query": "<search query or title>", "download": true }
  ```

#### `pause`
Pauses the current media playback.
- **Payload**: `{ "cmd": "pause" }`

#### `resume`
Resumes the media player.
- **Payload**: `{ "cmd": "resume" }`

#### `stop`
Stops playback, terminates recording, and fully clears the manual queue and autoplay recommendations pool.
- **Payload**: `{ "cmd": "stop" }`

#### `next`
Skips the current track and advances to the next item in the queue or autoplay pool.
- **Payload**: `{ "cmd": "next" }`

#### `previous`
Skips back to the previously played track from the history log.
- **Payload**: `{ "cmd": "previous" }`

#### `seek`
Seeks forward or backward within the current track.
- **Payload**:
  ```json
  { "cmd": "seek", "seconds": -15.0 }
  ```
- **Parameters**:
  - `seconds` (number, required): Seconds relative to current position (use negative numbers to rewind).

#### `volume`
Sets the system audio output volume level.
- **Payload**:
  ```json
  { "cmd": "volume", "level": 85 }
  ```
- **Parameters**:
  - `level` (number, required): Volume percentage clamped between `0` and `150`.

#### `mute`
Toggles the player's mute status.
- **Payload**: `{ "cmd": "mute" }`

#### `shuffle`
Randomizes the ordering of all tracks currently waiting in the manual play queue.
- **Payload**: `{ "cmd": "shuffle" }`

#### `clear`
Empties the manual play queue.
- **Payload**: `{ "cmd": "clear" }`

#### `autoplay`
Enables or disables automatic recommendation filling when the manual queue runs dry.
- **Payload**:
  ```json
  { "cmd": "autoplay", "enabled": true }
  ```

---

### 2. Assistant Coordination Commands
These commands are triggered automatically by the Waveshare board firmware when user microphone capturing is active.

#### `assistant_pause`
Pauses MPV playback due to voice capturing or assistant speech. The backend remembers if the player was active before pausing.
- **Payload**: `{ "cmd": "assistant_pause" }`

#### `assistant_play`
Signals that the assistant voice sequence is complete. The backend resumes playback *only* if music was active before `assistant_pause` was called.
- **Payload**: `{ "cmd": "assistant_play" }`

---

### 3. Query & Configuration Commands

#### `status`
Triggers an immediate publication of the player status to both MQTT (`mpv/status`) and WebSockets.
- **Payload**: `{ "cmd": "status" }`

#### `queue_list`
Requests the current play queue, which will be broadcasted.
- **Payload**: `{ "cmd": "queue_list" }`

#### `history`
Requests the list of the last 20 played tracks, fetched from SQLite history.
- **Payload**: `{ "cmd": "history" }`

#### `download`
Triggers an asynchronous download of a track via `yt-dlp` in the background.
- **Payload**:
  ```json
  { "cmd": "download", "video_id": "HgzGwKwLmgM" }
  ```

#### `search`
Executes a quick YouTube search query and returns the matches.
- **Payload**:
  ```json
  { "cmd": "search", "query": "Queen Bohemian Rhapsody" }
  ```

#### `get_cached_songs`
Requests a paginated list of all songs stored locally on disk.
- **Payload**:
  ```json
  { "cmd": "get_cached_songs", "page": 1, "limit": 20 }
  ```

#### `device_config_get` / `device_config_set`
Retrieves or updates the configuration profiles of the Waveshare hardware.
- **Payload (`device_config_set`)**:
  ```json
  { "cmd": "device_config_set", "content": "{\"wifi_ssid\":\"MyHomeWiFi\",\"wifi_pass\":\"...\"}" }
  ```

#### `gemini_config_get` / `gemini_config_set`
Retrieves or updates the Gemini Live credentials and configurations of the Waveshare hardware.
- **Payload (`gemini_config_set`)**:
  ```json
  { "cmd": "gemini_config_set", "content": "{\"api_key\":\"AIzaSy...\",\"system_instruction\":\"...\"}" }
  ```

---

## 📊 Broadcast Payload Schemas

When the backend publishes updates, they follow these unified schemas.

### 1. Playback status Payload (`"type": "status"`)
Published on topic `mpv/status` and WebSocket connections.
```json
{
  "type": "status",
  "state": "playing",
  "position": 142.0,
  "duration": 258.0,
  "volume": 75,
  "title": "Starboy",
  "uploader": "The Weeknd",
  "thumbnail_url": "https://i.ytimg.com/vi/34Na4j8AVgA/hqdefault.jpg",
  "queue_length": 3,
  "autoplay": true,
  "next_autoplay": {
    "title": "Blinding Lights",
    "video_id": "4NRXx6U8ABQ",
    "cached": true
  },
  "up_next": [
    { "position": 1, "video_id": "34Na4j8AVgA", "title": "Starboy", "source": "queue" },
    { "position": 2, "video_id": "4NRXx6U8ABQ", "title": "Blinding Lights", "source": "autoplay" }
  ]
}
```

### 2. Search Results Payload (`"type": "search_results"`)
Sent via WebSocket in response to a `search` command.
```json
{
  "type": "search_results",
  "query": "lofi hip hop",
  "results": [
    {
      "id": "jfKfPfyJRdk",
      "title": "lofi hip hop radio 📚 beats to relax/study to",
      "uploader": "Lofi Girl",
      "duration": 0
    }
  ]
}
```

### 3. Cached Songs List Payload (`"type": "cached_songs_list"`)
Sent via WebSocket in response to a `get_cached_songs` command.
```json
{
  "type": "cached_songs_list",
  "page": 1,
  "limit": 10,
  "total": 47,
  "has_more": true,
  "items": [
    {
      "query": "lofi study beats",
      "video_id": "jfKfPfyJRdk",
      "title": "beats to relax/study to",
      "uploader": "Lofi Girl",
      "duration": 7200,
      "thumbnail_path": "/home/user/mpv-relay/media/jfKfPfyJRdk.jpg",
      "thumbnail_url": "https://i.ytimg.com/vi/jfKfPfyJRdk/hqdefault.jpg",
      "file_path": "/home/user/mpv-relay/media/jfKfPfyJRdk.mkv",
      "cached": true
    }
  ]
}
```

---

## 🤖 Gemini Live Function Declarations (Tool Calling)

To hook up a Gemini Live LLM (either via Google AI Studio, a Node client, or the Waveshare firmware), configure the model with the following tool declaration array.

### Edge Device Tool declarations
Use the `"assistant"` key of `tools.json` (reduced write-only set for low-resource boards):
```json
[
  {
    "name": "play",
    "description": "Plays a track immediately (clears the queue). Resolves plain text queries. Pass the song title directly.",
    "parameters": {
      "type": "OBJECT",
      "properties": {
        "query": { "type": "STRING", "description": "Plain text search query (e.g. 'Beatles Let It Be'). No URLs." }
      },
      "required": ["query"]
    }
  },
  {
    "name": "queue",
    "description": "Appends a track to the end of the queue.",
    "parameters": {
      "type": "OBJECT",
      "properties": {
        "query": { "type": "STRING", "description": "Plain text search query." }
      },
      "required": ["query"]
    }
  },
  {
    "name": "pause",
    "description": "Pauses current media playback.",
    "parameters": { "type": "OBJECT", "properties": {} }
  },
  {
    "name": "resume",
    "description": "Resumes media playback if paused.",
    "parameters": { "type": "OBJECT", "properties": {} }
  },
  {
    "name": "stop",
    "description": "Stops all playback and clears the queue and recommendations.",
    "parameters": { "type": "OBJECT", "properties": {} }
  },
  {
    "name": "next",
    "description": "Skips the current track.",
    "parameters": { "type": "OBJECT", "properties": {} }
  },
  {
    "name": "seek",
    "description": "Seeks to a position in seconds.",
    "parameters": {
      "type": "OBJECT",
      "properties": {
        "seconds": { "type": "NUMBER", "description": "Position in seconds to seek to." }
      },
      "required": ["seconds"]
    }
  },
  {
    "name": "volume",
    "description": "Adjusts player volume.",
    "parameters": {
      "type": "OBJECT",
      "properties": {
        "level": { "type": "NUMBER", "description": "Volume percentage (0-100)." }
      },
      "required": ["level"]
    }
  }
]
```
When the model outputs a function call response, the client extracts the function `name` and its `args`, formats them as `{"cmd": name, ...args}`, and sends the JSON payload to the `mpv/command` topic.

### Web Client Tool Declarations
For Web clients, the full tool set is located under the `"web"` key of `tools.json` which extends the edge toolset with query commands (`status`, `queue_list`, `history`, `search`, `get_cached_songs`) to allow the LLM to query the server's database state directly.

---

## 💻 Client Integration Examples

### 1. Python Edge Device Integration
This script runs on a microcomputer or edge host, listening to prompts and translating them into MQTT command payloads using Gemini tool calling:

```python
import json
import paho.mqtt.client as mqtt
import google.generativeai as genai

# Configure Gemini
genai.configure(api_key="YOUR_GEMINI_API_KEY")

# Load only the assistant-centric tool definitions from unified config
with open("tools.json", "r") as f:
    assistant_tools = json.load(f)["assistant"]

model = genai.GenerativeModel(
    model_name="gemini-1.5-flash",
    tools=assistant_tools
)

# MQTT Broker Configuration
MQTT_BROKER = "your-cluster.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_USER = "username"
MQTT_PASSWORD = "password"
CMD_TOPIC = "mpv/command"

client = mqtt.Client()
client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
client.tls_set() # Enable TLS for cloud connections

client.connect(MQTT_BROKER, MQTT_PORT)
client.loop_start()

def process_voice_command(prompt: str):
    response = model.generate_content(
        f"You are a media remote. Convert the user prompt into a tool call: {prompt}"
    )
    
    if response.candidates and response.candidates[0].content.parts:
        for part in response.candidates[0].content.parts:
            if hasattr(part, 'function_call') and part.function_call:
                func = part.function_call
                payload = {
                    "cmd": func.name,
                    **func.args
                }
                client.publish(CMD_TOPIC, json.dumps(payload), qos=1)
                print(f"Published Command: {payload}")
                return
    print(f"No tool match. Assistant response: {response.text}")
```

### 2. Node.js Web Client Integration
This script demonstrates how a backend server or a rich client connects to the WebSocket endpoint and MQTT status broker to handle bi-directional media controls:

```javascript
import { GoogleGenAI } from '@google/generative-ai';
import mqtt from 'mqtt';
import fs from 'fs';

// Initialize Gemini and Load Web tools from unified config
const toolsConfig = JSON.parse(fs.readFileSync('tools.json', 'utf8'));
const webTools = toolsConfig.web;
const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });
const model = ai.getGenerativeModel({
  model: 'gemini-1.5-flash',
  tools: [{ functionDeclarations: webTools }]
});

// Connect to HiveMQ MQTT Broker
const client = mqtt.connect('mqtts://your-cluster.s1.eu.hivemq.cloud:8883', {
  username: 'username',
  password: 'password',
  clientId: 'node-client-controller'
});

const CMD_TOPIC = 'mpv/command';
const STATUS_TOPIC = 'mpv/status';

client.on('connect', () => {
  client.subscribe(STATUS_TOPIC);
  // Force a status broadcast on startup
  client.publish(CMD_TOPIC, JSON.stringify({ cmd: 'status' }));
});

client.on('message', (topic, message) => {
  const data = JSON.parse(message.toString());
  if (data.type === 'status') {
    console.log(`Playback Status: ${data.title} is currently [${data.state}]`);
  }
});

async function runPrompt(text) {
  const response = await model.generateContent(text);
  const calls = response.response.functionCalls;
  if (calls && calls.length > 0) {
    const payload = {
      cmd: calls[0].name,
      ...calls[0].args
    };
    client.publish(CMD_TOPIC, JSON.stringify(payload));
  }
}
```

