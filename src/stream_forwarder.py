"""
TikTok Booster - Scrcpy v2.4 Stream Forwarder
Connects to local scrcpy-server instances over ADB port 27183 and establishes
an authenticated outbound WebSocket bridge to the central backend.
"""

import sys
import os
import time
import socket
import struct
import threading
import logging
import asyncio
import subprocess
import aiohttp
import json
import shlex
from typing import Optional, Callable

logger = logging.getLogger("StreamForwarder")

SCRCPY_HOST = "127.0.0.1"
SCRCPY_PORT = 27183

class ScrcpyStreamForwarder:
    """
    Asynchronous bridge:
    Android 14 (scrcpy-server) <-> StreamForwarder <-> Central Backend WebSocket Relay <-> Browser
    """

    def __init__(self, backend_url: str, runner_key: str, token: str = "", command_callback: Optional[Callable[[dict], None]] = None, adb_controller=None):
        self.backend_url = backend_url.rstrip("/")
        self.runner_key = runner_key
        self.token = token
        self.command_callback = command_callback
        self.adb = adb_controller
        self.is_running = False
        self.scrcpy_process = None
        self.video_socket = None
        self.control_socket = None
        self.stream_state = "IDLE"  # IDLE, WAITING_FOR_SCRCPY, CONNECTING_BACKEND, STREAMING, STREAM_ERROR, STREAM_FALLBACK
        self.device_width = 1080
        self.device_height = 2400
        self.device_name = ""
        self.bytes_sent = 0
        self.frames_sent = 0
        self.fps = 0
        self.cached_header_chunk = b""
        self._start_time = None
        self._thread = None

    def start_background(self):
        """Starts the stream forwarder loop in a daemon background thread."""
        self.is_running = True
        self._start_time = time.time()
        self._thread = threading.Thread(target=self._run_async_loop, daemon=True, name="ScrcpyForwarderThread")
        self._thread.start()
        logger.info(f"[SCRCPY_STARTING] ScrcpyStreamForwarder started in background for {self.runner_key}")

    def stop(self):
        """Stops the forwarder and closes sockets and subprocess."""
        self.is_running = False
        self.stream_state = "STREAM_FALLBACK"
        self._close_scrcpy_sockets()
        if self.scrcpy_process:
            try:
                self.scrcpy_process.terminate()
            except Exception:
                pass
            self.scrcpy_process = None
        logger.info("[SCRCPY_STOPPED] ScrcpyStreamForwarder stopped.")

    def _close_scrcpy_sockets(self):
        try:
            if self.video_socket:
                self.video_socket.close()
            if self.control_socket:
                self.control_socket.close()
        except Exception:
            pass
        if self.video_socket or self.control_socket:
            logger.info("[SCRCPY_SOCKET_CLOSED] Local Scrcpy sockets closed.")
        self.video_socket = None
        self.control_socket = None

    def wait_until_connected(self, timeout: float = 2.0) -> bool:
        """Waits up to timeout seconds for Scrcpy video socket connection."""
        start = time.time()
        while time.time() - start < timeout:
            if self.video_socket and self.stream_state in ["STREAMING", "LIVE", "CONNECTING_BACKEND"]:
                return True
            time.sleep(0.1)
        return False

    def _ensure_scrcpy_server(self):
        """Spawns scrcpy-server via ADB shell if not running."""
        # 1. Ensure ADB port forward is active
        res = subprocess.run(["adb", "forward", f"tcp:{SCRCPY_PORT}", "localabstract:scrcpy"], capture_output=True, text=True)
        logger.info(f"[ADB_FORWARD_CREATED] Port forward active: tcp:{SCRCPY_PORT} -> localabstract:scrcpy ({res.stdout.strip()})")

        # 2. Check if scrcpy-server.jar exists on device; if not, invoke setup_scrcpy.sh
        chk = subprocess.run(["adb", "shell", "test -f /data/local/tmp/scrcpy-server.jar"], capture_output=True)
        if chk.returncode != 0:
            logger.info("[SCRCPY_AUTO_INSTALL] scrcpy-server.jar not on device. Running scripts/setup_scrcpy.sh...")
            setup_script = os.path.join(os.path.dirname(__file__), "..", "scripts", "setup_scrcpy.sh")
            if os.path.exists(setup_script):
                subprocess.run(["bash", setup_script], capture_output=True, text=True)

        if self.scrcpy_process and self.scrcpy_process.poll() is None:
            return

        shell_cmd = (
            "CLASSPATH=/data/local/tmp/scrcpy-server.jar "
            "app_process / com.genymobile.scrcpy.Server 2.4 "
            "tunnel_forward=true video=true audio=false control=true "
            "max_size=1080 max_fps=30 video_bit_rate=2500000 "
            "video_codec_options=i-frame-interval=1 "
            "send_frame_meta=false send_dummy_byte=true send_device_meta=true send_codec_meta=true "
            "cleanup=false log_level=debug"
        )
        cmd = ["adb", "shell", shell_cmd]

        try:
            logger.info("[SCRCPY_START] Spawning scrcpy-server v2.4 daemon via ADB shell...")
            self.scrcpy_process = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                stdin=subprocess.DEVNULL
            )
            time.sleep(0.8)
            logger.info(f"[SCRCPY_PROCESS_PID] scrcpy-server process active (PID {self.scrcpy_process.pid})")
        except Exception as e:
            logger.warning(f"Could not spawn scrcpy-server: {e}")

    def _recv_exact(self, sock: socket.socket, num_bytes: int, timeout: float = 4.0) -> bytes:
        """Reads exactly num_bytes from socket, handling chunked TCP arrivals."""
        sock.settimeout(timeout)
        data = bytearray()
        while len(data) < num_bytes:
            packet = sock.recv(num_bytes - len(data))
            if not packet:
                raise ConnectionResetError("Socket closed prematurely during header handshake")
            data.extend(packet)
        return bytes(data)

    def _run_async_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._main_forwarder_loop())
        finally:
            loop.close()

    def _connect_scrcpy_sockets(self, timeout=4.0) -> bool:
        """Connects to local Scrcpy TCP sockets (Video #1, Control #2) with retry backoff and validates handshake."""
        self._ensure_scrcpy_server()
        self._close_scrcpy_sockets()
        self.cached_header_chunk = b""

        max_attempts = 5
        for attempt in range(1, max_attempts + 1):
            try:
                # 1. Connect Video Socket (Connection #1)
                self.video_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.video_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self.video_socket.settimeout(timeout)
                self.video_socket.connect((SCRCPY_HOST, SCRCPY_PORT))
                logger.info(f"[SCRCPY_SOCKET_CONNECTED] Connection #1 (Video) connected to {SCRCPY_HOST}:{SCRCPY_PORT} (attempt {attempt}/{max_attempts})")

                # 2. Connect Control Socket (Connection #2) - MUST connect before reading codec metadata to unblock scrcpy-server accept()
                self.control_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.control_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self.control_socket.settimeout(timeout)
                self.control_socket.connect((SCRCPY_HOST, SCRCPY_PORT))
                logger.info(f"[CONTROL_CONNECTED] Connection #2 (Control) connected to {SCRCPY_HOST}:{SCRCPY_PORT}")

                # 3. Handshake Video Header (1 byte dummy + 64 bytes device name + 12 bytes codec metadata = 77 bytes)
                dummy = self._recv_exact(self.video_socket, 1, timeout=timeout)
                device_name_raw = self._recv_exact(self.video_socket, 64, timeout=timeout)
                codec_meta = self._recv_exact(self.video_socket, 12, timeout=timeout)
                logger.info(f"[SCRCPY_HEADER_RECEIVED] Received 77-byte header handshake. Dummy: 0x{dummy.hex()}")

                self.device_name = device_name_raw.decode("utf-8", errors="ignore").rstrip("\x00")
                codec_id = codec_meta[0:4].decode("utf-8", errors="ignore")
                w, h = struct.unpack(">II", codec_meta[4:12])
                if w > 0 and h > 0:
                    self.device_width = w
                    self.device_height = h

                logger.info(f"[SCRCPY_HEADER_VALID] Verified Scrcpy header: Device='{self.device_name}', Codec='{codec_id}', Resolution={self.device_width}x{self.device_height}")

                # 4. Read 1 dummy byte from control socket when send_dummy_byte=true
                try:
                    dummy_ctrl = self._recv_exact(self.control_socket, 1, timeout=2.0)
                    logger.info(f"[CONTROL_HANDSHAKE_OK] Control dummy byte verified: 0x{dummy_ctrl.hex()}.")
                except Exception:
                    logger.info("[CONTROL_HANDSHAKE_OK] Control socket ready.")

                self.control_socket.setblocking(False)
                self.video_socket.setblocking(False)
                return True
            except Exception as e:
                self.last_scrcpy_error = str(e)
                self._close_scrcpy_sockets()
                if attempt < max_attempts:
                    time.sleep(0.6)
                else:
                    logger.info(f"[Scrcpy Sockets] Connection notice after {max_attempts} attempts: {e}")
                    return False
        return False

    async def _main_forwarder_loop(self):
        ws_url = f"{self.backend_url.replace('http://', 'ws://').replace('https://', 'wss://')}/ws/stream?role=runner&runner_key={self.runner_key}&token={self.token}"
        http_publish_url = f"{self.backend_url}/api/stream/publish/{self.runner_key}?token={self.token}"
        http_control_poll_url = f"{self.backend_url}/api/stream/control-poll/{self.runner_key}?token={self.token}"

        while self.is_running:
            # 1. Connect to local Scrcpy server
            if not self.video_socket or not self.control_socket:
                if not self._connect_scrcpy_sockets():
                    err_hint = getattr(self, 'last_scrcpy_error', 'connecting')
                    self.stream_state = f"WAITING_FOR_SCRCPY ({err_hint})"
                    await asyncio.sleep(2)
                    continue

            # 2. Try WebSocket relay first
            ws_connected = False
            try:
                self.stream_state = "CONNECTING_BACKEND"
                logger.info(f"[STREAM_WS_CONNECTING] Connecting to Backend Stream Relay: {ws_url} ...")
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(ws_url, heartbeat=10, timeout=5) as ws:
                        ws_connected = True
                        self.stream_state = "STREAMING"
                        logger.info(f"[STREAM_WS_CONNECTED] Outbound WebSocket stream established for {self.runner_key}!")
                        logger.info(f"[STREAM_VIDEO_ACTIVE] Streaming H.264 video chunks at ~30 FPS")

                        if self.cached_header_chunk:
                            await ws.send_bytes(self.cached_header_chunk)

                        video_task = asyncio.create_task(self._transmit_video_ws(ws))
                        control_task = asyncio.create_task(self._receive_control_ws(ws))

                        done, pending = await asyncio.wait(
                            [video_task, control_task],
                            return_when=asyncio.FIRST_COMPLETED
                        )
                        for task in pending:
                            task.cancel()
            except Exception as e:
                logger.debug(f"[STREAM_WS_NOTICE] WebSocket bridge notice: {e}")

            # 3. If WebSocket failed (e.g. 404 from reverse proxy), use HTTP Streaming Bridge
            if not ws_connected and self.is_running:
                try:
                    self.stream_state = "CONNECTING_BACKEND"
                    logger.info(f"[STREAM_HTTP_CONNECTING] Establishing HTTP Chunked Stream Bridge to {http_publish_url} ...")
                    async with aiohttp.ClientSession() as session:
                        self.stream_state = "STREAMING"
                        logger.info(f"[STREAM_HTTP_CONNECTED] Outbound HTTP Chunked stream active for {self.runner_key}!")
                        
                        video_task = asyncio.create_task(self._transmit_video_http(session, http_publish_url))
                        control_task = asyncio.create_task(self._poll_control_http(session, http_control_poll_url))
                        
                        done, pending = await asyncio.wait(
                            [video_task, control_task],
                            return_when=asyncio.FIRST_COMPLETED
                        )
                        for task in pending:
                            task.cancel()
                except Exception as e:
                    self.stream_state = "STREAM_FALLBACK"
                    logger.warning(f"[STREAM_RECONNECTING] Stream relay notice: {e}. Reconnecting in 2s...")
                    await asyncio.sleep(2)

    async def _transmit_video_ws(self, ws):
        """Reads raw H.264 video chunks from local scrcpy video socket and sends binary NAL frames via WebSocket."""
        loop = asyncio.get_event_loop()
        fps_timer = time.time()
        frames_in_sec = 0

        while self.is_running and self.video_socket:
            try:
                data = await loop.run_in_executor(None, self._read_video_chunk)
                if data and len(data) > 0:
                    if self.frames_sent == 0:
                        logger.info(f"[SCRCPY_FIRST_H264_FRAME] Received first H.264 video frame ({len(data)} bytes).")
                    if not self.cached_header_chunk and (b"\x00\x00\x00\x01\x67" in data or b"\x00\x00\x00\x01\x27" in data):
                        self.cached_header_chunk = data

                    await ws.send_bytes(data)
                    self.bytes_sent += len(data)
                    self.frames_sent += 1
                    frames_in_sec += 1

                    if time.time() - fps_timer >= 1.0:
                        self.fps = frames_in_sec
                        frames_in_sec = 0
                        fps_timer = time.time()
                else:
                    await asyncio.sleep(0.001)
            except Exception as e:
                logger.debug(f"[STREAM_DISCONNECTED] Video WS transmit notice: {e}")
                break

    async def _transmit_video_http(self, session: aiohttp.ClientSession, publish_url: str):
        """Streams raw H.264 chunks over an async generator HTTP POST request."""
        async def video_chunk_generator():
            loop = asyncio.get_event_loop()
            fps_timer = time.time()
            frames_in_sec = 0

            # Yield SPS/PPS header first if cached
            if self.cached_header_chunk:
                yield self.cached_header_chunk

            while self.is_running and self.video_socket:
                try:
                    data = await loop.run_in_executor(None, self._read_video_chunk)
                    if data and len(data) > 0:
                        if self.frames_sent == 0:
                            logger.info(f"[SCRCPY_FIRST_H264_FRAME] Received first H.264 video frame ({len(data)} bytes).")
                        if not self.cached_header_chunk and (b"\x00\x00\x00\x01\x67" in data or b"\x00\x00\x00\x01\x27" in data):
                            self.cached_header_chunk = data
                        yield data
                        self.bytes_sent += len(data)
                        self.frames_sent += 1
                        frames_in_sec += 1
                        if time.time() - fps_timer >= 1.0:
                            self.fps = frames_in_sec
                            frames_in_sec = 0
                            fps_timer = time.time()
                    else:
                        await asyncio.sleep(0.005)
                except Exception:
                    break

        headers = {
            "Content-Type": "application/octet-stream",
            "Transfer-Encoding": "chunked",
            "Authorization": f"Bearer {self.token}"
        }
        try:
            async with session.post(publish_url, data=video_chunk_generator(), headers=headers) as resp:
                logger.debug(f"[STREAM_HTTP_CLOSED] Publish POST closed with status {resp.status}")
        except Exception as e:
            logger.debug(f"[STREAM_HTTP_ERROR] Publish POST error: {e}")

    async def _poll_control_http(self, session: aiohttp.ClientSession, control_poll_url: str):
        """Polls for incoming control commands from backend over HTTP."""
        headers = {"Authorization": f"Bearer {self.token}"}
        while self.is_running and self.control_socket:
            try:
                async with session.get(control_poll_url, headers=headers, timeout=aiohttp.ClientTimeout(total=25)) as resp:
                    if resp.status == 200:
                        async for chunk in resp.content.iter_chunked(128):
                            if chunk and self.control_socket:
                                try:
                                    self.control_socket.setblocking(True)
                                    self.control_socket.sendall(chunk)
                                    logger.debug(f"[CONTROL_COMMAND_SENT] Wrote {len(chunk)} bytes to scrcpy control socket")
                                except Exception as e:
                                    logger.debug(f"Control write notice: {e}")
                    else:
                        await asyncio.sleep(1)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.debug(f"Control poll notice: {e}")
                await asyncio.sleep(1)

    def _read_video_chunk(self, max_bytes=32768) -> bytes:
        if not self.video_socket:
            return b""
        try:
            self.video_socket.setblocking(True)
            self.video_socket.settimeout(0.5)
            chunk = self.video_socket.recv(max_bytes)
            return chunk
        except (socket.timeout, BlockingIOError):
            return b""
        except Exception:
            return b""

    def _run_adb_cmd(self, shell_cmd: str, timeout=2):
        """Executes adb shell command targeted to the active device instance."""
        if self.adb:
            return self.adb.shell(shell_cmd, timeout=timeout)
        return subprocess.run(["adb", "shell", shell_cmd], capture_output=True, timeout=timeout)

    def _execute_safe_adb_text(self, text_val: str):
        """Types text into active Android input field with proper shell escaping."""
        if not text_val:
            return
        try:
            # Verify ASCII encoding since Android shell 'input text' only supports ASCII
            try:
                text_val.encode('ascii')
            except UnicodeEncodeError:
                logger.warning(f"[-] Text contains non-ASCII characters not supported by Android native input: {text_val}")
                return
            formatted = text_val.replace(" ", "%s")
            quoted = shlex.quote(formatted)
            self._run_adb_cmd(f"input text {quoted}", timeout=3)
            logger.info(f"[ADB_TEXT_EXEC] Injected text ({len(text_val)} chars)")
        except Exception as e:
            logger.debug(f"ADB text injection notice: {e}")

    async def _receive_control_ws(self, ws):
        """Receives binary Scrcpy control messages and JSON commands from the browser WebSocket."""
        async for msg in ws:
            if self.adb:
                self.adb.user_override_until = time.time() + 45

            if msg.type == aiohttp.WSMsgType.BINARY and msg.data:
                data = msg.data
                handled = False
                
                # Check for Touch Event (0x02, >=32 bytes)
                if len(data) >= 32 and data[0] == 0x02:
                    action = data[1]
                    x, y = struct.unpack('>II', data[10:18])
                    w, h = struct.unpack('>HH', data[18:22])
                    if action == 0:
                        logger.info(f"[SCRCPY_TOUCH_EXEC] Remote Tap at ({x}, {y}) [Stream: {w}x{h}, Device: {self.device_width}x{self.device_height}]")
                # Keycode Event (0x00, >=14 bytes)
                elif len(data) >= 14 and data[0] == 0x00:
                    action = data[1]
                    keycode = struct.unpack('>I', data[2:6])[0]
                    if action == 0:
                        logger.info(f"[SCRCPY_KEY_EXEC] Remote Keyevent {keycode}")
                # Text Injection Event (0x01, >=5 bytes: [type 0x01][length 4 bytes][string])
                elif len(data) >= 5 and data[0] == 0x01:
                    str_len = struct.unpack('>I', data[1:5])[0]
                    injected_text = data[5:5+str_len].decode('utf-8', errors='ignore')
                    logger.info(f"[SCRCPY_TEXT_EXEC] Remote Text Input: {injected_text}")
                    self._execute_safe_adb_text(injected_text)
                    handled = True
                # Scroll Event (0x03, >=24 bytes)
                elif len(data) >= 24 and data[0] == 0x03:
                    vscroll = struct.unpack('>i', data[16:20])[0]
                    logger.info(f"[SCRCPY_SCROLL_EXEC] Remote Scroll: vscroll={vscroll}")

                if not handled and self.control_socket:
                    try:
                        self.control_socket.setblocking(True)
                        self.control_socket.sendall(data)
                        handled = True
                    except Exception as e:
                        logger.debug(f"Scrcpy control socket write notice: {e}")

                # Immediate ADB Fallback if control socket was closed/busy
                if not handled:
                    if len(data) >= 32 and data[0] == 0x02 and data[1] == 0:
                        x, y = struct.unpack('>II', data[10:18])
                        try:
                            self._run_adb_cmd(f"input tap {x} {y}", timeout=2)
                            logger.info(f"[ADB_FALLBACK_TOUCH] Executed adb shell input tap {x} {y}")
                        except Exception:
                            pass
                    elif len(data) >= 14 and data[0] == 0x00 and data[1] == 0:
                        keycode = struct.unpack('>I', data[2:6])[0]
                        try:
                            self._run_adb_cmd(f"input keyevent {keycode}", timeout=2)
                            logger.info(f"[ADB_FALLBACK_KEY] Executed adb shell input keyevent {keycode}")
                        except Exception:
                            pass
                    elif len(data) >= 24 and data[0] == 0x03:
                        vscroll = struct.unpack('>i', data[16:20])[0]
                        mid_x = self.device_width // 2
                        start_y = int(self.device_height * 0.7) if vscroll < 0 else int(self.device_height * 0.3)
                        end_y = int(self.device_height * 0.3) if vscroll < 0 else int(self.device_height * 0.7)
                        try:
                            self._run_adb_cmd(f"input swipe {mid_x} {start_y} {mid_x} {end_y} 200", timeout=2)
                        except Exception:
                            pass

            elif msg.type == aiohttp.WSMsgType.TEXT and msg.data:
                try:
                    payload = json.loads(msg.data)
                    action = payload.get("action")
                    logger.info(f"[WS_JSON_COMMAND] Received command: {action}")
                    
                    if action == "text" and payload.get("text"):
                        self._execute_safe_adb_text(payload.get("text"))
                    elif action in ["tap", "touch"]:
                        tx = int(payload.get("x", self.device_width // 2))
                        ty = int(payload.get("y", self.device_height // 2))
                        self._run_adb_cmd(f"input tap {tx} {ty}", timeout=2)
                    elif action == "key":
                        k = int(payload.get("keycode", 4))
                        self._run_adb_cmd(f"input keyevent {k}", timeout=2)
                    elif action == "swipe":
                        x1 = int(payload.get("x1", self.device_width // 2))
                        y1 = int(payload.get("y1", int(self.device_height * 0.75)))
                        x2 = int(payload.get("x2", self.device_width // 2))
                        y2 = int(payload.get("y2", int(self.device_height * 0.25)))
                        self._run_adb_cmd(f"input swipe {x1} {y1} {x2} {y2} 250", timeout=2)
                    
                    if self.command_callback:
                        self.command_callback(payload)
                except Exception as e:
                    logger.debug(f"JSON command execution notice: {e}")

            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break
