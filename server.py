import argparse
import asyncio
import sys
from contextlib import asynccontextmanager
from typing import Dict

import uvicorn
from bot import run_bot
from appointment_bot import run_appointment_bot
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from loguru import logger
from pipecat.transports.smallwebrtc.connection import IceServer, SmallWebRTCConnection

load_dotenv(override=True)

app = FastAPI()

# Add CORS middleware to allow WebRTC connections
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

pcs_map: Dict[str, SmallWebRTCConnection] = {}


# STUN servers for NAT traversal + TURN servers for connection relay
# TURN is critical when direct p2p connection fails (common in cloud deployments)
ice_servers = [
    # STUN servers (detect public IP and NAT type)
    IceServer(urls="stun:stun.l.google.com:19302"),
    IceServer(urls="stun:stun1.l.google.com:19302"),
    
    # TURN servers (relay connection when direct connection fails)
    # Using free OpenRelay TURN servers - for production, use Twilio or your own
    IceServer(
        urls="turn:openrelay.metered.ca:80",
        username="openrelayproject",
        credential="openrelayproject",
    ),
    IceServer(
        urls="turn:openrelay.metered.ca:443",
        username="openrelayproject",
        credential="openrelayproject",
    ),
    IceServer(
        urls="turn:openrelay.metered.ca:443?transport=tcp",
        username="openrelayproject",
        credential="openrelayproject",
    ),
]


@app.post("/api/offer")
async def offer(request: dict, background_tasks: BackgroundTasks):
    pc_id = request.get("pc_id")
    model = request.get("model", "gemini_live_llm")  # Default to gemini
    voice = request.get("voice", "Puck")  # Default voice
    mode = request.get("mode", "general")  # 'general' or 'appointment'
    language = request.get("language", "en")  # Default to English, supports: en, ar, hi, es, fr
    
    logger.info(f"Received offer request - pc_id: {pc_id}, model: {model}, voice: {voice}, mode: {mode}, language: {language}")

    if pc_id and pc_id in pcs_map:
        pipecat_connection = pcs_map[pc_id]
        logger.info(f"Reusing existing connection for pc_id: {pc_id}")
        await pipecat_connection.renegotiate(sdp=request["sdp"], type=request["type"])
    else:
        logger.info("Creating new WebRTC connection...")
        pipecat_connection = SmallWebRTCConnection(ice_servers)
        await pipecat_connection.initialize(sdp=request["sdp"], type=request["type"])
        logger.info("WebRTC connection initialized")

        @pipecat_connection.event_handler("closed")
        async def handle_disconnected(webrtc_connection: SmallWebRTCConnection):
            logger.info(f"Discarding peer connection for pc_id: {webrtc_connection.pc_id}")
            pcs_map.pop(webrtc_connection.pc_id, None)

        # Route to appropriate bot based on mode
        if mode == "appointment":
            logger.info(f"Starting appointment booking bot with language: {language}")
            background_tasks.add_task(run_appointment_bot, pipecat_connection, voice, language)
        else:
            logger.info(f"Starting general bot with model: {model}, voice: {voice}")
            background_tasks.add_task(run_bot, pipecat_connection, model, voice)

    answer = pipecat_connection.get_answer()
    pcs_map[answer["pc_id"]] = pipecat_connection
    
    logger.info(f"Returning answer for pc_id: {answer['pc_id']}")
    return answer


@app.get("/")
async def serve_index():
    return FileResponse("index.html")


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield  # Run app
    coros = [pc.disconnect() for pc in pcs_map.values()]
    await asyncio.gather(*coros)
    pcs_map.clear()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WebRTC demo")
    parser.add_argument(
        "--host", default="localhost", help="Host for HTTP server (default: localhost)"
    )
    parser.add_argument(
        "--port", type=int, default=5501, help="Port for HTTP server (default: 5501)"
    )
    parser.add_argument("--verbose", "-v", action="count")
    args = parser.parse_args()

    logger.remove(0)
    if args.verbose:
        logger.add(sys.stderr, level="TRACE")
    else:
        logger.add(sys.stderr, level="DEBUG")

    uvicorn.run(app, host=args.host, port=args.port)
