import os
import sys

from dotenv import load_dotenv
from loguru import logger
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import LLMRunFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.openai_llm_context import OpenAILLMContext
from pipecat.services.google.gemini_live.llm import GeminiLiveLLMService
from pipecat.transports.base_transport import TransportParams
from pipecat.transports.smallwebrtc.transport import SmallWebRTCTransport
from pipecat.services.openai_realtime_beta.openai import OpenAIRealtimeBetaLLMService
from pipecat.services.openai_realtime_beta.events import (
    InputAudioNoiseReduction,
    InputAudioTranscription,
    SemanticTurnDetection,
    SessionProperties,
)
from pipecat.processors.transcript_processor import TranscriptProcessor
from pipecat.services.google.gemini_live.llm_vertex import GeminiLiveVertexLLMService



load_dotenv(override=True)
print("OPENAI_API_KEY:", os.getenv("OPENAI_API_KEY"))
print("GOOGLE_API_KEY:", os.getenv("GOOGLE_API_KEY"))
print("GOOGLE_VERTEX_TEST_CREDENTIALS:", os.getenv("GOOGLE_VERTEX_TEST_CREDENTIALS"))
print("GOOGLE_CLOUD_PROJECT_ID:", os.getenv("GOOGLE_CLOUD_PROJECT_ID"))
print("GOOGLE_CLOUD_LOCATION:", os.getenv("GOOGLE_CLOUD_LOCATION"))

SYSTEM_INSTRUCTION = f"""
"You are Zavis Chatbot, a friendly, helpful robot.

Always respond in English, no matter what language the user speaks.

Your goal is to demonstrate your capabilities in a succinct way.

Your output will be converted to audio so don't include special characters in your answers.

Respond to what the user said in a creative and helpful way. Keep your responses brief. One or two sentences at most.
"""

session_properties = SessionProperties(
        input_audio_transcription=InputAudioTranscription(),
        # Set openai TurnDetection parameters. Not setting this at all will turn it
        # on by default
        turn_detection=SemanticTurnDetection(),
        # Or set to False to disable openai turn detection and use transport VAD
        # turn_detection=False,
        input_audio_noise_reduction=InputAudioNoiseReduction(type="near_field"),
        # tools=tools,
        instructions=SYSTEM_INSTRUCTION,
        )

async def run_bot(webrtc_connection, model: str = "gemini_live_llm", voice: str = "Puck"):
    logger.info(f"Starting bot with model={model}, voice={voice}")
    
    pipecat_transport = SmallWebRTCTransport(
        webrtc_connection=webrtc_connection,
        params=TransportParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            vad_analyzer=SileroVADAnalyzer(),
            audio_out_10ms_chunks=2,
        ),
    )

    # Instantiate the correct LLM service based on user selection
    if model == "gemini_live_llm":
        llm_service = GeminiLiveLLMService(
            api_key=os.getenv("GOOGLE_API_KEY") or '',
            voice_id=voice,  # Aoede, Charon, Fenrir, Kore, Puck
            transcribe_user_audio=True,
            transcribe_model_audio=True,
            system_instruction=SYSTEM_INSTRUCTION,
        )
    elif model == "openai_realtime_llm":
        # Update session properties with the selected voice
        session_props = SessionProperties(
            input_audio_transcription=InputAudioTranscription(),
            turn_detection=SemanticTurnDetection(),
            input_audio_noise_reduction=InputAudioNoiseReduction(type="near_field"),
            instructions=SYSTEM_INSTRUCTION,
            voice=voice,  # alloy, echo, shimmer, ash, ballad, coral, sage, verse
        )
        llm_service = OpenAIRealtimeBetaLLMService(
            api_key=os.getenv("OPENAI_API_KEY") or '',
            session_properties=session_props,
            start_audio_paused=False,
        )
    # elif model == "gemini_vertex_llm":
    #     gemini_vertex_llm = GeminiLiveVertexLLMService(
    #     credentials=os.getenv("GOOGLE_VERTEX_TEST_CREDENTIALS") or '',
    #     project_id=os.getenv("GOOGLE_CLOUD_PROJECT_ID") or '',
    #     location=os.getenv("GOOGLE_CLOUD_LOCATION") or '',
    #     system_instruction=SYSTEM_INSTRUCTION,
    #     voice_id="Charon",  # Aoede, Charon, Fenrir, Kore, Puck
    #     # tools=tools,
    # )
    else:
        logger.error(f"Unknown model: {model}")
        raise ValueError(f"Unknown model: {model}")

    transcript = TranscriptProcessor()

    context = OpenAILLMContext(
        [
            {
                "role": "user",
                "content": "Start by greeting the user warmly and introducing yourself.",
            }
        ],
    )
    
    context_aggregator = llm_service.create_context_aggregator(context)

    pipeline = Pipeline(
        [
            pipecat_transport.input(),
            context_aggregator.user(),
            llm_service,
            pipecat_transport.output(),
            context_aggregator.assistant(),
        ]
    )

    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            enable_metrics=True,
            enable_usage_metrics=True,
        ),
    )

    @pipecat_transport.event_handler("on_client_connected")
    async def on_client_connected(transport, client):
        logger.info("Pipecat Client connected")
        # Kick off the conversation.
        await task.queue_frames([LLMRunFrame()])

    @pipecat_transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client):
        logger.info("Pipecat Client disconnected")
        await task.cancel()

    runner = PipelineRunner(handle_sigint=False)

    await runner.run(task)
    
    
    
    