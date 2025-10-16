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
from pipecat.services.openai_realtime_beta import (
    InputAudioNoiseReduction,
    InputAudioTranscription,
    OpenAIRealtimeBetaLLMService,
    SemanticTurnDetection,
    SessionProperties,
)
from pipecat.processors.transcript_processor import TranscriptProcessor



load_dotenv(override=True)

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

async def run_bot(webrtc_connection):
    pipecat_transport = SmallWebRTCTransport(
        webrtc_connection=webrtc_connection,
        params=TransportParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            vad_analyzer=SileroVADAnalyzer(),
            audio_out_10ms_chunks=2,
        ),
    )

    gemini_live_llm = GeminiLiveLLMService(
        api_key='AIzaSyDjrv7MXwtJ9GOc8CZLokXP0hXh1rkDn_M',
        voice_id="Puck",  # Aoede, Charon, Fenrir, Kore, Puck
        transcribe_user_audio=True,
        transcribe_model_audio=True,
        system_instruction=SYSTEM_INSTRUCTION,
    )
    
    openai_realtime_llm = OpenAIRealtimeBetaLLMService(
        api_key='',
        session_properties=session_properties,
        start_audio_paused=False,
    )

    transcript = TranscriptProcessor()

    context = OpenAILLMContext(
        [
            {
                "role": "user",
                "content": "Start by greeting the user warmly and introducing yourself.",
            }
        ],
    )
    
    # context_aggregator = gemini_live_llm.create_context_aggregator(context)
    context_aggregator = openai_realtime_llm.create_context_aggregator(context)

    pipeline = Pipeline(
        [
            pipecat_transport.input(),
            context_aggregator.user(),
            
            
            # gemini_live_llm,
            openai_realtime_llm,
            
            #  transcript.user(),
            
            pipecat_transport.output(),
            
            # transcript.assistant(),
            
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
    
    
    
    