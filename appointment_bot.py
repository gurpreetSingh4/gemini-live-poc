import os
from datetime import datetime
from typing import Dict, List, Optional

from dotenv import load_dotenv
from loguru import logger

from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import LLMRunFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.openai_llm_context import OpenAILLMContext
from pipecat.processors.transcript_processor import TranscriptProcessor
from pipecat.services.google.gemini_live.llm_vertex import GeminiLiveVertexLLMService
from pipecat.services.llm_service import FunctionCallParams
from pipecat.transports.base_transport import TransportParams
from pipecat.transports.smallwebrtc.transport import SmallWebRTCTransport

load_dotenv(override=True)

# Mock database for demonstration
# In production, replace with actual database calls
MOCK_CLIENTS_DB = {}
MOCK_APPOINTMENTS_DB = []
MOCK_DOCTORS_DB = [
    {"id": "dr_001", "name": "Dr. Sarah Johnson", "specialty": "General Medicine"},
    {"id": "dr_002", "name": "Dr. Michael Chen", "specialty": "Cardiology"},
    {"id": "dr_003", "name": "Dr. Emily Rodriguez", "specialty": "Dermatology"},
]
MOCK_AVAILABLE_SLOTS = {
    "dr_001": ["09:00", "10:00", "11:00", "14:00", "15:00", "16:00"],
    "dr_002": ["09:30", "11:00", "13:00", "15:00"],
    "dr_003": ["10:00", "11:30", "14:00", "16:00"],
}

# Tool functions for appointment booking


async def check_client_existence(params: FunctionCallParams):
    """Check if a client exists by phone number"""
    phone = params.arguments.get("phone")
    logger.info(f"Checking client existence for phone: {phone}")
    
    # Mock implementation - replace with actual database query
    client = MOCK_CLIENTS_DB.get(phone)
    
    if client:
        await params.result_callback({
            "exists": True,
            "client_data": client
        })
    else:
        await params.result_callback({
            "exists": False,
            "message": "Client not found. Please provide registration details."
        })


async def get_doctor_list(params: FunctionCallParams):
    """Get list of available doctors"""
    logger.info("Fetching doctor list")
    
    # Mock implementation - replace with actual database query
    await params.result_callback({
        "doctors": MOCK_DOCTORS_DB
    })


async def get_available_slots(params: FunctionCallParams):
    """Get available appointment slots for a doctor on a specific date"""
    doctor_id = params.arguments.get("doctor_id")
    date = params.arguments.get("date")
    duration = params.arguments.get("duration", 30)
    
    # Validate duration
    valid_durations = [15, 30, 45, 60]
    if duration not in valid_durations:
        logger.warning(f"Invalid duration {duration}, defaulting to 30 minutes")
        duration = 30
    
    logger.info(f"Fetching available slots for doctor: {doctor_id}, date: {date}, duration: {duration}")
    
    # Mock implementation - replace with actual database query
    slots: List[str] = []
    if doctor_id and isinstance(doctor_id, str):
        slots = MOCK_AVAILABLE_SLOTS.get(doctor_id, [])
    
    await params.result_callback({
        "doctor_id": doctor_id,
        "date": date,
        "duration": duration,
        "available_slots": slots
    })


async def book_appointment(params: FunctionCallParams):
    """Book a new appointment"""
    doctor_id = params.arguments.get("doctor_id")
    doctor_name = params.arguments.get("doctor_name")
    date = params.arguments.get("date")
    timeslot = params.arguments.get("timeslot")
    duration = params.arguments.get("duration")
    
    # Client information
    full_name = params.arguments.get("full_name")
    dob = params.arguments.get("dob")
    gender = params.arguments.get("gender")
    email = params.arguments.get("email")
    phone = params.arguments.get("phone")
    phone_country_code = params.arguments.get("phone_country_code", "+1")
    complaint = params.arguments.get("complaint", "")
    
    logger.info(f"Booking appointment for {full_name} with {doctor_name} on {date} at {timeslot}")
    
    # Create appointment ID
    appointment_id = f"apt_{len(MOCK_APPOINTMENTS_DB) + 1:04d}"
    
    # Store client if new
    if phone not in MOCK_CLIENTS_DB:
        MOCK_CLIENTS_DB[phone] = {
            "full_name": full_name,
            "dob": dob,
            "gender": gender,
            "email": email,
            "phone": phone,
            "phone_country_code": phone_country_code
        }
    
    # Create appointment
    appointment = {
        "id": appointment_id,
        "doctor_id": doctor_id,
        "doctor_name": doctor_name,
        "date": date,
        "timeslot": timeslot,
        "duration": duration,
        "status": "booked",
        "client_phone": phone,
        "client_name": full_name,
        "complaint": complaint,
        "created_at": datetime.now().isoformat()
    }
    
    MOCK_APPOINTMENTS_DB.append(appointment)
    
    await params.result_callback({
        "success": True,
        "appointment_id": appointment_id,
        "message": f"Appointment successfully booked with {doctor_name} on {date} at {timeslot}",
        "appointment_details": appointment
    })


async def get_client_appointments(params: FunctionCallParams):
    """Get all appointments for a client by phone number"""
    phone = params.arguments.get("phone")
    
    logger.info(f"Fetching appointments for phone: {phone}")
    
    # Mock implementation - replace with actual database query
    appointments = [apt for apt in MOCK_APPOINTMENTS_DB if apt.get("client_phone") == phone]
    
    # Filter out cancelled appointments or include all based on requirements
    upcoming_appointments = [apt for apt in appointments if apt.get("status") != "cancelled"]
    
    await params.result_callback({
        "phone": phone,
        "appointments": upcoming_appointments,
        "count": len(upcoming_appointments)
    })


async def update_appointment(params: FunctionCallParams):
    """Update an existing appointment (reschedule or cancel)"""
    appointment_id = params.arguments.get("appointment_id")
    status = params.arguments.get("status")
    timeslot = params.arguments.get("timeslot")
    duration = params.arguments.get("duration")
    
    logger.info(f"Updating appointment {appointment_id}: status={status}, timeslot={timeslot}, duration={duration}")
    
    # Find appointment
    appointment = None
    for apt in MOCK_APPOINTMENTS_DB:
        if apt.get("id") == appointment_id:
            appointment = apt
            break
    
    if not appointment:
        await params.result_callback({
            "success": False,
            "message": "Appointment not found"
        })
        return
    
    # Update appointment
    if status:
        appointment["status"] = status
    if timeslot:
        appointment["timeslot"] = timeslot
    if duration:
        appointment["duration"] = duration
    
    appointment["updated_at"] = datetime.now().isoformat()
    
    await params.result_callback({
        "success": True,
        "message": "Appointment updated successfully",
        "appointment_details": appointment
    })


# System instruction for appointment booking assistant
APPOINTMENT_SYSTEM_INSTRUCTION = """
You are a friendly and professional appointment booking assistant for a medical clinic.

Your name is Zavis Appointment Assistant.

Your primary responsibilities are:
1. Help users book new appointments
2. View and manage existing appointments
3. Reschedule or cancel appointments
4. Verify client information

Guidelines:
- Always be warm, friendly, and professional like a clinic receptionist
- Keep responses brief and clear (1-2 sentences)
- Use emojis sparingly: 👋 ✅ 🌿
- Never assume or invent information
- Always confirm details before proceeding
- If you don't understand, ask for clarification

Workflow Overview:
1. **View/Manage Appointments**: If user wants to view, reschedule, or cancel
   - Use @client_appointments to fetch their appointments
   - Display appointments with all details
   - Help them update or cancel as needed

2. **Book New Appointment**:
   - Show available doctors using @doctor_list
   - Get date preference from user
   - Ask for appointment duration (15, 30, 45, or 60 minutes)
   - Show available slots using @available_slots
   - Verify client with @client_existence_check
   - If client exists, confirm their details
   - If new client, collect: name, DOB (YYYY-MM-DD), gender, email, phone
   - Collect complaint/remarks
   - Book using @appointment_booking_tool

3. **Update Appointment**:
   - For cancellation: directly update with status="cancel"
   - For rescheduling: fetch new slots, then update with new timeslot/duration

Important: Always respond in English, keep it conversational, and guide users step by step.
"""


async def run_appointment_bot(webrtc_connection, voice: str = "Puck"):
    """Run the appointment booking bot"""
    logger.info(f"Starting appointment bot with voice={voice}")
    
    pipecat_transport = SmallWebRTCTransport(
        webrtc_connection=webrtc_connection,
        params=TransportParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            vad_analyzer=SileroVADAnalyzer(),
            audio_out_10ms_chunks=2,
        ),
    )

    # Define function schemas for tool calling
    client_existence_check_function = FunctionSchema(
        name="client_existence_check",
        description="Check if a client exists in the system using their phone number",
        properties={
            "phone": {
                "type": "string",
                "description": "Client's phone number (without country code)",
            },
        },
        required=["phone"],
    )

    doctor_list_function = FunctionSchema(
        name="doctor_list",
        description="Get the list of available doctors",
        properties={},
        required=[],
    )

    available_slots_function = FunctionSchema(
        name="available_slots",
        description="Get available appointment slots for a specific doctor on a specific date",
        properties={
            "doctor_id": {
                "type": "string",
                "description": "The unique identifier of the doctor",
            },
            "date": {
                "type": "string",
                "description": "The date for the appointment in YYYY-MM-DD format",
            },
            "duration": {
                "type": "integer",
                "description": "Duration of appointment in minutes. Must be one of: 15, 30, 45, or 60 minutes.",
            },
        },
        required=["doctor_id", "date", "duration"],
    )

    appointment_booking_function = FunctionSchema(
        name="appointment_booking_tool",
        description="Book a new appointment with all client and appointment details",
        properties={
            "doctor_id": {"type": "string", "description": "Doctor's unique ID"},
            "doctor_name": {"type": "string", "description": "Doctor's full name"},
            "date": {"type": "string", "description": "Appointment date (YYYY-MM-DD)"},
            "timeslot": {"type": "string", "description": "Appointment time slot (HH:MM format)"},
            "duration": {"type": "integer", "description": "Appointment duration in minutes"},
            "full_name": {"type": "string", "description": "Client's full name"},
            "dob": {"type": "string", "description": "Client's date of birth (YYYY-MM-DD)"},
            "gender": {"type": "string", "description": "Client's gender", "enum": ["male", "female"]},
            "email": {"type": "string", "description": "Client's email address"},
            "phone": {"type": "string", "description": "Client's phone number"},
            "phone_country_code": {"type": "string", "description": "Phone country code (e.g., +1)"},
            "complaint": {"type": "string", "description": "Client's complaint or reason for visit"},
        },
        required=["doctor_id", "doctor_name", "date", "timeslot", "duration", 
                 "full_name", "dob", "gender", "email", "phone"],
    )

    client_appointments_function = FunctionSchema(
        name="client_appointments",
        description="Get all appointments for a client using their phone number",
        properties={
            "phone": {
                "type": "string",
                "description": "Client's phone number",
            },
        },
        required=["phone"],
    )

    update_appointment_function = FunctionSchema(
        name="update_appointment",
        description="Update an existing appointment (reschedule or cancel)",
        properties={
            "appointment_id": {"type": "string", "description": "The unique appointment ID"},
            "status": {"type": "string", "description": "New status", "enum": ["booked", "confirmed", "cancel"]},
            "timeslot": {"type": "string", "description": "New time slot (HH:MM) if rescheduling"},
            "duration": {"type": "integer", "description": "New duration if changing"},
        },
        required=["appointment_id"],
    )

    # Create tools schema
    tools = ToolsSchema(
        standard_tools=[
            client_existence_check_function,
            doctor_list_function,
            available_slots_function,
            appointment_booking_function,
            client_appointments_function,
            update_appointment_function,
        ]
    )

    # Initialize Gemini Vertex LLM service
    llm_service = GeminiLiveVertexLLMService(
        credentials=os.getenv("GOOGLE_VERTEX_TEST_CREDENTIALS") or '',
        project_id=os.getenv("GOOGLE_CLOUD_PROJECT_ID") or '',
        location=os.getenv("GOOGLE_CLOUD_LOCATION") or '',
        system_instruction=APPOINTMENT_SYSTEM_INSTRUCTION,
        voice_id=voice,
        tools=tools,
    )

    # Register function handlers
    llm_service.register_function("client_existence_check", check_client_existence)
    llm_service.register_function("doctor_list", get_doctor_list)
    llm_service.register_function("available_slots", get_available_slots)
    llm_service.register_function("appointment_booking_tool", book_appointment)
    llm_service.register_function("client_appointments", get_client_appointments)
    llm_service.register_function("update_appointment", update_appointment)

    transcript = TranscriptProcessor()

    context = OpenAILLMContext(
        [
            {
                "role": "user",
                "content": "Greet the user warmly and introduce yourself as the appointment booking assistant.",
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
        logger.info("Appointment bot client connected")
        await task.queue_frames([LLMRunFrame()])

    @pipecat_transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client):
        logger.info("Appointment bot client disconnected")
        await task.cancel()

    runner = PipelineRunner(handle_sigint=False)
    await runner.run(task)
