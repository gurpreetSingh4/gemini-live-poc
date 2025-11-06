import os
from datetime import datetime
from typing import Dict, List, Optional
import httpx
import json

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

# API Configuration
API_BASE_URL = "https://10953088fc2b.ngrok-free.app"
API_DOCTOR_BASE_URL = "https://appointment.zavis.ai"
API_ACCOUNT_ID = 1
API_CENTER_ID = 0
API_TIMEZONE = "Asia/Dubai"

logger.info(f"🔧 API Configuration:")
logger.info(f"   Base URL: {API_BASE_URL}")
logger.info(f"   Doctor API: {API_DOCTOR_BASE_URL}")
logger.info(f"   Account ID: {API_ACCOUNT_ID}")
logger.info(f"   Center ID: {API_CENTER_ID}")
logger.info(f"   Timezone: {API_TIMEZONE}")

# Tool functions for appointment booking


async def check_client_existence(params: FunctionCallParams):
    """Check if a client exists by phone number"""
    phone = params.arguments.get("phone")
    logger.info(f"🔍 Tool Call: check_client_existence")
    logger.info(f"   📞 Phone: {phone}")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            payload = {
                "accountId": API_ACCOUNT_ID,
                "phoneWithOutCountryCode": phone
            }
            logger.info(f"   📤 API Request: POST {API_BASE_URL}/clients/find-or-fetch")
            logger.info(f"   📦 Payload: {json.dumps(payload, indent=2)}")
            
            response = await client.post(
                f"{API_BASE_URL}/clients/find-or-fetch",
                json=payload
            )
            response.raise_for_status()
            data = response.json()
            
            logger.info(f"   ✅ API Response: {json.dumps(data, indent=2)}")
            
            # Check if client exists based on API response
            if data and isinstance(data, dict) and data.get("id"):
                result = {
                    "exists": True,
                    "client_data": data
                }
                logger.info(f"   👤 Client Found: ID={data.get('id')}, Name={data.get('firstName', '')} {data.get('lastName', '')}")
            else:
                result = {
                    "exists": False,
                    "message": "Client not found. Please provide registration details."
                }
                logger.info(f"   ❌ Client Not Found")
            
            await params.result_callback(result)
            
    except Exception as e:
        logger.error(f"   ❌ Error checking client: {str(e)}")
        await params.result_callback({
            "exists": False,
            "error": str(e),
            "message": "Unable to check client details. Please try again."
        })


async def get_doctor_list(params: FunctionCallParams):
    """Get list of available doctors"""
    logger.info("🔍 Tool Call: get_doctor_list")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            url = f"{API_BASE_URL}/professionals?accountId={API_ACCOUNT_ID}"
            logger.info(f"   📤 API Request: GET {url}")
            
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            
            logger.info(f"   ✅ API Response: {json.dumps(data, indent=2)}")
            
            # Extract doctors list from response
            doctors = data if isinstance(data, list) else []
            logger.info(f"   👨‍⚕️ Found {len(doctors)} doctors")
            
            await params.result_callback({
                "doctors": doctors,
                "count": len(doctors)
            })
            
    except Exception as e:
        logger.error(f"   ❌ Error fetching doctor list: {str(e)}")
        await params.result_callback({
            "doctors": [],
            "error": str(e),
            "message": "Unable to fetch doctor list. Please try again."
        })


async def get_available_slots(params: FunctionCallParams):
    """Get available appointment slots for a doctor on a specific date"""
    doctor_id = params.arguments.get("doctor_id")
    date = params.arguments.get("date")
    duration = params.arguments.get("duration", 30)
    
    # Validate duration
    valid_durations = ["15", "30", "45", "60"]
    if duration not in valid_durations:
        logger.warning(f"Invalid duration {duration}, defaulting to 30 minutes")
        duration = 30
    
    logger.info(f"🔍 Tool Call: get_available_slots")
    logger.info(f"   👨‍⚕️ Doctor ID: {doctor_id}")
    logger.info(f"   📅 Date: {date}")
    logger.info(f"   ⏱️  Duration: {duration} minutes")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            payload = {
                "accountId": API_ACCOUNT_ID,
                "platformCenterId": API_CENTER_ID,
                "timezone": API_TIMEZONE,
                "professionalId": int(doctor_id) if doctor_id is not None else 0,
                "appointmentDate": date,
                "durations": duration
            }
            logger.info(f"   📤 API Request: POST {API_BASE_URL}/professionals/available-slots-v2")
            logger.info(f"   📦 Payload: {json.dumps(payload, indent=2)}")
            
            response = await client.post(
                f"{API_BASE_URL}/professionals/available-slots-v2",
                json=payload
            )
            response.raise_for_status()
            data = response.json()
            
            logger.info(f"   ✅ API Response: {json.dumps(data, indent=2)}")
            
            # Extract slots from response
            slots = data.get("data", []) if isinstance(data, dict) else data if isinstance(data, list) else []
            logger.info(f"   🕐 Found {len(slots)} available slots")
            
            await params.result_callback({
                "doctor_id": doctor_id,
                "date": date,
                "duration": duration,
                "available_slots": slots,
                "count": len(slots)
            })
            
    except Exception as e:
        logger.error(f"   ❌ Error fetching available slots: {str(e)}")
        await params.result_callback({
            "doctor_id": doctor_id,
            "date": date,
            "duration": duration,
            "available_slots": [],
            "error": str(e),
            "message": "Unable to fetch available slots. Please try again."
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
    dob = params.arguments.get("client_dob")
    gender = params.arguments.get("gender")
    email = params.arguments.get("email")
    phone = params.arguments.get("phone")
    phone_country_code = params.arguments.get("phone_country_code", "+971")
    client_id = params.arguments.get("client_id", "")
    remarks = params.arguments.get("remarks", "")
    complaint = params.arguments.get("complaint", "")
    
    logger.info(f"🔍 Tool Call: book_appointment")
    logger.info(f"   👤 Client: {full_name}")
    logger.info(f"   👨‍⚕️ Doctor: {doctor_name} (ID: {doctor_id})")
    logger.info(f"   📅 Date: {date}")
    logger.info(f"   🕐 Time: {timeslot}")
    logger.info(f"   ⏱️  Duration: {duration} minutes")
    
    try:
        # Calculate end time based on duration
        from datetime import datetime, timedelta
        end_time_iso = timeslot or ""
        try:
            if timeslot and duration:
                start_time = datetime.fromisoformat(str(timeslot).replace('Z', '+00:00'))
                end_time = start_time + timedelta(minutes=int(duration))
                end_time_iso = end_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        except:
            pass
        
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            payload = {
                "accountId": API_ACCOUNT_ID,
                "platformCenterId": API_CENTER_ID,
                "professionalId": doctor_id,
                "clientFullName": full_name,
                "clientPhoneNumberWithoutCountryCode": phone,
                "clientPhoneCountryCode": phone_country_code,
                "clientDob": dob,
                "clientGender": gender,
                "clientEmail": email,
                "appointmentSlot": timeslot,
                "duration": duration,
                "complaint": complaint,
                "remarks": remarks,
                "paymentRequired": True,
                "paymentMethod": "online",
                "currency": "aed",
                "amount": 2,
                "platformClientId": client_id,
                "timezone": API_TIMEZONE,
            }
            logger.info(f"   📤 API Request: POST {API_BASE_URL}/appointments")
            logger.info(f"   📦 Payload: {json.dumps(payload, indent=2)}")
            
            response = await client.post(
                f"{API_BASE_URL}/appointments",
                json=payload
            )
            response.raise_for_status()
            data = response.json()
            
            logger.info(f"   ✅ API Response: {json.dumps(data, indent=2)}")
            
            appointment_id = data.get("id") or data.get("appointmentId") or "N/A"
            logger.info(f"   🎉 Appointment Booked Successfully! ID: {appointment_id}")
            
            await params.result_callback({
                "success": True,
                "appointment_id": appointment_id,
                "message": f"Appointment successfully booked with {doctor_name} on {date} at {timeslot}",
                "appointment_details": data
            })
            
    except Exception as e:
        logger.error(f"   ❌ Error booking appointment: {str(e)}")
        await params.result_callback({
            "success": False,
            "error": str(e),
            "message": "Sorry, something went wrong while booking. Please try again or contact our front desk."
        })


async def get_client_appointments(params: FunctionCallParams):
    """Get all appointments for a client by phone number"""
    phone_number = params.arguments.get("phone_number")
    phone_country_code = params.arguments.get("phone_country_code")
    
    logger.info(f"🔍 Tool Call: get_client_appointments")
    logger.info(f"   📞 Phone: {phone_country_code}{phone_number}")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            payload = {
                "accountId": API_ACCOUNT_ID,
                "centerId": API_CENTER_ID,
                "timezone": API_TIMEZONE,
                "phoneWithOutCountryCode": phone_number,
                "phoneCountryCode": phone_country_code
            }
            logger.info(f"   📤 API Request: POST {API_BASE_URL}/appointments/v2")
            logger.info(f"   📦 Payload: {json.dumps(payload, indent=2)}")
            
            response = await client.post(
                f"{API_BASE_URL}/appointments/v2",
                json=payload
            )
            response.raise_for_status()
            data = response.json()
            
            logger.info(f"   ✅ API Response: {json.dumps(data, indent=2)}")
            
            # Extract appointments from response
            appointments = data.get("data", []) if isinstance(data, dict) else data if isinstance(data, list) else []
            
            # Filter upcoming appointments
            upcoming_appointments = [apt for apt in appointments if apt.get("status") != "cancelled"]
            logger.info(f"   📋 Found {len(upcoming_appointments)} upcoming appointments")
            
            await params.result_callback({
                "phone": f"{phone_country_code}{phone_number}",
                "appointments": upcoming_appointments,
                "count": len(upcoming_appointments)
            })
            
    except Exception as e:
        logger.error(f"   ❌ Error fetching appointments: {str(e)}")
        await params.result_callback({
            "phone": f"{phone_country_code}{phone_number}",
            "appointments": [],
            "count": 0,
            "error": str(e),
            "message": "Unable to fetch appointments. Please try again."
        })


async def update_appointment(params: FunctionCallParams):
    """Update an existing appointment (reschedule or cancel)"""
    appointment_id = params.arguments.get("appointment_id")
    status = params.arguments.get("status")
    timeslot = params.arguments.get("timeslot")
    duration = params.arguments.get("duration")
    
    logger.info(f"🔍 Tool Call: update_appointment")
    logger.info(f"   🎫 Appointment ID: {appointment_id}")
    logger.info(f"   📊 Status: {status}")
    logger.info(f"   🕐 New Timeslot: {timeslot}")
    logger.info(f"   ⏱️  New Duration: {duration}")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            payload = {
                "accountId": API_ACCOUNT_ID,
                "centerId": API_CENTER_ID,
                "timezone": API_TIMEZONE,
                "appointment_id": appointment_id
            }
            
            if status:
                payload["status"] = status
            if timeslot:
                payload["timeslot"] = timeslot
            if duration:
                payload["duration"] = duration
            
            logger.info(f"   📤 API Request: POST {API_BASE_URL}/appointments/update-v2")
            logger.info(f"   📦 Payload: {json.dumps(payload, indent=2)}")
            
            response = await client.post(
                f"{API_BASE_URL}/appointments/update-v2",
                json=payload
            )
            response.raise_for_status()
            data = response.json()
            
            logger.info(f"   ✅ API Response: {json.dumps(data, indent=2)}")
            logger.info(f"   ✨ Appointment Updated Successfully!")
            
            await params.result_callback({
                "success": True,
                "message": "Your appointment has been updated successfully!" if status != "cancel" else "✅ Your appointment has been successfully cancelled.",
                "appointment_details": data
            })
            
    except Exception as e:
        logger.error(f"   ❌ Error updating appointment: {str(e)}")
        await params.result_callback({
            "success": False,
            "error": str(e),
            "message": "Sorry, I couldn't update your appointment. Please try again later."
        })


# System instruction for appointment booking assistant
APPOINTMENT_SYSTEM_INSTRUCTION = """
You are a friendly and professional appointment booking assistant for a medical clinic.

Your name is Zavis Appointment Assistant.

🎯 TRIGGER
Activate when the user mentions anything related to booking or appointments, such as:
"book", "appointment", "see a doctor", "schedule a visit", "view my appointments".

👋 GREETING & CONTEXT
Start with: "Hello How can I help you today?"

---

📋 1. VIEW / MANAGE EXISTING APPOINTMENTS

If user wants to view, update, reschedule, or cancel an appointment:

✅ Do NOT ask for phone number - use client's existing phone from context
✅ Call: @client_appointments

**If appointments found:**
"Here are your upcoming appointments:"
(Display details exactly as returned — id, doctor, date, time, status)

**Branch A: Change Status or Update**
a. Identify Appointment:
   "Which appointment would you like to update?" (if multiple)

b. Determine Update Type:

   **Case 1: Status = "cancel"**
   If user says "cancel my appointment":
   ✅ Directly call @update_appointment with:
   {
     "appointment_id": <id>,
     "status": "cancel"
   }
   Response: "✅ Your appointment has been successfully cancelled."

   **Case 2: Reschedule (change timeslot/duration)**
   1. Ask: "Sure! What date would you like to reschedule to?"
   2. Ask: "How long would you like your appointment to be — 15, 30, 45, or 60 minutes?"
   3. Call: @available_slots with selected doctor, new date, duration
   4. Show: "Here are available slots for [Doctor] on [Date], each [Duration] mins. Which one?"
   5. When slot selected → Call: @update_appointment with:
   {
     "appointment_id": <id>,
     "duration": <selectedDuration>,
     "timeslot": "<selectedSlot>"
   }
   
   ✅ On success: "Your appointment has been updated successfully!"
   ❌ On failure: "Sorry, I couldn't update your appointment. Please try again later."

**If none found:**
"I couldn't find any upcoming appointments linked to your number."

---

🏥 2. BOOK A NEW APPOINTMENT

**a. Fetch Doctors**
Call: @doctor_list
✅ If available → "Here are our available doctors — please choose one."
❌ If none → "Looks like we don't have available doctors right now. Please check back later."

**b. Doctor Selection**
After selection: "Great! You chose [Doctor Name]. Which date (day month year) would you like to schedule your appointment for?"
tell selected doctors date (day, month, year)

**c. Duration Selection**
"How long would you like your appointment to be — 15, 30, 45, or 60 minutes?"

**d. Fetch Available Slots**
Call: @available_slots with doctor ID, date, and duration
✅ Show: "Here are available slots for [Date], each [Duration] mins. Which one would you like?"
you get  [
    "18 Dec 2025 07:30:00",
    "18 Dec 2025 07:45:00",
    "18 Dec 2025 08:00:00",
    "18 Dec 2025 13:15:00",]
you have to speak as Date 18 Dec 2025 Time slots is like 7:30 AM, 1:15PM
❌ If none: "No open slots for that doctor on this date. Would you like to try another duration, date, or doctor?"

---

👤 3. CLIENT VERIFICATION

After slot selection:
"Please share your phone number (without country code) so I can check if you're already registered."

Call: @client_existence_check with the provided phone

**Branch A: Client Exists**
Display found details for confirmation:
"I found your details:
Name: [Full Name]
DOB: [Date of Birth]
Gender: [Gender]
Email: [Email]
Phone: [phone]
PhoneCountryCode: [phoneCountryCode]

Please confirm if everything looks correct."

✅ If confirmed → Ask for complaint/remarks
✅ Call: @appointment_booking_tool with all details

**Branch B: Client Not Found**
"Looks like this number isn't registered yet. Let's get your details."

Collect sequentially:
1. Full Name
2. Date of Birth (ISO format: YYYY-MM-DDTHH:MM:SS.sssZ)
3. Gender ("male", "female")
4. Email
5. Phone (local number)
6. PhoneCountryCode (e.g., +971, +91)

Then collect complaint or remarks
✅ Call: @appointment_booking_tool with full details
appointment timeslot is in (ISO format: YYYY-MM-DDTHH:MM:SS.sssZ)
dob ( client date of birth) should be in (ISO format: YYYY-MM-DDTHH:MM:SS.sssZ)
platform client Id is like "FSMR000044", "MR0000"

---

✅ 4. BOOKING CONFIRMATION

While booking: "Booking your appointment, please hold on…"

**On Success:**
"✅ Your appointment with [Doctor Name] has been confirmed! You'll receive a confirmation message shortly."

**On Failure:**
"Sorry, something went wrong while booking. Please try again or contact our front desk."

---

❓ 5. GENERAL QUERIES

If unrelated question (hours, address, etc.):
"I can help you with appointment bookings right now. For other details, please contact our front desk or I can connect you to an agent."

If medical/legal advice:
"I'm not qualified to provide medical advice, but I can book you an appointment with a specialist."

---

🎨 TONE & STYLE
✅ Friendly, calm, professional — like a clinic receptionist
✅ Keep responses short (1-2 sentences)
✅ Never invent or assume data
✅ Use emojis sparingly: 👋 ✅ 🌿
✅ Always confirm before proceeding
✅ Guide users step by step
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
                "description": "The unique identifier of the doctor (number as string)",
            },
            "date": {
                "type": "string",
                "description": "The date for the appointment in ISO format (e.g., 2025-12-18T00:37:35.553Z)",
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
            "full_name": {"type": "string", "description": "Client's full name"},
            "phone": {"type": "string", "description": "Client's phone number without country code (e.g., 9876543210)"},
            "phone_country_code": {"type": "string", "description": "Phone country code (e.g., +971, +91)"},
            "dob": {"type": "string", "description": "Client's date of birth in ISO format (e.g., 1990-01-15T00:00:00.000Z)"},
            "gender": {"type": "string", "description": "Client's gender (male, female)"},
            "email": {"type": "string", "description": "Client's email address"},
            "timeslot": {"type": "string", "description": "Appointment time slot in ISO format (e.g., 2025-12-18T09:00:00.000Z)"},
            "duration": {"type": "integer", "description": "Appointment duration in minutes (15, 30, 45, or 60)"},
            "complaint": {"type": "string", "description": "Client's complaint or reason for visit"},
            "remarks": {"type": "string", "description": "Additional notes or remarks"},
            "client_id": {"type": "string", "description": "platform ClientId  from system (if client exists)"},
        },
        required=["doctor_id", "full_name", "timeslot", "duration", 
                 "full_name", "dob", "gender", "email", "phone", "phone_country_code", "client_id", "remarks", "complaint"],
    )

    client_appointments_function = FunctionSchema(
        name="client_appointments",
        description="Get all appointments for a client using their phone number",
        properties={
            "phone_number": {
                "type": "string",
                "description": "The client's phone number without country code (e.g., 9876543210)",
            },
            "phone_country_code": {
                "type": "string",
                "description": "The country code for the phone number (e.g., +971, +91)",
            },
        },
        required=["phone_number", "phone_country_code"],
    )

    update_appointment_function = FunctionSchema(
        name="update_appointment",
        description="Update an existing appointment (reschedule or cancel)",
        properties={
            "appointment_id": {"type": "integer", "description": "The unique appointment ID (integer)"},
            "status": {"type": "string", "description": "New status (booked, confirmed, cancel)"},
            "timeslot": {"type": "string", "description": "New time slot in ISO format (e.g., 2025-12-18T09:00:00.000Z) if rescheduling"},
            "duration": {"type": "integer", "description": "New duration in minutes (15, 30, 45, 60) if changing"},
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
