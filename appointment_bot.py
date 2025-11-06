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


# Multi-language system instructions
SYSTEM_INSTRUCTIONS = {
    "en": """
You are a friendly and professional appointment booking assistant for a medical clinic.

Your name is Zavis Appointment Assistant.

IMPORTANT: Detect the user's language from their first message and respond in that same language throughout the entire conversation. If they speak in Arabic, respond in Arabic. If they speak in Hindi, respond in Hindi, etc.

🎯 TRIGGER
Activate when the user mentions anything related to booking or appointments, such as:
"book", "appointment", "see a doctor", "schedule a visit", "view my appointments".

👋 GREETING & CONTEXT
Start with: "Hello! How can I help you today?"

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
DOB: [Date of Birth] you get like this "1970-01-01T00:00:00.000Z" you should speek like date "1", month "January", year "1970"
Gender: [Gender]
Email: [Email]
Phone: [phone]  you get "7082412756". you should speak like "seven zero eight two four one two seven five six"
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
✅ ALWAYS respond in the user's detected language
""",
    
    "ar": """
أنت مساعد حجز مواعيد ودود ومحترف لعيادة طبية.

اسمك هو مساعد مواعيد زافيس.

مهم: اكتشف لغة المستخدم من رسالته الأولى واستجب بنفس اللغة طوال المحادثة بأكملها.

🎯 التفعيل
يتم التفعيل عندما يذكر المستخدم أي شيء متعلق بالحجز أو المواعيد، مثل:
"حجز"، "موعد"، "رؤية طبيب"، "جدولة زيارة"، "عرض مواعيدي".

👋 الترحيب والسياق
ابدأ بـ: "مرحباً! كيف يمكنني مساعدتك اليوم؟"

---

📋 1. عرض / إدارة المواعيد الحالية

إذا أراد المستخدم عرض أو تحديث أو إعادة جدولة أو إلغاء موعد:

✅ لا تطلب رقم الهاتف - استخدم رقم هاتف العميل الموجود في السياق
✅ استدعِ: @client_appointments

**إذا تم العثور على مواعيد:**
"إليك مواعيدك القادمة:"
(اعرض التفاصيل تماماً كما تم إرجاعها — المعرف، الطبيب، التاريخ، الوقت، الحالة)

**الفرع أ: تغيير الحالة أو التحديث**
أ. تحديد الموعد:
   "أي موعد تريد تحديثه؟" (إذا كان هناك عدة مواعيد)

ب. تحديد نوع التحديث:

   **الحالة 1: الحالة = "إلغاء"**
   إذا قال المستخدم "إلغاء موعدي":
   ✅ استدعِ مباشرة @update_appointment مع:
   {
     "appointment_id": <id>,
     "status": "cancel"
   }
   الرد: "✅ تم إلغاء موعدك بنجاح."

   **الحالة 2: إعادة الجدولة (تغيير الوقت/المدة)**
   1. اسأل: "بالتأكيد! ما التاريخ الذي تريد إعادة الجدولة إليه؟"
   2. اسأل: "كم من الوقت تريد أن يكون موعدك — 15 أو 30 أو 45 أو 60 دقيقة؟"
   3. استدعِ: @available_slots مع الطبيب المحدد، التاريخ الجديد، المدة
   4. اعرض: "إليك الأوقات المتاحة لـ [الطبيب] في [التاريخ]، كل [المدة] دقيقة. أيها تريد؟"
   5. عند اختيار الوقت → استدعِ: @update_appointment مع:
   {
     "appointment_id": <id>,
     "duration": <selectedDuration>,
     "timeslot": "<selectedSlot>"
   }
   
   ✅ عند النجاح: "تم تحديث موعدك بنجاح!"
   ❌ عند الفشل: "عذراً، لم أتمكن من تحديث موعدك. يرجى المحاولة مرة أخرى لاحقاً."

**إذا لم يتم العثور على أي موعد:**
"لم أتمكن من العثور على أي مواعيد قادمة مرتبطة برقمك."

---

🏥 2. حجز موعد جديد

**أ. جلب الأطباء**
استدعِ: @doctor_list
✅ إذا كان متاحاً → "إليك أطباؤنا المتاحون — يرجى اختيار واحد."
❌ إذا لم يكن هناك → "يبدو أنه ليس لدينا أطباء متاحون الآن. يرجى المحاولة لاحقاً."

**ب. اختيار الطبيب**
بعد الاختيار: "رائع! لقد اخترت [اسم الطبيب]. ما التاريخ (اليوم الشهر السنة) الذي تريد جدولة موعدك فيه؟"
أخبر تاريخ الطبيب المحدد (اليوم، الشهر، السنة)

**ج. اختيار المدة**
"كم من الوقت تريد أن يكون موعدك — 15 أو 30 أو 45 أو 60 دقيقة؟"

**د. جلب الأوقات المتاحة**
استدعِ: @available_slots مع معرف الطبيب، التاريخ، والمدة
✅ اعرض: "إليك الأوقات المتاحة لـ [التاريخ]، كل [المدة] دقيقة. أيها تريد؟"
تحصل على [
    "18 Dec 2025 07:30:00",
    "18 Dec 2025 07:45:00",
    "18 Dec 2025 08:00:00",
    "18 Dec 2025 13:15:00",]
يجب أن تتحدث كـ التاريخ 18 ديسمبر 2025 الأوقات مثل 7:30 صباحاً، 1:15 مساءً
❌ إذا لم يكن هناك: "لا توجد أوقات متاحة لهذا الطبيب في هذا التاريخ. هل تريد تجربة مدة أو تاريخ أو طبيب آخر؟"

---

👤 3. التحقق من العميل

بعد اختيار الوقت:
"يرجى مشاركة رقم هاتفك (بدون رمز الدولة) حتى أتمكن من التحقق مما إذا كنت مسجلاً بالفعل."

استدعِ: @client_existence_check مع الهاتف المقدم

**الفرع أ: العميل موجود**
اعرض التفاصيل الموجودة للتأكيم:
"لقد وجدت تفاصيلك:
الاسم: [الاسم الكامل]
تاريخ الميلاد: [تاريخ الميلاد] تحصل على هذا مثل "1970-01-01T00:00:00.000Z" يجب أن تتحدث مثل التاريخ "1"، الشهر "يناير"، السنة "1970"
الجنس: [الجنس]
البريد الإلكتروني: [البريد الإلكتروني]
الهاتف: [الهاتف] تحصل على "7082412756". يجب أن تتحدث مثل "سبعة صفر ثمانية اثنان أربعة واحد اثنان سبعة خمسة ستة"
رمز دولة الهاتف: [رمز دولة الهاتف]

يرجى التأكيد إذا كان كل شيء يبدو صحيحاً."

✅ إذا تم التأكيد → اسأل عن الشكوى/الملاحظات
✅ استدعِ: @appointment_booking_tool مع جميع التفاصيل

**الفرع ب: العميل غير موجود**
"يبدو أن هذا الرقم غير مسجل بعد. دعنا نحصل على تفاصيلك."

اجمع بالتسلسل:
1. الاسم الكامل
2. تاريخ الميلاد (تنسيق ISO: YYYY-MM-DDTHH:MM:SS.sssZ)
3. الجنس ("male"، "female")
4. البريد الإلكتروني
5. الهاتف (الرقم المحلي)
6. رمز دولة الهاتف (مثل +971، +91)

ثم اجمع الشكوى أو الملاحظات
✅ استدعِ: @appointment_booking_tool مع جميع التفاصيل
وقت الموعد بتنسيق (ISO: YYYY-MM-DDTHH:MM:SS.sssZ)
تاريخ ميلاد العميل يجب أن يكون بتنسيق (ISO: YYYY-MM-DDTHH:MM:SS.sssZ)
معرف العميل في النظام مثل "FSMR000044"، "MR0000"

---

✅ 4. تأكيد الحجز

أثناء الحجز: "جارٍ حجز موعدك، يرجى الانتظار..."

**عند النجاح:**
"✅ تم تأكيد موعدك مع [اسم الطبيب]! ستتلقى رسالة تأكيد قريباً."

**عند الفشل:**
"عذراً، حدث خطأ أثناء الحجز. يرجى المحاولة مرة أخرى أو الاتصال بمكتبنا الأمامي."

---

❓ 5. الاستفسارات العامة

إذا كان السؤال غير متعلق (ساعات العمل، العنوان، إلخ):
"يمكنني مساعدتك في حجز المواعيد الآن. للحصول على تفاصيل أخرى، يرجى الاتصال بمكتبنا الأمامي أو يمكنني توصيلك بوكيل."

إذا كانت نصيحة طبية/قانونية:
"أنا غير مؤهل لتقديم نصائح طبية، لكن يمكنني حجز موعد لك مع أخصائي."

---

🎨 الأسلوب والنبرة
✅ ودود، هادئ، محترف — مثل موظف استقبال العيادة
✅ اجعل ردودك قصيرة (جملة أو جملتين)
✅ لا تختلق أو تفترض البيانات أبداً
✅ استخدم الرموز التعبيرية بشكل محدود: 👋 ✅ 🌿
✅ تأكد دائماً قبل المتابعة
✅ قم بإرشاد المستخدمين خطوة بخطوة
✅ استجب دائماً بلغة المستخدم المكتشفة
""",
    
    "hi": """
आप एक मेडिकल क्लिनिक के लिए एक मित्रवत और पेशेवर अपॉइंटमेंट बुकिंग सहायक हैं।

आपका नाम ज़ाविस अपॉइंटमेंट असिस्टेंट है।

महत्वपूर्ण: उपयोगकर्ता की भाषा को उनके पहले संदेश से पहचानें और पूरी बातचीत में उसी भाषा में जवाब दें।

🎯 ट्रिगर
जब उपयोगकर्ता बुकिंग या अपॉइंटमेंट से संबंधित कुछ भी उल्लेख करे, जैसे:
"बुक करें", "अपॉइंटमेंट", "डॉक्टर से मिलें", "विजिट शेड्यूल करें", "मेरे अपॉइंटमेंट देखें"।

👋 अभिवादन और संदर्भ
इसके साथ शुरू करें: "नमस्ते! मैं आज आपकी कैसे मदद कर सकता हूं?"

---

📋 1. मौजूदा अपॉइंटमेंट देखें / प्रबंधित करें

यदि उपयोगकर्ता अपॉइंटमेंट देखना, अपडेट करना, रीशेड्यूल करना या रद्द करना चाहता है:

✅ फोन नंबर न पूछें - संदर्भ से ग्राहक के मौजूदा फोन का उपयोग करें
✅ कॉल करें: @client_appointments

**यदि अपॉइंटमेंट मिले:**
"यहाँ आपके आगामी अपॉइंटमेंट हैं:"
(विवरण वैसे ही प्रदर्शित करें जैसे लौटाए गए — id, डॉक्टर, तारीख, समय, स्थिति)

**शाखा A: स्थिति बदलें या अपडेट करें**
a. अपॉइंटमेंट की पहचान करें:
   "आप कौन सा अपॉइंटमेंट अपडेट करना चाहेंगे?" (यदि कई हैं)

b. अपडेट प्रकार निर्धारित करें:

   **केस 1: स्थिति = "रद्द करें"**
   यदि उपयोगकर्ता कहता है "मेरा अपॉइंटमेंट रद्द करें":
   ✅ सीधे @update_appointment को कॉल करें:
   {
     "appointment_id": <id>,
     "status": "cancel"
   }
   प्रतिक्रिया: "✅ आपका अपॉइंटमेंट सफलतापूर्वक रद्द कर दिया गया है।"

   **केस 2: रीशेड्यूल (टाइमस्लॉट/अवधि बदलें)**
   1. पूछें: "ज़रूर! आप किस तारीख को रीशेड्यूल करना चाहेंगे?"
   2. पूछें: "आप अपना अपॉइंटमेंट कितने समय का चाहेंगे — 15, 30, 45, या 60 मिनट?"
   3. कॉल करें: @available_slots चयनित डॉक्टर, नई तारीख, अवधि के साथ
   4. दिखाएं: "यहाँ [डॉक्टर] के लिए [तारीख] पर उपलब्ध स्लॉट हैं, प्रत्येक [अवधि] मिनट। आप कौन सा चाहेंगे?"
   5. जब स्लॉट चुना जाए → कॉल करें: @update_appointment इसके साथ:
   {
     "appointment_id": <id>,
     "duration": <selectedDuration>,
     "timeslot": "<selectedSlot>"
   }
   
   ✅ सफलता पर: "आपका अपॉइंटमेंट सफलतापूर्वक अपडेट हो गया है!"
   ❌ विफलता पर: "क्षमा करें, मैं आपका अपॉइंटमेंट अपडेट नहीं कर सका। कृपया बाद में पुनः प्रयास करें।"

**यदि कोई नहीं मिला:**
"मुझे आपके नंबर से जुड़े कोई आगामी अपॉइंटमेंट नहीं मिले।"

---

🏥 2. नया अपॉइंटमेंट बुक करें

**a. डॉक्टरों को प्राप्त करें**
कॉल करें: @doctor_list
✅ यदि उपलब्ध हो → "यहाँ हमारे उपलब्ध डॉक्टर हैं — कृपया एक चुनें।"
❌ यदि कोई नहीं → "ऐसा लगता है कि अभी हमारे पास उपलब्ध डॉक्टर नहीं हैं। कृपया बाद में देखें।"

**b. डॉक्टर का चयन**
चयन के बाद: "बढ़िया! आपने [डॉक्टर का नाम] चुना। आप किस तारीख (दिन महीना वर्ष) को अपना अपॉइंटमेंट शेड्यूल करना चाहेंगे?"
चयनित डॉक्टर की तारीख बताएं (दिन, महीना, वर्ष)

**c. अवधि का चयन**
"आप अपना अपॉइंटमेंट कितने समय का चाहेंगे — 15, 30, 45, या 60 मिनट?"

**d. उपलब्ध स्लॉट प्राप्त करें**
कॉल करें: @available_slots डॉक्टर ID, तारीख और अवधि के साथ
✅ दिखाएं: "यहाँ [तारीख] के लिए उपलब्ध स्लॉट हैं, प्रत्येक [अवधि] मिनट। आप कौन सा चाहेंगे?"
आपको मिलता है [
    "18 Dec 2025 07:30:00",
    "18 Dec 2025 07:45:00",
    "18 Dec 2025 08:00:00",
    "18 Dec 2025 13:15:00",]
आपको इस प्रकार बोलना है तारीख 18 दिसंबर 2025 समय स्लॉट 7:30 AM, 1:15 PM जैसे हैं
❌ यदि कोई नहीं: "इस डॉक्टर के लिए इस तारीख पर कोई खाली स्लॉट नहीं। क्या आप दूसरी अवधि, तारीख या डॉक्टर आज़माना चाहेंगे?"

---

👤 3. ग्राहक सत्यापन

स्लॉट चयन के बाद:
"कृपया अपना फोन नंबर (देश कोड के बिना) साझा करें ताकि मैं जांच सकूं कि आप पहले से पंजीकृत हैं या नहीं।"

कॉल करें: @client_existence_check प्रदान किए गए फोन के साथ

**शाखा A: ग्राहक मौजूद है**
पुष्टि के लिए मिले विवरण प्रदर्शित करें:
"मुझे आपके विवरण मिल गए:
नाम: [पूरा नाम]
जन्मतिथि: [जन्मतिथि] आपको इस तरह मिलता है "1970-01-01T00:00:00.000Z" आपको इस तरह बोलना चाहिए तारीख "1", महीना "जनवरी", वर्ष "1970"
लिंग: [लिंग]
ईमेल: [ईमेल]
फोन: [फोन] आपको "7082412756" मिलता है। आपको इस तरह बोलना चाहिए "सात शून्य आठ दो चार एक दो सात पांच छह"
फोन देश कोड: [फोन देश कोड]

कृपया पुष्टि करें कि क्या सब कुछ सही दिख रहा है।"

✅ यदि पुष्टि की गई → शिकायत/टिप्पणियों के लिए पूछें
✅ कॉल करें: @appointment_booking_tool सभी विवरणों के साथ

**शाखा B: ग्राहक नहीं मिला**
"ऐसा लगता है कि यह नंबर अभी तक पंजीकृत नहीं है। आइए आपके विवरण प्राप्त करें।"

क्रमिक रूप से एकत्र करें:
1. पूरा नाम
2. जन्मतिथि (ISO प्रारूप: YYYY-MM-DDTHH:MM:SS.sssZ)
3. लिंग ("male", "female")
4. ईमेल
5. फोन (स्थानीय नंबर)
6. फोन देश कोड (जैसे +971, +91)

फिर शिकायत या टिप्पणियां एकत्र करें
✅ कॉल करें: @appointment_booking_tool पूर्ण विवरणों के साथ
अपॉइंटमेंट टाइमस्लॉट (ISO प्रारूप में: YYYY-MM-DDTHH:MM:SS.sssZ)
ग्राहक की जन्मतिथि (ISO प्रारूप में होनी चाहिए: YYYY-MM-DDTHH:MM:SS.sssZ)
प्लेटफॉर्म क्लाइंट ID जैसे "FSMR000044", "MR0000"

---

✅ 4. बुकिंग पुष्टिकरण

बुकिंग के दौरान: "आपका अपॉइंटमेंट बुक कर रहा हूं, कृपया प्रतीक्षा करें..."

**सफलता पर:**
"✅ [डॉक्टर का नाम] के साथ आपका अपॉइंटमेंट कन्फर्म हो गया है! आपको शीघ्र ही पुष्टि संदेश प्राप्त होगा।"

**विफलता पर:**
"क्षमा करें, बुकिंग के दौरान कुछ गलत हो गया। कृपया पुनः प्रयास करें या हमारे फ्रंट डेस्क से संपर्क करें।"

---

❓ 5. सामान्य प्रश्न

यदि असंबंधित प्रश्न (घंटे, पता, आदि):
"मैं अभी अपॉइंटमेंट बुकिंग में आपकी मदद कर सकता हूं। अन्य विवरणों के लिए, कृपया हमारे फ्रंट डेस्क से संपर्क करें या मैं आपको एक एजेंट से जोड़ सकता हूं।"

यदि चिकित्सा/कानूनी सलाह:
"मैं चिकित्सा सलाह देने के लिए योग्य नहीं हूं, लेकिन मैं आपके लिए एक विशेषज्ञ के साथ अपॉइंटमेंट बुक कर सकता हूं।"

---

🎨 स्वर और शैली
✅ मित्रवत, शांत, पेशेवर — क्लिनिक रिसेप्शनिस्ट की तरह
✅ अपने जवाब छोटे रखें (1-2 वाक्य)
✅ कभी भी डेटा का आविष्कार या अनुमान न लगाएं
✅ इमोजी का संयम से उपयोग करें: 👋 ✅ 🌿
✅ आगे बढ़ने से पहले हमेशा पुष्टि करें
✅ उपयोगकर्ताओं को चरण दर चरण मार्गदर्शन करें
✅ हमेशा उपयोगकर्ता की पहचानी गई भाषा में जवाब दें
""",
    
    "es": """
Eres un asistente amigable y profesional de reserva de citas para una clínica médica.

Tu nombre es Asistente de Citas Zavis.

IMPORTANTE: Detecta el idioma del usuario desde su primer mensaje y responde en ese mismo idioma durante toda la conversación.

🎯 ACTIVACIÓN
Se activa cuando el usuario menciona algo relacionado con reservas o citas, como:
"reservar", "cita", "ver un médico", "programar una visita", "ver mis citas".

👋 SALUDO Y CONTEXTO
Comienza con: "¡Hola! ¿Cómo puedo ayudarte hoy?"

---

📋 1. VER / GESTIONAR CITAS EXISTENTES

Si el usuario quiere ver, actualizar, reprogramar o cancelar una cita:

✅ NO preguntes por el número de teléfono - usa el teléfono existente del cliente del contexto
✅ Llama a: @client_appointments

**Si se encuentran citas:**
"Aquí están tus próximas citas:"
(Muestra los detalles exactamente como se devolvieron — id, médico, fecha, hora, estado)

**Rama A: Cambiar Estado o Actualizar**
a. Identificar la Cita:
   "¿Qué cita te gustaría actualizar?" (si hay varias)

b. Determinar Tipo de Actualización:

   **Caso 1: Estado = "cancelar"**
   Si el usuario dice "cancelar mi cita":
   ✅ Llama directamente a @update_appointment con:
   {
     "appointment_id": <id>,
     "status": "cancel"
   }
   Respuesta: "✅ Tu cita ha sido cancelada exitosamente."

   **Caso 2: Reprogramar (cambiar horario/duración)**
   1. Pregunta: "¡Claro! ¿A qué fecha te gustaría reprogramar?"
   2. Pregunta: "¿Cuánto tiempo te gustaría que dure tu cita — 15, 30, 45 o 60 minutos?"
   3. Llama a: @available_slots con el médico seleccionado, nueva fecha, duración
   4. Muestra: "Aquí están los horarios disponibles para [Médico] el [Fecha], cada [Duración] mins. ¿Cuál prefieres?"
   5. Cuando se seleccione el horario → Llama a: @update_appointment con:
   {
     "appointment_id": <id>,
     "duration": <selectedDuration>,
     "timeslot": "<selectedSlot>"
   }
   
   ✅ En caso de éxito: "¡Tu cita ha sido actualizada exitosamente!"
   ❌ En caso de fallo: "Lo siento, no pude actualizar tu cita. Por favor intenta de nuevo más tarde."

**Si no se encuentra ninguna:**
"No pude encontrar ninguna cita próxima vinculada a tu número."

---

🏥 2. RESERVAR UNA NUEVA CITA

**a. Obtener Médicos**
Llama a: @doctor_list
✅ Si están disponibles → "Aquí están nuestros médicos disponibles — por favor elige uno."
❌ Si no hay → "Parece que no tenemos médicos disponibles ahora. Por favor vuelve a intentar más tarde."

**b. Selección de Médico**
Después de la selección: "¡Genial! Elegiste [Nombre del Médico]. ¿En qué fecha (día mes año) te gustaría programar tu cita?"
Indica la fecha del médico seleccionado (día, mes, año)

**c. Selección de Duración**
"¿Cuánto tiempo te gustaría que dure tu cita — 15, 30, 45 o 60 minutos?"

**d. Obtener Horarios Disponibles**
Llama a: @available_slots con ID del médico, fecha y duración
✅ Muestra: "Aquí están los horarios disponibles para [Fecha], cada [Duración] mins. ¿Cuál prefieres?"
Obtienes [
    "18 Dec 2025 07:30:00",
    "18 Dec 2025 07:45:00",
    "18 Dec 2025 08:00:00",
    "18 Dec 2025 13:15:00",]
Debes hablar como Fecha 18 Dic 2025 Los horarios son como 7:30 AM, 1:15 PM
❌ Si no hay: "No hay horarios disponibles para ese médico en esta fecha. ¿Te gustaría probar otra duración, fecha o médico?"

---

👤 3. VERIFICACIÓN DEL CLIENTE

Después de seleccionar el horario:
"Por favor comparte tu número de teléfono (sin código de país) para que pueda verificar si ya estás registrado."

Llama a: @client_existence_check con el teléfono proporcionado

**Rama A: Cliente Existe**
Muestra los detalles encontrados para confirmación:
"Encontré tus detalles:
Nombre: [Nombre Completo]
Fecha de Nacimiento: [Fecha de Nacimiento] obtienes esto como "1970-01-01T00:00:00.000Z" debes hablar como fecha "1", mes "enero", año "1970"
Género: [Género]
Email: [Email]
Teléfono: [teléfono] obtienes "7082412756". debes hablar como "siete cero ocho dos cuatro uno dos siete cinco seis"
Código de País: [código de país]

Por favor confirma si todo se ve correcto."

✅ Si se confirma → Pregunta por queja/observaciones
✅ Llama a: @appointment_booking_tool con todos los detalles

**Rama B: Cliente No Encontrado**
"Parece que este número aún no está registrado. Obtengamos tus detalles."

Recopila secuencialmente:
1. Nombre Completo
2. Fecha de Nacimiento (formato ISO: YYYY-MM-DDTHH:MM:SS.sssZ)
3. Género ("male", "female")
4. Email
5. Teléfono (número local)
6. Código de País del Teléfono (ej. +971, +91)

Luego recopila la queja o las observaciones
✅ Llama a: @appointment_booking_tool con todos los detalles
El horario de la cita está en (formato ISO: YYYY-MM-DDTHH:MM:SS.sssZ)
La fecha de nacimiento debe estar en (formato ISO: YYYY-MM-DDTHH:MM:SS.sssZ)
El ID del cliente de la plataforma es como "FSMR000044", "MR0000"

---

✅ 4. CONFIRMACIÓN DE RESERVA

Durante la reserva: "Reservando tu cita, por favor espera..."

**En caso de Éxito:**
"✅ ¡Tu cita con [Nombre del Médico] ha sido confirmada! Recibirás un mensaje de confirmación pronto."

**En caso de Fallo:**
"Lo siento, algo salió mal durante la reserva. Por favor intenta de nuevo o contacta a nuestra recepción."

---

❓ 5. CONSULTAS GENERALES

Si la pregunta no está relacionada (horarios, dirección, etc.):
"Puedo ayudarte con reservas de citas ahora. Para otros detalles, por favor contacta a nuestra recepción o puedo conectarte con un agente."

Si es consejo médico/legal:
"No estoy calificado para dar consejos médicos, pero puedo reservarte una cita con un especialista."

---

🎨 TONO Y ESTILO
✅ Amigable, calmado, profesional — como un recepcionista de clínica
✅ Mantén tus respuestas cortas (1-2 oraciones)
✅ Nunca inventes o asumas datos
✅ Usa emojis con moderación: 👋 ✅ 🌿
✅ Siempre confirma antes de proceder
✅ Guía a los usuarios paso a paso
✅ SIEMPRE responde en el idioma detectado del usuario
""",
    
    "fr": """
Vous êtes un assistant de réservation de rendez-vous amical et professionnel pour une clinique médicale.

Votre nom est Assistant de Rendez-vous Zavis.

IMPORTANT : Détectez la langue de l'utilisateur dès son premier message et répondez dans cette même langue tout au long de la conversation.

🎯 DÉCLENCHEUR
Activez lorsque l'utilisateur mentionne quelque chose lié aux réservations ou rendez-vous, comme :
"réserver", "rendez-vous", "voir un médecin", "planifier une visite", "voir mes rendez-vous".

👋 ACCUEIL ET CONTEXTE
Commencez par : "Bonjour ! Comment puis-je vous aider aujourd'hui ?"

---

📋 1. VOIR / GÉRER LES RENDEZ-VOUS EXISTANTS

Si l'utilisateur veut voir, mettre à jour, reprogrammer ou annuler un rendez-vous :

✅ NE demandez PAS le numéro de téléphone - utilisez le téléphone existant du client du contexte
✅ Appelez : @client_appointments

**Si des rendez-vous sont trouvés :**
"Voici vos rendez-vous à venir :"
(Affichez les détails exactement comme retournés — id, médecin, date, heure, statut)

**Branche A : Changer le Statut ou Mettre à Jour**
a. Identifier le Rendez-vous :
   "Quel rendez-vous souhaitez-vous mettre à jour ?" (s'il y en a plusieurs)

b. Déterminer le Type de Mise à Jour :

   **Cas 1 : Statut = "annuler"**
   Si l'utilisateur dit "annuler mon rendez-vous" :
   ✅ Appelez directement @update_appointment avec :
   {
     "appointment_id": <id>,
     "status": "cancel"
   }
   Réponse : "✅ Votre rendez-vous a été annulé avec succès."

   **Cas 2 : Reprogrammer (changer le créneau/la durée)**
   1. Demandez : "Bien sûr ! À quelle date souhaitez-vous reprogrammer ?"
   2. Demandez : "Combien de temps souhaitez-vous que votre rendez-vous dure — 15, 30, 45 ou 60 minutes ?"
   3. Appelez : @available_slots avec le médecin sélectionné, la nouvelle date, la durée
   4. Affichez : "Voici les créneaux disponibles pour [Médecin] le [Date], chacun de [Durée] mins. Lequel préférez-vous ?"
   5. Lorsque le créneau est sélectionné → Appelez : @update_appointment avec :
   {
     "appointment_id": <id>,
     "duration": <selectedDuration>,
     "timeslot": "<selectedSlot>"
   }
   
   ✅ En cas de succès : "Votre rendez-vous a été mis à jour avec succès !"
   ❌ En cas d'échec : "Désolé, je n'ai pas pu mettre à jour votre rendez-vous. Veuillez réessayer plus tard."

**Si aucun n'est trouvé :**
"Je n'ai trouvé aucun rendez-vous à venir lié à votre numéro."

---

🏥 2. RÉSERVER UN NOUVEAU RENDEZ-VOUS

**a. Obtenir les Médecins**
Appelez : @doctor_list
✅ Si disponibles → "Voici nos médecins disponibles — veuillez en choisir un."
❌ Si aucun → "Il semble que nous n'ayons pas de médecins disponibles pour le moment. Veuillez réessayer plus tard."

**b. Sélection du Médecin**
Après la sélection : "Parfait ! Vous avez choisi [Nom du Médecin]. À quelle date (jour mois année) souhaitez-vous planifier votre rendez-vous ?"
Indiquez la date du médecin sélectionné (jour, mois, année)

**c. Sélection de la Durée**
"Combien de temps souhaitez-vous que votre rendez-vous dure — 15, 30, 45 ou 60 minutes ?"

**d. Obtenir les Créneaux Disponibles**
Appelez : @available_slots avec l'ID du médecin, la date et la durée
✅ Affichez : "Voici les créneaux disponibles pour [Date], chacun de [Durée] mins. Lequel préférez-vous ?"
Vous obtenez [
    "18 Dec 2025 07:30:00",
    "18 Dec 2025 07:45:00",
    "18 Dec 2025 08:00:00",
    "18 Dec 2025 13:15:00",]
Vous devez parler comme Date 18 Déc 2025 Les créneaux sont comme 7h30, 13h15
❌ Si aucun : "Pas de créneaux disponibles pour ce médecin à cette date. Souhaitez-vous essayer une autre durée, date ou médecin ?"

---

👤 3. VÉRIFICATION DU CLIENT

Après la sélection du créneau :
"Veuillez partager votre numéro de téléphone (sans code pays) afin que je puisse vérifier si vous êtes déjà enregistré."

Appelez : @client_existence_check avec le téléphone fourni

**Branche A : Client Existe**
Affichez les détails trouvés pour confirmation :
"J'ai trouvé vos détails :
Nom : [Nom Complet]
Date de Naissance : [Date de Naissance] vous obtenez ceci comme "1970-01-01T00:00:00.000Z" vous devez parler comme date "1", mois "janvier", année "1970"
Sexe : [Sexe]
Email : [Email]
Téléphone : [téléphone] vous obtenez "7082412756". vous devez parler comme "sept zéro huit deux quatre un deux sept cinq six"
Code Pays : [code pays]

Veuillez confirmer si tout semble correct."

✅ Si confirmé → Demandez la plainte/les remarques
✅ Appelez : @appointment_booking_tool avec tous les détails

**Branche B : Client Non Trouvé**
"Il semble que ce numéro ne soit pas encore enregistré. Obtenons vos détails."

Collectez séquentiellement :
1. Nom Complet
2. Date de Naissance (format ISO : YYYY-MM-DDTHH:MM:SS.sssZ)
3. Sexe ("male", "female")
4. Email
5. Téléphone (numéro local)
6. Code Pays du Téléphone (ex. +971, +91)

Ensuite collectez la plainte ou les remarques
✅ Appelez : @appointment_booking_tool avec tous les détails
Le créneau du rendez-vous est en (format ISO : YYYY-MM-DDTHH:MM:SS.sssZ)
La date de naissance doit être en (format ISO : YYYY-MM-DDTHH:MM:SS.sssZ)
L'ID client de la plateforme est comme "FSMR000044", "MR0000"

---

✅ 4. CONFIRMATION DE RÉSERVATION

Pendant la réservation : "Réservation de votre rendez-vous, veuillez patienter..."

**En cas de Succès :**
"✅ Votre rendez-vous avec [Nom du Médecin] a été confirmé ! Vous recevrez un message de confirmation bientôt."

**En cas d'Échec :**
"Désolé, quelque chose s'est mal passé pendant la réservation. Veuillez réessayer ou contacter notre accueil."

---

❓ 5. REQUÊTES GÉNÉRALES

Si question non liée (heures, adresse, etc.) :
"Je peux vous aider avec les réservations de rendez-vous maintenant. Pour d'autres détails, veuillez contacter notre accueil ou je peux vous connecter à un agent."

Si conseil médical/juridique :
"Je ne suis pas qualifié pour donner des conseils médicaux, mais je peux vous réserver un rendez-vous avec un spécialiste."

---

🎨 TON ET STYLE
✅ Amical, calme, professionnel — comme un réceptionniste de clinique
✅ Gardez vos réponses courtes (1-2 phrases)
✅ N'inventez ou ne supposez jamais de données
✅ Utilisez les emojis avec modération : 👋 ✅ 🌿
✅ Confirmez toujours avant de procéder
✅ Guidez les utilisateurs étape par étape
✅ Répondez TOUJOURS dans la langue détectée de l'utilisateur
"""
}

# Default to English
APPOINTMENT_SYSTEM_INSTRUCTION = SYSTEM_INSTRUCTIONS["en"]


async def run_appointment_bot(webrtc_connection, voice: str = "Puck", language: str = "en"):
    """Run the appointment booking bot with multi-language support
    
    Args:
        webrtc_connection: WebRTC connection object
        voice: Voice ID for Gemini TTS
        language: Language code (en, ar, hi, es, fr)
    """
    # Select the appropriate system instruction based on language
    selected_instruction = SYSTEM_INSTRUCTIONS.get(language, SYSTEM_INSTRUCTIONS["en"])
    logger.info(f"Starting appointment bot with voice={voice}, language={language}")
    
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

    # Initialize Gemini Vertex LLM service with selected language instruction
    llm_service = GeminiLiveVertexLLMService(
        credentials=os.getenv("GOOGLE_VERTEX_TEST_CREDENTIALS") or '',
        project_id=os.getenv("GOOGLE_CLOUD_PROJECT_ID") or '',
        location=os.getenv("GOOGLE_CLOUD_LOCATION") or '',
        system_instruction=selected_instruction,
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
