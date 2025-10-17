# 🎯 Gemini Vertex Integration - Setup Guide

## ✅ Changes You Made

Great job adding Gemini Vertex support! Here's what you implemented:

### Backend (`bot.py`):
```python
elif model == "gemini_vertex_llm":
    llm_service = GeminiLiveVertexLLMService(
        credentials=os.getenv("GOOGLE_VERTEX_TEST_CREDENTIALS") or '',
        project_id=os.getenv("GOOGLE_CLOUD_PROJECT_ID") or '',
        location=os.getenv("GOOGLE_CLOUD_LOCATION") or '',
        system_instruction=SYSTEM_INSTRUCTION,
        voice_id=voice,
    )
```

### Frontend (`index.html`):
- Added "Gemini Vertex" option to model dropdown
- Added voice handling (same voices as Gemini Live: Puck, Aoede, Charon, Fenrir, Kore)

---

## 🔧 Required Configuration

### Step 1: Get Google Cloud Credentials

1. **Go to Google Cloud Console:**
   https://console.cloud.google.com

2. **Create/Select Project:**
   - Create a new project or select existing
   - Note your PROJECT_ID

3. **Enable Vertex AI API:**
   - Go to "APIs & Services" → "Enable APIs and Services"
   - Search for "Vertex AI API"
   - Click "Enable"

4. **Create Service Account:**
   ```
   IAM & Admin → Service Accounts → Create Service Account
   Name: gemini-vertex-bot
   Role: Vertex AI User
   ```

5. **Generate Key:**
   - Click on the service account
   - "Keys" tab → "Add Key" → "Create new key"
   - Choose JSON format
   - Download the JSON file

---

## 🚀 Configure Railway

### Option A: Using Service Account JSON (Recommended)

1. **Prepare the credentials:**
   - Open the downloaded JSON file
   - Copy the entire JSON content (it's a single line)

2. **Add to Railway:**
   - Go to Railway dashboard
   - Your service → "Variables" tab
   - Add these variables:

   ```
   GOOGLE_VERTEX_TEST_CREDENTIALS={"type":"service_account","project_id":"your-project-id",...}
   GOOGLE_CLOUD_PROJECT_ID=your-project-id
   GOOGLE_CLOUD_LOCATION=us-central1
   ```

### Option B: Using Service Account File Path (Local Testing)

For local testing only:
```bash
# Save JSON to file
echo '{"type":"service_account",...}' > vertex-credentials.json

# Add to .env
GOOGLE_VERTEX_TEST_CREDENTIALS=vertex-credentials.json
GOOGLE_CLOUD_PROJECT_ID=your-project-id
GOOGLE_CLOUD_LOCATION=us-central1
```

---

## ⚠️ Potential Issue: Credentials Parameter

The `GeminiLiveVertexLLMService` might expect credentials in different formats:

### Format 1: JSON String
```python
credentials=os.getenv("GOOGLE_VERTEX_TEST_CREDENTIALS")  # JSON string
```

### Format 2: File Path
```python
credentials=os.getenv("GOOGLE_VERTEX_TEST_CREDENTIALS")  # path to .json file
```

### Format 3: Credentials Object
```python
import json
from google.oauth2 import service_account

creds_json = os.getenv("GOOGLE_VERTEX_TEST_CREDENTIALS")
if creds_json:
    creds_dict = json.loads(creds_json)
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
```

---

## 🔍 Check Required: Pipecat API

Let me verify which format pipecat expects. You might need to adjust `bot.py`:

### If it expects a file path:
```python
# Keep as is
credentials=os.getenv("GOOGLE_VERTEX_TEST_CREDENTIALS") or ''
```

### If it expects parsed credentials:
```python
import json

# At the top of run_bot function
vertex_creds = os.getenv("GOOGLE_VERTEX_TEST_CREDENTIALS")
if vertex_creds and model == "gemini_vertex_llm":
    try:
        # Try parsing as JSON first
        creds_dict = json.loads(vertex_creds)
        # pipecat might need the dict or might create credentials from it
        credentials = creds_dict
    except json.JSONDecodeError:
        # If not JSON, assume it's a file path
        credentials = vertex_creds
```

---

## 🧪 Testing Gemini Vertex

### After Configuration:

1. **Deploy to Railway** (already done with your changes)
2. **Add environment variables** in Railway dashboard
3. **Restart the service**
4. **Open Railway URL**
5. **Select "Gemini Vertex" from dropdown**
6. **Select voice**
7. **Click "Connect"**

### Expected Logs:
```
✅ Starting bot with model=gemini_vertex_llm, voice=Puck
✅ Connecting to Gemini service (Vertex)
✅ Connected to Gemini service
✅ Pipecat Client connected
```

---

## 📋 Checklist

Before testing Gemini Vertex:

- [ ] Google Cloud project created
- [ ] Vertex AI API enabled
- [ ] Service account created with "Vertex AI User" role
- [ ] JSON key downloaded
- [ ] Environment variables added to Railway:
  - [ ] GOOGLE_VERTEX_TEST_CREDENTIALS (JSON string)
  - [ ] GOOGLE_CLOUD_PROJECT_ID
  - [ ] GOOGLE_CLOUD_LOCATION
- [ ] Railway service restarted
- [ ] Test connection from UI

---

## 💰 Costs

**Vertex AI Pricing:**
- Input: ~$0.125 per 1M characters
- Output: ~$0.375 per 1M characters
- Very affordable for testing/demos

**Free Tier:**
- Google Cloud offers $300 credit for new accounts
- Plenty for testing

---

## 🐛 Troubleshooting

### Error: "Invalid credentials"
- Check JSON format in environment variable
- Verify service account has Vertex AI permissions
- Check project ID matches

### Error: "API not enabled"
- Enable Vertex AI API in Google Cloud Console
- Wait 1-2 minutes for propagation

### Error: "Permission denied"
- Service account needs "Vertex AI User" role
- Or "Vertex AI Administrator" for full access

### Connection works but no audio
- Check voice_id is correct (Puck, Aoede, etc.)
- Check system_instruction is set
- View Railway logs for Gemini errors

---

## 🔄 Commit Your Changes

Your changes look good! Let's commit them:

```bash
git add bot.py index.html
git commit -m "Add Gemini Vertex LLM support with voice selection"
git push
```

---

## ✅ Summary

**What works:**
- ✅ Code structure is correct
- ✅ Model selection in UI
- ✅ Voice options properly configured
- ✅ Backend routing to GeminiLiveVertexLLMService

**What's needed:**
- ⚠️ Add Google Cloud credentials to Railway
- ⚠️ Verify credentials format (JSON vs file path)
- ⚠️ Test after configuration

**Next steps:**
1. Add environment variables to Railway
2. Commit and push your changes
3. Test all three models:
   - Gemini Live ✅
   - OpenAI Realtime ✅
   - Gemini Vertex 🆕

Great work adding this feature! Let me know if you need help with the Google Cloud setup or credentials configuration.
