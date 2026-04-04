from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import os

load_dotenv()

client = AsyncIOMotorClient(os.getenv("MONGODB_URI"))
db = client.antonrx

policies = db.policies
policy_versions = db.policy_versions
policy_changelogs = db.policy_changelogs
extraction_audit_log = db.extraction_audit_log