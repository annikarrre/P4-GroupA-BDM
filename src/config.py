import os

POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://postgres:postgres@localhost:5432/postgres",
)

MINIO_URL = os.getenv("MINIO_URL", "http://localhost:9000")
MINIO_KEY = os.getenv("MINIO_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "rico-raw")

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
LLM_MODEL = os.getenv("LLM_MODEL", "qwen2.5:3b")

CLIP_ARCH = "ViT-B-32"
CLIP_PRETRAINED = "laion2b_s34b_b79k"
CLIP_MODEL_VERSION = f"open-clip-{CLIP_ARCH}-{CLIP_PRETRAINED}"

SBERT_MODEL_VERSION = "sentence-transformers/all-MiniLM-L6-v2"

PROMPT_VERSION = "v1"