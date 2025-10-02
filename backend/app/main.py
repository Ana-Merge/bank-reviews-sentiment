import uvicorn
from app.core.setup import create_app
from app.core.settings import AppSettings

app = create_app()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8005)
