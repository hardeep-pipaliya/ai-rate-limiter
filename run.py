# run.py
from app import create_app

app = create_app()

if __name__ == "__main__":
    # In development you can use Flask's reloader; in production use gunicorn:
    app.run(host="0.0.0.0", port=8501, debug=True)