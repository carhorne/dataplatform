FROM python:3.12-slim

WORKDIR /app

# Install uv
RUN pip install uv

# Copy dependency files first (better layer caching)
COPY pyproject.toml .

# Install dependencies
RUN uv sync

# Copy the rest of the application
COPY . .

CMD ["uv", "run", "python", "main.py"]