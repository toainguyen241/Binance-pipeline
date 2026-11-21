FROM python:3.14-slim@sha256:ce40764625a4ff50df3548277632e7f96c4e77fe75fa848aae9885476e7df5a4

WORKDIR /app

RUN pip install --no-cache-dir uv==0.13.4

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --extra dashboard

COPY src/dashboard ./src/dashboard

EXPOSE 8000

CMD ["uv", "run", "--frozen", "--no-sync", "uvicorn", "src.dashboard.main:app", "--host", "0.0.0.0", "--port", "8000"]
