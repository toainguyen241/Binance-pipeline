FROM python:3.14-slim@sha256:9aaa474097d094040649f2162a627c0049b408f52138d980916788d80f04802f

WORKDIR /app

RUN pip install --no-cache-dir uv==0.13.4

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --extra dashboard

COPY src/dashboard ./src/dashboard

EXPOSE 8000

CMD ["uv", "run", "--frozen", "--no-sync", "uvicorn", "src.dashboard.main:app", "--host", "0.0.0.0", "--port", "8000"]
