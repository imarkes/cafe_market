FROM python:3.12-slim-bookworm AS runtime

ARG UV_VERSION=0.8.14

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH="/app/.venv/bin:${PATH}" \
    SPARK_LOCAL_IP=127.0.0.1 \
    HOME=/home/appuser

WORKDIR /app

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        openjdk-17-jre-headless \
        curl \
        ca-certificates \
        tini \
    && rm -rf /var/lib/apt/lists/*

# Instala uma versão fixa do uv.
RUN curl -LsSf "https://astral.sh/uv/${UV_VERSION}/install.sh" | sh \
    && mv /root/.local/bin/uv /usr/local/bin/uv \
    && uv --version

# Copiar primeiro os arquivos de dependências melhora o cache do build.
COPY pyproject.toml uv.lock ./

# Instala apenas as dependências.
RUN uv sync \
    --frozen \
    --no-dev \
    --no-install-project

# Copia o restante do projeto.
COPY app ./app
COPY main.py ./main.py

# Cria os diretórios persistentes.
RUN mkdir -p \
        /app/storage/raw \
        /app/storage/bronze \
        /app/storage/silver \
        /app/storage/gold \
        /app/storage/meta \
        /app/logs \
        /home/appuser/.ivy2 \
    && groupadd --system appgroup \
    && useradd \
        --system \
        --gid appgroup \
        --home-dir /home/appuser \
        --create-home \
        appuser \
    && chown -R appuser:appgroup \
        /app \
        /home/appuser

USER appuser

ENTRYPOINT ["/usr/bin/tini", "--"]

# CMD ["python", "-m", "app.src.run"]
CMD ["python", "main.py"]
