# =============================================================================
# Evidence Docker image for FHIRTableSaw reports
# =============================================================================
# Multi-stage build:
#   Stage 1 (base)   – install Node dependencies
#   Stage 2 (dev)    – run Evidence dev server (for local development)
#   Stage 3 (build)  – extract sources & build static site
#   Stage 4 (static) – serve the static build with a minimal image
#
# Usage:
#   Development:  docker compose up evidence-dev
#   Static build: docker compose run evidence-build
#   See docker-compose.yml and EVIDENCE_REPORTS.md for full details.
# =============================================================================

# ---------------------------------------------------------------------------
# Stage 1: base – install dependencies
# ---------------------------------------------------------------------------
FROM node:20-slim AS base

WORKDIR /app

# Copy only dependency-related files first for better layer caching
COPY package.json package-lock.json* ./
COPY .npmrc ./

RUN npm ci --prefer-offline 2>/dev/null || npm install

# Copy the rest of the Evidence project
COPY . .

# ---------------------------------------------------------------------------
# Stage 2: dev – Evidence development server
# ---------------------------------------------------------------------------
FROM base AS dev

EXPOSE 3000

# The dev server runs sources automatically on startup
CMD ["npm", "run", "dev"]

# ---------------------------------------------------------------------------
# Stage 3: build – extract sources and build static site for GitHub Pages
# ---------------------------------------------------------------------------
FROM base AS build

# Build arguments for GitHub Pages base path (defaults to repo name)
ARG BASE_PATH="/FHIRTableSaw"
ENV BASE_PATH=${BASE_PATH}

# Run source extraction and build
# The EVIDENCE_BUILD_DIR makes the output land in build/<base_path> for GH Pages
RUN npm run sources && \
    EVIDENCE_BUILD_DIR=./build${BASE_PATH} npm run build

# ---------------------------------------------------------------------------
# Stage 4: static – serve the built site with a tiny web server
# ---------------------------------------------------------------------------
FROM node:20-slim AS static

WORKDIR /app

RUN npm install -g serve@14

ARG BASE_PATH="/FHIRTableSaw"
COPY --from=build /app/build /app/build

EXPOSE 3000

CMD ["serve", "build", "-l", "3000", "--single"]
