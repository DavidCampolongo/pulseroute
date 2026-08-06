# syntax=docker/dockerfile:1

ARG NODE_IMAGE=node:24.18.0-bookworm-slim

FROM ${NODE_IMAGE} AS build

ENV PNPM_HOME="/pnpm"
ENV PATH="$PNPM_HOME:$PATH"

WORKDIR /workspace

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
      g++ \
      make \
      python3 \
    && rm -rf /var/lib/apt/lists/*

RUN corepack enable \
    && corepack install --global pnpm@11.12.0

COPY . .

RUN pnpm install --frozen-lockfile

# Prisma configuration requires DATABASE_URL while generating the client,
# but generation does not connect to this build-only placeholder database.
RUN DATABASE_URL=postgresql://pulseroute:build-only@127.0.0.1:5432/pulseroute \
    pnpm --filter @pulseroute/db run db:generate

RUN pnpm --filter @pulseroute/db run build \
    && pnpm --filter @pulseroute/shared run build \
    && pnpm --filter @pulseroute/scoring run build \
    && pnpm --filter @pulseroute/worker run build

RUN pnpm \
    --filter @pulseroute/worker \
    --prod \
    deploy \
    --legacy \
    /prod/worker

FROM ${NODE_IMAGE} AS runtime

ENV NODE_ENV=production

WORKDIR /app/apps/worker

COPY --from=build --chown=node:node /prod/worker/ ./

USER node

STOPSIGNAL SIGTERM

CMD ["node", "dist/index.js"]