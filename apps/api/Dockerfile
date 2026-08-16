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
    && pnpm --filter @pulseroute/api run build

RUN pnpm \
    --filter @pulseroute/api \
    --prod \
    deploy \
    --legacy \
    /prod/api

FROM ${NODE_IMAGE} AS runtime

ENV NODE_ENV=production

WORKDIR /app/apps/api

RUN apt-get update \
    && apt-get install --yes --no-install-recommends openssl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build --chown=node:node /prod/api/ ./

# Keep production migrations and the one-time fictional seed in the same
# package tree as their pinned CLI/runtime dependencies. Railway runs only
# `npm run db:migrate:deploy` automatically; seeding remains explicit.
COPY --from=build --chown=node:node \
  /workspace/packages/db/prisma.config.ts \
  ./prisma.config.ts
COPY --from=build --chown=node:node \
  /workspace/packages/db/prisma/ \
  ./prisma/
COPY --from=build --chown=node:node \
  /workspace/packages/db/src/generated/ \
  ./src/generated/

USER node

STOPSIGNAL SIGTERM

CMD ["node", "dist/index.js"]
