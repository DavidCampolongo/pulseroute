# syntax=docker/dockerfile:1

ARG NODE_IMAGE=node:24.18.0-bookworm-slim

FROM ${NODE_IMAGE} AS build

ENV PNPM_HOME="/pnpm"
ENV PATH="$PNPM_HOME:$PATH"

WORKDIR /workspace

RUN corepack enable \
    && corepack install --global pnpm@11.12.0

COPY . .

RUN pnpm install --frozen-lockfile

RUN pnpm --filter @pulseroute/fake-receiver run build

RUN pnpm \
    --filter @pulseroute/fake-receiver \
    --prod \
    deploy \
    --legacy \
    /prod/fake-receiver

FROM ${NODE_IMAGE} AS runtime

ENV NODE_ENV=production

WORKDIR /app

COPY --from=build --chown=node:node /prod/fake-receiver/ ./

USER node

STOPSIGNAL SIGTERM

CMD ["node", "dist/cli.js"]
