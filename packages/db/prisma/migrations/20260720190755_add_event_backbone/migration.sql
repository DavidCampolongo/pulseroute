/*
  Warnings:

  - A unique constraint covering the columns `[id,organization_id]` on the table `assignments` will be added. If there are existing duplicate values, this will fail.
  - A unique constraint covering the columns `[id,organization_id]` on the table `users` will be added. If there are existing duplicate values, this will fail.

*/
-- CreateEnum
CREATE TYPE "WebhookEventStatus" AS ENUM ('PROCESSED', 'MALFORMED');

-- CreateEnum
CREATE TYPE "OutboxEventStatus" AS ENUM ('PENDING', 'PROCESSING', 'DELIVERED', 'EXHAUSTED');

-- CreateEnum
CREATE TYPE "RoutingDecisionOutcome" AS ENUM ('ASSIGNED', 'UNROUTABLE');

-- CreateEnum
CREATE TYPE "WebhookDeliveryStatus" AS ENUM ('STARTED', 'SUCCEEDED', 'FAILED');

-- CreateEnum
CREATE TYPE "AuditActorType" AS ENUM ('USER', 'SYSTEM');

-- CreateTable
CREATE TABLE "webhook_events" (
    "id" UUID NOT NULL,
    "organization_id" UUID NOT NULL,
    "service_request_id" UUID,
    "provider" TEXT NOT NULL,
    "external_event_id" TEXT,
    "status" "WebhookEventStatus" NOT NULL,
    "raw_body" BYTEA NOT NULL,
    "parsed_payload" JSONB,
    "malformed_reason" TEXT,
    "received_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "classified_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "webhook_events_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "outbox_events" (
    "id" UUID NOT NULL,
    "organization_id" UUID NOT NULL,
    "event_type" TEXT NOT NULL,
    "aggregate_type" TEXT NOT NULL,
    "aggregate_id" UUID NOT NULL,
    "status" "OutboxEventStatus" NOT NULL DEFAULT 'PENDING',
    "payload" JSONB NOT NULL,
    "attempt_count" INTEGER NOT NULL DEFAULT 0,
    "next_attempt_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "processing_started_at" TIMESTAMP(3),
    "processed_at" TIMESTAMP(3),
    "last_error" JSONB,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "outbox_events_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "routing_decisions" (
    "id" UUID NOT NULL,
    "organization_id" UUID NOT NULL,
    "service_request_id" UUID NOT NULL,
    "assignment_id" UUID,
    "scoring_version" TEXT NOT NULL,
    "outcome" "RoutingDecisionOutcome" NOT NULL,
    "decision_snapshot" JSONB NOT NULL,
    "decided_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "routing_decisions_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "webhook_deliveries" (
    "id" UUID NOT NULL,
    "organization_id" UUID NOT NULL,
    "outbox_event_id" UUID NOT NULL,
    "attempt_number" INTEGER NOT NULL,
    "status" "WebhookDeliveryStatus" NOT NULL DEFAULT 'STARTED',
    "started_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "completed_at" TIMESTAMP(3),
    "http_status" INTEGER,
    "error_details" JSONB,
    "response_metadata" JSONB,

    CONSTRAINT "webhook_deliveries_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "audit_logs" (
    "id" UUID NOT NULL,
    "organization_id" UUID NOT NULL,
    "actor_type" "AuditActorType" NOT NULL,
    "actor_user_id" UUID,
    "action" TEXT NOT NULL,
    "entity_type" TEXT NOT NULL,
    "entity_id" TEXT NOT NULL,
    "correlation_id" TEXT,
    "metadata" JSONB,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "audit_logs_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "webhook_events_organization_id_status_received_at_idx" ON "webhook_events"("organization_id", "status", "received_at");

-- CreateIndex
CREATE INDEX "webhook_events_organization_id_provider_external_event_id_idx" ON "webhook_events"("organization_id", "provider", "external_event_id");

-- CreateIndex
CREATE INDEX "webhook_events_organization_id_service_request_id_idx" ON "webhook_events"("organization_id", "service_request_id");

-- CreateIndex
CREATE INDEX "outbox_events_status_next_attempt_at_idx" ON "outbox_events"("status", "next_attempt_at");

-- CreateIndex
CREATE INDEX "outbox_events_organization_id_created_at_idx" ON "outbox_events"("organization_id", "created_at");

-- CreateIndex
CREATE INDEX "outbox_events_organization_id_aggregate_type_aggregate_id_idx" ON "outbox_events"("organization_id", "aggregate_type", "aggregate_id");

-- CreateIndex
CREATE UNIQUE INDEX "outbox_events_id_organization_id_key" ON "outbox_events"("id", "organization_id");

-- CreateIndex
CREATE INDEX "routing_decisions_organization_id_service_request_id_decide_idx" ON "routing_decisions"("organization_id", "service_request_id", "decided_at");

-- CreateIndex
CREATE INDEX "routing_decisions_organization_id_outcome_decided_at_idx" ON "routing_decisions"("organization_id", "outcome", "decided_at");

-- CreateIndex
CREATE UNIQUE INDEX "routing_decisions_assignment_id_organization_id_key" ON "routing_decisions"("assignment_id", "organization_id");

-- CreateIndex
CREATE INDEX "webhook_deliveries_organization_id_status_started_at_idx" ON "webhook_deliveries"("organization_id", "status", "started_at");

-- CreateIndex
CREATE UNIQUE INDEX "webhook_deliveries_organization_id_outbox_event_id_attempt__key" ON "webhook_deliveries"("organization_id", "outbox_event_id", "attempt_number");

-- CreateIndex
CREATE INDEX "audit_logs_organization_id_created_at_idx" ON "audit_logs"("organization_id", "created_at");

-- CreateIndex
CREATE INDEX "audit_logs_organization_id_entity_type_entity_id_created_at_idx" ON "audit_logs"("organization_id", "entity_type", "entity_id", "created_at");

-- CreateIndex
CREATE INDEX "audit_logs_organization_id_action_created_at_idx" ON "audit_logs"("organization_id", "action", "created_at");

-- CreateIndex
CREATE UNIQUE INDEX "assignments_id_organization_id_key" ON "assignments"("id", "organization_id");

-- CreateIndex
CREATE UNIQUE INDEX "users_id_organization_id_key" ON "users"("id", "organization_id");

-- AddForeignKey
ALTER TABLE "webhook_events" ADD CONSTRAINT "webhook_events_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "organizations"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "webhook_events" ADD CONSTRAINT "webhook_events_service_request_id_organization_id_fkey" FOREIGN KEY ("service_request_id", "organization_id") REFERENCES "service_requests"("id", "organization_id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "outbox_events" ADD CONSTRAINT "outbox_events_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "organizations"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "routing_decisions" ADD CONSTRAINT "routing_decisions_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "organizations"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "routing_decisions" ADD CONSTRAINT "routing_decisions_service_request_id_organization_id_fkey" FOREIGN KEY ("service_request_id", "organization_id") REFERENCES "service_requests"("id", "organization_id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "routing_decisions" ADD CONSTRAINT "routing_decisions_assignment_id_organization_id_fkey" FOREIGN KEY ("assignment_id", "organization_id") REFERENCES "assignments"("id", "organization_id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "webhook_deliveries" ADD CONSTRAINT "webhook_deliveries_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "organizations"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "webhook_deliveries" ADD CONSTRAINT "webhook_deliveries_outbox_event_id_organization_id_fkey" FOREIGN KEY ("outbox_event_id", "organization_id") REFERENCES "outbox_events"("id", "organization_id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "audit_logs" ADD CONSTRAINT "audit_logs_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "organizations"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "audit_logs" ADD CONSTRAINT "audit_logs_actor_user_id_organization_id_fkey" FOREIGN KEY ("actor_user_id", "organization_id") REFERENCES "users"("id", "organization_id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddCheckConstraint
ALTER TABLE "routing_decisions"
ADD CONSTRAINT "routing_decisions_outcome_assignment_check"
CHECK (
    ("outcome" = 'ASSIGNED' AND "assignment_id" IS NOT NULL)
    OR
    ("outcome" = 'UNROUTABLE' AND "assignment_id" IS NULL)
);

-- AddCheckConstraint
ALTER TABLE "audit_logs"
ADD CONSTRAINT "audit_logs_actor_identity_check"
CHECK (
    ("actor_type" = 'USER' AND "actor_user_id" IS NOT NULL)
    OR
    ("actor_type" = 'SYSTEM' AND "actor_user_id" IS NULL)
);

-- AddCheckConstraint
ALTER TABLE "outbox_events"
ADD CONSTRAINT "outbox_events_attempt_count_nonnegative_check"
CHECK ("attempt_count" >= 0);

-- AddCheckConstraint
ALTER TABLE "webhook_deliveries"
ADD CONSTRAINT "webhook_deliveries_attempt_number_positive_check"
CHECK ("attempt_number" > 0);

-- AddCheckConstraint
ALTER TABLE "webhook_deliveries"
ADD CONSTRAINT "webhook_deliveries_http_status_range_check"
CHECK (
    "http_status" IS NULL
    OR "http_status" BETWEEN 100 AND 599
);

-- AddCheckConstraint
ALTER TABLE "webhook_deliveries"
ADD CONSTRAINT "webhook_deliveries_completion_check"
CHECK (
    ("status" = 'STARTED' AND "completed_at" IS NULL)
    OR
    ("status" IN ('SUCCEEDED', 'FAILED') AND "completed_at" IS NOT NULL)
);
