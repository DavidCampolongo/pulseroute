-- CreateEnum
CREATE TYPE "UserRole" AS ENUM ('ADMIN', 'DISPATCHER', 'VIEWER');

-- CreateEnum
CREATE TYPE "OperatorStatus" AS ENUM ('AVAILABLE', 'UNAVAILABLE', 'INACTIVE');

-- CreateEnum
CREATE TYPE "ServiceRequestStatus" AS ENUM ('PENDING', 'ASSIGNED', 'UNROUTABLE', 'CANCELLED');

-- CreateEnum
CREATE TYPE "ServiceRequestPriority" AS ENUM ('LOW', 'NORMAL', 'HIGH');

-- CreateEnum
CREATE TYPE "AssignmentStatus" AS ENUM ('ACTIVE', 'CANCELLED');

-- CreateTable
CREATE TABLE "organizations" (
    "id" UUID NOT NULL,
    "name" TEXT NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "organizations_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "users" (
    "id" UUID NOT NULL,
    "organization_id" UUID NOT NULL,
    "email" TEXT NOT NULL,
    "password_hash" TEXT NOT NULL,
    "role" "UserRole" NOT NULL DEFAULT 'VIEWER',
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "users_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "operators" (
    "id" UUID NOT NULL,
    "organization_id" UUID NOT NULL,
    "name" TEXT NOT NULL,
    "status" "OperatorStatus" NOT NULL DEFAULT 'UNAVAILABLE',
    "region" TEXT NOT NULL,
    "max_concurrent_assignments" INTEGER NOT NULL DEFAULT 1,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "operators_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "skills" (
    "id" UUID NOT NULL,
    "organization_id" UUID NOT NULL,
    "name" TEXT NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "skills_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "operator_skills" (
    "id" UUID NOT NULL,
    "organization_id" UUID NOT NULL,
    "operator_id" UUID NOT NULL,
    "skill_id" UUID NOT NULL,
    "level" INTEGER NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "operator_skills_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "service_requests" (
    "id" UUID NOT NULL,
    "organization_id" UUID NOT NULL,
    "external_id" TEXT NOT NULL,
    "required_skill_id" UUID NOT NULL,
    "status" "ServiceRequestStatus" NOT NULL DEFAULT 'PENDING',
    "priority" "ServiceRequestPriority" NOT NULL DEFAULT 'NORMAL',
    "region" TEXT NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "service_requests_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "assignments" (
    "id" UUID NOT NULL,
    "organization_id" UUID NOT NULL,
    "service_request_id" UUID NOT NULL,
    "operator_id" UUID NOT NULL,
    "status" "AssignmentStatus" NOT NULL DEFAULT 'ACTIVE',
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "assignments_pkey" PRIMARY KEY ("id")
);

-- AddCheckConstraint
ALTER TABLE "operators" ADD CONSTRAINT "operators_max_concurrent_assignments_positive_check" CHECK ("max_concurrent_assignments" > 0);
ALTER TABLE "operator_skills" ADD CONSTRAINT "operator_skills_level_valid_range_check" CHECK ("level" BETWEEN 1 AND 5);

-- CreateIndex
CREATE UNIQUE INDEX "users_organization_id_email_key" ON "users"("organization_id", "email");

-- CreateIndex
CREATE INDEX "operators_organization_id_status_region_idx" ON "operators"("organization_id", "status", "region");

-- CreateIndex
CREATE UNIQUE INDEX "operators_id_organization_id_key" ON "operators"("id", "organization_id");

-- CreateIndex
CREATE UNIQUE INDEX "skills_organization_id_name_key" ON "skills"("organization_id", "name");

-- CreateIndex
CREATE UNIQUE INDEX "skills_id_organization_id_key" ON "skills"("id", "organization_id");

-- CreateIndex
CREATE INDEX "operator_skills_organization_id_skill_id_idx" ON "operator_skills"("organization_id", "skill_id");

-- CreateIndex
CREATE UNIQUE INDEX "operator_skills_operator_id_skill_id_key" ON "operator_skills"("operator_id", "skill_id");

-- CreateIndex
CREATE INDEX "service_requests_organization_id_status_created_at_idx" ON "service_requests"("organization_id", "status", "created_at");

-- CreateIndex
CREATE INDEX "service_requests_organization_id_priority_created_at_idx" ON "service_requests"("organization_id", "priority", "created_at");

-- CreateIndex
CREATE INDEX "service_requests_organization_id_region_created_at_idx" ON "service_requests"("organization_id", "region", "created_at");

-- CreateIndex
CREATE INDEX "service_requests_organization_id_required_skill_id_created__idx" ON "service_requests"("organization_id", "required_skill_id", "created_at");

-- CreateIndex
CREATE UNIQUE INDEX "service_requests_organization_id_external_id_key" ON "service_requests"("organization_id", "external_id");

-- CreateIndex
CREATE UNIQUE INDEX "service_requests_id_organization_id_key" ON "service_requests"("id", "organization_id");

-- CreateIndex
CREATE INDEX "assignments_organization_id_operator_id_status_idx" ON "assignments"("organization_id", "operator_id", "status");

-- CreateIndex
CREATE INDEX "assignments_organization_id_service_request_id_created_at_idx" ON "assignments"("organization_id", "service_request_id", "created_at");

-- AddForeignKey
ALTER TABLE "users" ADD CONSTRAINT "users_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "organizations"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "operators" ADD CONSTRAINT "operators_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "organizations"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "skills" ADD CONSTRAINT "skills_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "organizations"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "operator_skills" ADD CONSTRAINT "operator_skills_operator_id_organization_id_fkey" FOREIGN KEY ("operator_id", "organization_id") REFERENCES "operators"("id", "organization_id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "operator_skills" ADD CONSTRAINT "operator_skills_skill_id_organization_id_fkey" FOREIGN KEY ("skill_id", "organization_id") REFERENCES "skills"("id", "organization_id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "service_requests" ADD CONSTRAINT "service_requests_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "organizations"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "service_requests" ADD CONSTRAINT "service_requests_required_skill_id_organization_id_fkey" FOREIGN KEY ("required_skill_id", "organization_id") REFERENCES "skills"("id", "organization_id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "assignments" ADD CONSTRAINT "assignments_service_request_id_organization_id_fkey" FOREIGN KEY ("service_request_id", "organization_id") REFERENCES "service_requests"("id", "organization_id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "assignments" ADD CONSTRAINT "assignments_operator_id_organization_id_fkey" FOREIGN KEY ("operator_id", "organization_id") REFERENCES "operators"("id", "organization_id") ON DELETE RESTRICT ON UPDATE CASCADE;
