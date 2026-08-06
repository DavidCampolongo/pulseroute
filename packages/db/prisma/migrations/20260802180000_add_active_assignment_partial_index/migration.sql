CREATE UNIQUE INDEX "assignments_service_request_id_active_key"
ON "assignments" ("service_request_id")
WHERE "status" = 'ACTIVE'::"AssignmentStatus";
