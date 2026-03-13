CREATE TABLE IF NOT EXISTS "public"."practitioner_role_healthcare_service" (
  "id" bigserial NOT NULL,
  "practitioner_role_id" bigint NOT NULL,
  "healthcare_service_id" bigint NOT NULL,
  CONSTRAINT "practitioner_role_healthcare_service_pk" PRIMARY KEY ("id"),
  CONSTRAINT "practitioner_role_healthcare_service_uniq" UNIQUE NULLS NOT DISTINCT ("practitioner_role_id", "healthcare_service_id")
);
CREATE INDEX IF NOT EXISTS "practitioner_role_healthcare_service_practitioner_role_idx" ON "public"."practitioner_role_healthcare_service" ("practitioner_role_id");
CREATE INDEX IF NOT EXISTS "practitioner_role_healthcare_service_healthcare_service_idx" ON "public"."practitioner_role_healthcare_service" ("healthcare_service_id");
