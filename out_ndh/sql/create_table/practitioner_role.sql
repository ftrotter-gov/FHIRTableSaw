CREATE TABLE IF NOT EXISTS "public"."practitioner_role" (
  "id" bigserial NOT NULL,
  "fhir_id" text NOT NULL,
  "resource_type" text,
  "language" text,
  "text_status" text,
  "text_div" text,
  "active" boolean,
  "period_end" timestamp,
  "period_start" timestamp,
  "practitioner_type" text,
  "organization_type" text,
  "organization_id" bigint,
  "practitioner_id" bigint,
  "verification_status" text,
  "identifier_status" text,
  "identifier_system" text,
  "identifier_value" text,
  "coding_system" text,
  "coding_code" text,
  "coding_display" text,
  "coding_system" text,
  "coding_code" text,
  "coding_display" text,
  CONSTRAINT "practitioner_role_pk" PRIMARY KEY ("id"),
  CONSTRAINT "practitioner_role_fhir_id_uniq" UNIQUE NULLS NOT DISTINCT ("fhir_id")
);
CREATE INDEX IF NOT EXISTS "practitioner_role_fhir_id_idx" ON "public"."practitioner_role" ("fhir_id");
CREATE INDEX IF NOT EXISTS "practitioner_role_organization_id_idx" ON "public"."practitioner_role" ("organization_id");
CREATE INDEX IF NOT EXISTS "practitioner_role_practitioner_id_idx" ON "public"."practitioner_role" ("practitioner_id");
