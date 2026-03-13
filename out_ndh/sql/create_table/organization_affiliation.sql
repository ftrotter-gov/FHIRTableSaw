CREATE TABLE IF NOT EXISTS "public"."organization_affiliation" (
  "id" bigserial NOT NULL,
  "fhir_id" text NOT NULL,
  "resource_type" text,
  "language" text,
  "text_status" text,
  "text_div" text,
  "active" boolean,
  "period_end" timestamp,
  "period_start" timestamp,
  "organization_id" bigint,
  "participating_organization_id" bigint,
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
  CONSTRAINT "organization_affiliation_pk" PRIMARY KEY ("id"),
  CONSTRAINT "organization_affiliation_fhir_id_uniq" UNIQUE NULLS NOT DISTINCT ("fhir_id")
);
CREATE INDEX IF NOT EXISTS "organization_affiliation_fhir_id_idx" ON "public"."organization_affiliation" ("fhir_id");
CREATE INDEX IF NOT EXISTS "organization_affiliation_organization_id_idx" ON "public"."organization_affiliation" ("organization_id");
CREATE INDEX IF NOT EXISTS "organization_affiliation_participating_organization_id_idx" ON "public"."organization_affiliation" ("participating_organization_id");
