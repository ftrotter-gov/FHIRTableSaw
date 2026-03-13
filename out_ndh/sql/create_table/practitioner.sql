CREATE TABLE IF NOT EXISTS "public"."practitioner" (
  "id" bigserial NOT NULL,
  "fhir_id" text NOT NULL,
  "resource_type" text,
  "language" text,
  "text_status" text,
  "text_div" text,
  "active" boolean,
  "communication_proficiency" text,
  "verification_status" text,
  "name_text" text,
  "name_family" text,
  "name_use" text,
  "coding_system" text,
  "coding_version" text,
  "coding_code" text,
  "coding_display" text,
  "coding_system" text,
  "coding_code" text,
  CONSTRAINT "practitioner_pk" PRIMARY KEY ("id"),
  CONSTRAINT "practitioner_fhir_id_uniq" UNIQUE NULLS NOT DISTINCT ("fhir_id")
);
CREATE INDEX IF NOT EXISTS "practitioner_fhir_id_idx" ON "public"."practitioner" ("fhir_id");
