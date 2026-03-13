CREATE TABLE IF NOT EXISTS "public"."organization_fac" (
  "id" bigserial NOT NULL,
  "fhir_id" text NOT NULL,
  "resource_type" text,
  "language" text,
  "text_status" text,
  "text_div" text,
  "active" boolean,
  "name" text,
  "part_of_id" bigint,
  "qualification" text,
  "verification_status" text,
  "description" text,
  "type_text" text,
  "coding_system" text,
  "coding_code" text,
  "coding_display" text,
  "address_city" text,
  "address_state" text,
  "address_postal_code" text,
  "telecom_system" text,
  "telecom_value" text,
  "telecom_rank" integer,
  CONSTRAINT "organization_fac_pk" PRIMARY KEY ("id"),
  CONSTRAINT "organization_fac_fhir_id_uniq" UNIQUE NULLS NOT DISTINCT ("fhir_id")
);
CREATE INDEX IF NOT EXISTS "organization_fac_fhir_id_idx" ON "public"."organization_fac" ("fhir_id");
CREATE INDEX IF NOT EXISTS "organization_fac_part_of_id_idx" ON "public"."organization_fac" ("part_of_id");
