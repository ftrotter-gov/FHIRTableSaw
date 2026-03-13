CREATE TABLE IF NOT EXISTS "public"."organization_ntwk" (
  "id" bigserial NOT NULL,
  "fhir_id" text NOT NULL,
  "resource_type" text,
  "language" text,
  "text_status" text,
  "text_div" text,
  "active" boolean,
  "name" text,
  "part_of_id" bigint,
  "verification_status" text,
  "type_text" text,
  "coding_system" text,
  "coding_version" text,
  "coding_code" text,
  "coding_display" text,
  "contact_name_family" text,
  CONSTRAINT "organization_ntwk_pk" PRIMARY KEY ("id"),
  CONSTRAINT "organization_ntwk_fhir_id_uniq" UNIQUE NULLS NOT DISTINCT ("fhir_id")
);
CREATE INDEX IF NOT EXISTS "organization_ntwk_fhir_id_idx" ON "public"."organization_ntwk" ("fhir_id");
CREATE INDEX IF NOT EXISTS "organization_ntwk_part_of_id_idx" ON "public"."organization_ntwk" ("part_of_id");
