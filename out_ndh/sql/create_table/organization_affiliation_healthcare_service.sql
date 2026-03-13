CREATE TABLE IF NOT EXISTS "public"."organization_affiliation_healthcare_service" (
  "id" bigserial NOT NULL,
  "organization_affiliation_id" bigint NOT NULL,
  "healthcare_service_id" bigint NOT NULL,
  CONSTRAINT "organization_affiliation_healthcare_service_pk" PRIMARY KEY ("id"),
  CONSTRAINT "organization_affiliation_healthcare_service_uniq" UNIQUE NULLS NOT DISTINCT ("organization_affiliation_id", "healthcare_service_id")
);
CREATE INDEX IF NOT EXISTS "organization_affiliation_healthcare_service_organization_affiliation_idx" ON "public"."organization_affiliation_healthcare_service" ("organization_affiliation_id");
CREATE INDEX IF NOT EXISTS "organization_affiliation_healthcare_service_healthcare_service_idx" ON "public"."organization_affiliation_healthcare_service" ("healthcare_service_id");
