CREATE TABLE IF NOT EXISTS "public"."organization_affiliation_location" (
  "id" bigserial NOT NULL,
  "organization_affiliation_id" bigint NOT NULL,
  "location_id" bigint NOT NULL,
  CONSTRAINT "organization_affiliation_location_pk" PRIMARY KEY ("id"),
  CONSTRAINT "organization_affiliation_location_uniq" UNIQUE NULLS NOT DISTINCT ("organization_affiliation_id", "location_id")
);
CREATE INDEX IF NOT EXISTS "organization_affiliation_location_organization_affiliation_idx" ON "public"."organization_affiliation_location" ("organization_affiliation_id");
CREATE INDEX IF NOT EXISTS "organization_affiliation_location_location_idx" ON "public"."organization_affiliation_location" ("location_id");
