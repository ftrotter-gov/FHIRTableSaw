CREATE TABLE IF NOT EXISTS "public"."organization_affiliation_organization" (
  "id" bigserial NOT NULL,
  "organization_affiliation_id" bigint NOT NULL,
  "organization_id" bigint NOT NULL,
  CONSTRAINT "organization_affiliation_organization_pk" PRIMARY KEY ("id"),
  CONSTRAINT "organization_affiliation_organization_uniq" UNIQUE NULLS NOT DISTINCT ("organization_affiliation_id", "organization_id")
);
CREATE INDEX IF NOT EXISTS "organization_affiliation_organization_organization_affiliation_idx" ON "public"."organization_affiliation_organization" ("organization_affiliation_id");
CREATE INDEX IF NOT EXISTS "organization_affiliation_organization_organization_idx" ON "public"."organization_affiliation_organization" ("organization_id");
