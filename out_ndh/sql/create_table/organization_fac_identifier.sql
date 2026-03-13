CREATE TABLE IF NOT EXISTS "public"."organization_fac_identifier" (
  "id" bigserial NOT NULL,
  "organization_fac_parent_id" bigint NOT NULL,
  "idx" integer,
  "system" text,
  "value" text,
  CONSTRAINT "organization_fac_identifier_pk" PRIMARY KEY ("id"),
  CONSTRAINT "organization_fac_identifier_uniq" UNIQUE NULLS NOT DISTINCT ("organization_fac_parent_id", "system", "value")
);
CREATE INDEX IF NOT EXISTS "organization_fac_identifier_organization_fac_parent_id_idx" ON "public"."organization_fac_identifier" ("organization_fac_parent_id");
