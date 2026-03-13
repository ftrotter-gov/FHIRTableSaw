CREATE TABLE IF NOT EXISTS "public"."organization_fac_taxonomy" (
  "id" bigserial NOT NULL,
  "organization_fac_id" bigint NOT NULL,
  "taxonomy_code" text NOT NULL,
  CONSTRAINT "organization_fac_taxonomy_pk" PRIMARY KEY ("id"),
  CONSTRAINT "organization_fac_taxonomy_uniq" UNIQUE NULLS NOT DISTINCT ("organization_fac_id", "taxonomy_code")
);
CREATE INDEX IF NOT EXISTS "organization_fac_taxonomy_organization_fac_id_idx" ON "public"."organization_fac_taxonomy" ("organization_fac_id");
CREATE INDEX IF NOT EXISTS "organization_fac_taxonomy_taxonomy_code_idx" ON "public"."organization_fac_taxonomy" ("taxonomy_code");
