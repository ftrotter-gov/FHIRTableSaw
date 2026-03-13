CREATE TABLE IF NOT EXISTS "public"."organization_fac_telecom" (
  "id" bigserial NOT NULL,
  "organization_fac_parent_id" bigint NOT NULL,
  "idx" integer,
  "rank" integer,
  "system" text,
  "value" text,
  CONSTRAINT "organization_fac_telecom_pk" PRIMARY KEY ("id"),
  CONSTRAINT "organization_fac_telecom_uniq" UNIQUE NULLS NOT DISTINCT ("organization_fac_parent_id", "rank", "system", "value")
);
CREATE INDEX IF NOT EXISTS "organization_fac_telecom_organization_fac_parent_id_idx" ON "public"."organization_fac_telecom" ("organization_fac_parent_id");
