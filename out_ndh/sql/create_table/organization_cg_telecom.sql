CREATE TABLE IF NOT EXISTS "public"."organization_cg_telecom" (
  "id" bigserial NOT NULL,
  "organization_cg_parent_id" bigint NOT NULL,
  "idx" integer,
  "system" text,
  "use" text,
  "value" text,
  CONSTRAINT "organization_cg_telecom_pk" PRIMARY KEY ("id"),
  CONSTRAINT "organization_cg_telecom_uniq" UNIQUE NULLS NOT DISTINCT ("organization_cg_parent_id", "system", "use", "value")
);
CREATE INDEX IF NOT EXISTS "organization_cg_telecom_organization_cg_parent_id_idx" ON "public"."organization_cg_telecom" ("organization_cg_parent_id");
