CREATE TABLE IF NOT EXISTS "public"."organization_govt_telecom" (
  "id" bigserial NOT NULL,
  "organization_govt_parent_id" bigint NOT NULL,
  "idx" integer,
  "system" text,
  "use" text,
  "value" text,
  CONSTRAINT "organization_govt_telecom_pk" PRIMARY KEY ("id"),
  CONSTRAINT "organization_govt_telecom_uniq" UNIQUE NULLS NOT DISTINCT ("organization_govt_parent_id", "system", "use", "value")
);
CREATE INDEX IF NOT EXISTS "organization_govt_telecom_organization_govt_parent_id_idx" ON "public"."organization_govt_telecom" ("organization_govt_parent_id");
