CREATE TABLE IF NOT EXISTS "public"."organization_ntwk_identifier" (
  "id" bigserial NOT NULL,
  "organization_ntwk_parent_id" bigint NOT NULL,
  "idx" integer,
  "system" text,
  "value" text,
  CONSTRAINT "organization_ntwk_identifier_pk" PRIMARY KEY ("id"),
  CONSTRAINT "organization_ntwk_identifier_uniq" UNIQUE NULLS NOT DISTINCT ("organization_ntwk_parent_id", "system", "value")
);
CREATE INDEX IF NOT EXISTS "organization_ntwk_identifier_organization_ntwk_parent_id_idx" ON "public"."organization_ntwk_identifier" ("organization_ntwk_parent_id");
