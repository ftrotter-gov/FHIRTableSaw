CREATE TABLE IF NOT EXISTS "public"."organization_pay_taxonomy" (
  "id" bigserial NOT NULL,
  "organization_pay_id" bigint NOT NULL,
  "taxonomy_code" text NOT NULL,
  CONSTRAINT "organization_pay_taxonomy_pk" PRIMARY KEY ("id"),
  CONSTRAINT "organization_pay_taxonomy_uniq" UNIQUE NULLS NOT DISTINCT ("organization_pay_id", "taxonomy_code")
);
CREATE INDEX IF NOT EXISTS "organization_pay_taxonomy_organization_pay_id_idx" ON "public"."organization_pay_taxonomy" ("organization_pay_id");
CREATE INDEX IF NOT EXISTS "organization_pay_taxonomy_taxonomy_code_idx" ON "public"."organization_pay_taxonomy" ("taxonomy_code");
