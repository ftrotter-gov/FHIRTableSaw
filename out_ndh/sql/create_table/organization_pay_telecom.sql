CREATE TABLE IF NOT EXISTS "public"."organization_pay_telecom" (
  "id" bigserial NOT NULL,
  "organization_pay_parent_id" bigint NOT NULL,
  "idx" integer,
  "rank" integer,
  "system" text,
  "value" text,
  CONSTRAINT "organization_pay_telecom_pk" PRIMARY KEY ("id"),
  CONSTRAINT "organization_pay_telecom_uniq" UNIQUE NULLS NOT DISTINCT ("organization_pay_parent_id", "rank", "system", "value")
);
CREATE INDEX IF NOT EXISTS "organization_pay_telecom_organization_pay_parent_id_idx" ON "public"."organization_pay_telecom" ("organization_pay_parent_id");
