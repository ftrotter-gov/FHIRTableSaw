CREATE TABLE IF NOT EXISTS "public"."practitioner_role_endpoint" (
  "id" bigserial NOT NULL,
  "practitioner_role_id" bigint NOT NULL,
  "endpoint_id" bigint NOT NULL,
  CONSTRAINT "practitioner_role_endpoint_pk" PRIMARY KEY ("id"),
  CONSTRAINT "practitioner_role_endpoint_uniq" UNIQUE NULLS NOT DISTINCT ("practitioner_role_id", "endpoint_id")
);
CREATE INDEX IF NOT EXISTS "practitioner_role_endpoint_practitioner_role_idx" ON "public"."practitioner_role_endpoint" ("practitioner_role_id");
CREATE INDEX IF NOT EXISTS "practitioner_role_endpoint_endpoint_idx" ON "public"."practitioner_role_endpoint" ("endpoint_id");
