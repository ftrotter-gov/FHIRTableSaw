CREATE TABLE IF NOT EXISTS "public"."practitioner_role_telecom" (
  "id" bigserial NOT NULL,
  "practitioner_role_parent_id" bigint NOT NULL,
  "idx" integer,
  "rank" integer,
  "system" text,
  "value" text,
  CONSTRAINT "practitioner_role_telecom_pk" PRIMARY KEY ("id"),
  CONSTRAINT "practitioner_role_telecom_uniq" UNIQUE NULLS NOT DISTINCT ("practitioner_role_parent_id", "rank", "system", "value")
);
CREATE INDEX IF NOT EXISTS "practitioner_role_telecom_practitioner_role_parent_id_idx" ON "public"."practitioner_role_telecom" ("practitioner_role_parent_id");
