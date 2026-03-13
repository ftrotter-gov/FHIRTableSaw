CREATE TABLE IF NOT EXISTS "public"."practitioner_role_specialty" (
  "id" bigserial NOT NULL,
  "practitioner_role_parent_id" bigint NOT NULL,
  "idx" integer,
  CONSTRAINT "practitioner_role_specialty_pk" PRIMARY KEY ("id"),
  CONSTRAINT "practitioner_role_specialty_uniq" UNIQUE NULLS NOT DISTINCT ("practitioner_role_parent_id")
);
CREATE INDEX IF NOT EXISTS "practitioner_role_specialty_practitioner_role_parent_id_idx" ON "public"."practitioner_role_specialty" ("practitioner_role_parent_id");
