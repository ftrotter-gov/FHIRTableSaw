CREATE TABLE IF NOT EXISTS "public"."practitioner_role_location" (
  "id" bigserial NOT NULL,
  "practitioner_role_id" bigint NOT NULL,
  "location_id" bigint NOT NULL,
  CONSTRAINT "practitioner_role_location_pk" PRIMARY KEY ("id"),
  CONSTRAINT "practitioner_role_location_uniq" UNIQUE NULLS NOT DISTINCT ("practitioner_role_id", "location_id")
);
CREATE INDEX IF NOT EXISTS "practitioner_role_location_practitioner_role_idx" ON "public"."practitioner_role_location" ("practitioner_role_id");
CREATE INDEX IF NOT EXISTS "practitioner_role_location_location_idx" ON "public"."practitioner_role_location" ("location_id");
