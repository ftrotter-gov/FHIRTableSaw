CREATE TABLE IF NOT EXISTS "public"."practitioner_identifier" (
  "id" bigserial NOT NULL,
  "practitioner_parent_id" bigint NOT NULL,
  "idx" integer,
  "system" text,
  "use" text,
  "value" text,
  CONSTRAINT "practitioner_identifier_pk" PRIMARY KEY ("id"),
  CONSTRAINT "practitioner_identifier_uniq" UNIQUE NULLS NOT DISTINCT ("practitioner_parent_id", "system", "use", "value")
);
CREATE INDEX IF NOT EXISTS "practitioner_identifier_practitioner_parent_id_idx" ON "public"."practitioner_identifier" ("practitioner_parent_id");
