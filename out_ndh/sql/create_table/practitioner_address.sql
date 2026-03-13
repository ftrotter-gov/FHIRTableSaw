CREATE TABLE IF NOT EXISTS "public"."practitioner_address" (
  "id" bigserial NOT NULL,
  "practitioner_parent_id" bigint NOT NULL,
  "idx" integer,
  "city" text,
  "country" text,
  "postal_code" text,
  "state" text,
  "type" text,
  "use" text,
  CONSTRAINT "practitioner_address_pk" PRIMARY KEY ("id"),
  CONSTRAINT "practitioner_address_uniq" UNIQUE NULLS NOT DISTINCT ("practitioner_parent_id", "city", "country", "postal_code", "state", "type", "use")
);
CREATE INDEX IF NOT EXISTS "practitioner_address_practitioner_parent_id_idx" ON "public"."practitioner_address" ("practitioner_parent_id");
