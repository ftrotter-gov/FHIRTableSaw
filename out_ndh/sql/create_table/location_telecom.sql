CREATE TABLE IF NOT EXISTS "public"."location_telecom" (
  "id" bigserial NOT NULL,
  "location_parent_id" bigint NOT NULL,
  "idx" integer,
  "rank" integer,
  "system" text,
  "use" text,
  "value" text,
  CONSTRAINT "location_telecom_pk" PRIMARY KEY ("id"),
  CONSTRAINT "location_telecom_uniq" UNIQUE NULLS NOT DISTINCT ("location_parent_id", "rank", "system", "use", "value")
);
CREATE INDEX IF NOT EXISTS "location_telecom_location_parent_id_idx" ON "public"."location_telecom" ("location_parent_id");
