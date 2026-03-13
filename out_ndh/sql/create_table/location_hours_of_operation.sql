CREATE TABLE IF NOT EXISTS "public"."location_hours_of_operation" (
  "id" bigserial NOT NULL,
  "location_parent_id" bigint NOT NULL,
  "idx" integer,
  "all_day" boolean,
  "closing_time" text,
  "opening_time" text,
  CONSTRAINT "location_hours_of_operation_pk" PRIMARY KEY ("id"),
  CONSTRAINT "location_hours_of_operation_uniq" UNIQUE NULLS NOT DISTINCT ("location_parent_id", "all_day", "closing_time", "opening_time")
);
CREATE INDEX IF NOT EXISTS "location_hours_of_operation_location_parent_id_idx" ON "public"."location_hours_of_operation" ("location_parent_id");
