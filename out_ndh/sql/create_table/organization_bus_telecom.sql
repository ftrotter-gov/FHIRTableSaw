CREATE TABLE IF NOT EXISTS "public"."organization_bus_telecom" (
  "id" bigserial NOT NULL,
  "organization_bus_parent_id" bigint NOT NULL,
  "idx" integer,
  "rank" integer,
  "system" text,
  "value" text,
  CONSTRAINT "organization_bus_telecom_pk" PRIMARY KEY ("id"),
  CONSTRAINT "organization_bus_telecom_uniq" UNIQUE NULLS NOT DISTINCT ("organization_bus_parent_id", "rank", "system", "value")
);
CREATE INDEX IF NOT EXISTS "organization_bus_telecom_organization_bus_parent_id_idx" ON "public"."organization_bus_telecom" ("organization_bus_parent_id");
