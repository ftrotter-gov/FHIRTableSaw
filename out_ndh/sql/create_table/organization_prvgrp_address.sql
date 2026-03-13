CREATE TABLE IF NOT EXISTS "public"."organization_prvgrp_address" (
  "id" bigserial NOT NULL,
  "organization_prvgrp_parent_id" bigint NOT NULL,
  "idx" integer,
  "city" text,
  "country" text,
  "postal_code" text,
  "state" text,
  "text" text,
  "type" text,
  "use" text,
  CONSTRAINT "organization_prvgrp_address_pk" PRIMARY KEY ("id"),
  CONSTRAINT "organization_prvgrp_address_uniq" UNIQUE NULLS NOT DISTINCT ("organization_prvgrp_parent_id", "city", "country", "postal_code", "state", "text", "type", "use")
);
CREATE INDEX IF NOT EXISTS "organization_prvgrp_address_organization_prvgrp_parent_id_idx" ON "public"."organization_prvgrp_address" ("organization_prvgrp_parent_id");
