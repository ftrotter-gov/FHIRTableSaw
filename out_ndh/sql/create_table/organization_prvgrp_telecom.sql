CREATE TABLE IF NOT EXISTS "public"."organization_prvgrp_telecom" (
  "id" bigserial NOT NULL,
  "organization_prvgrp_parent_id" bigint NOT NULL,
  "idx" integer,
  "rank" integer,
  "system" text,
  "use" text,
  "value" text,
  CONSTRAINT "organization_prvgrp_telecom_pk" PRIMARY KEY ("id"),
  CONSTRAINT "organization_prvgrp_telecom_uniq" UNIQUE NULLS NOT DISTINCT ("organization_prvgrp_parent_id", "rank", "system", "use", "value")
);
CREATE INDEX IF NOT EXISTS "organization_prvgrp_telecom_organization_prvgrp_parent_id_idx" ON "public"."organization_prvgrp_telecom" ("organization_prvgrp_parent_id");
